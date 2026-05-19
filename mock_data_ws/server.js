/**
 * mock_data_ws/server.js
 *
 * A self-contained WebSocket + HTTP server that mimics Zoom RTMS behaviour
 * but streams data from local test fixtures (transcripts & calendar invites).
 *
 * WebSocket endpoints
 *   ws://localhost:9095/signaling              — RTMS-style handshake then transcript stream
 *   wss://localhost:9095/subscribe?touchpoint= — Meeting-assistant style: auth then transcript stream
 *
 * HTTP endpoints
 *   POST /auth                     — exchange client_id+secret for a bearer token
 *   GET  /health                   — liveness check
 *   GET  /touchpoints              — list valid touchpoint IDs
 *   GET  /meetings                 — list available mock meeting UUIDs
 *   GET  /transcripts              — all transcript sessions
 *   GET  /transcripts/:meetingUuid — transcript for a specific meeting
 *   GET  /calendar-invites         — all calendar invites
 *   GET  /calendar-invites/:id     — single invite by id or meeting_uuid
 *
 * Message types (mirrors Zoom RTMS protocol)
 *   1   SIGNALING_HAND_SHAKE_REQ
 *   2   SIGNALING_HAND_SHAKE_RESP
 *   5   EVENT_SUBSCRIPTION
 *   6   EVENT_UPDATE
 *   9   SESSION_STATE_UPDATE
 *   12  KEEP_ALIVE_REQ
 *   13  KEEP_ALIVE_RESP
 *   17  MEDIA_DATA_TRANSCRIPT
 *   50  CALENDAR_INVITE  (custom extension)
 *
 * /subscribe message protocol
 *   → { type: "auth", token: "Bearer <token>" }   (first message from client)
 *   ← { type: "auth_ok",  session_id, touchpoint }  (server accepts)
 *   ← { type: "auth_err", message }                 (server rejects → socket closed)
 *   ← { type: "invite",  ...calendarInvite }         (pushed immediately after auth)
 *   ← { type: "transcript", user_id, display_name, text, is_final, sequence, timestamp }
 *   ← { type: "session_end", reason }               (stream finished)
 *   → { type: "ping" }  /  ← { type: "pong" }       (keep-alive)
 */

'use strict';

const http    = require('http');
const WebSocket = require('ws');
const express = require('express');
const crypto  = require('crypto');
const path    = require('path');

// ─── Test data ────────────────────────────────────────────────────────────────
const TRANSCRIPTS     = require('./test_data/transcripts.json');
const CALENDAR_DATA   = require('./test_data/calendar_invites.json');

// ─── Config ───────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 9095;

// ─── Message type constants (matches Zoom RTMS) ───────────────────────────────
const MSG = {
  SIGNALING_HAND_SHAKE_REQ:  1,
  SIGNALING_HAND_SHAKE_RESP: 2,
  EVENT_SUBSCRIPTION:        5,
  EVENT_UPDATE:              6,
  SESSION_STATE_UPDATE:      9,
  KEEP_ALIVE_REQ:           12,
  KEEP_ALIVE_RESP:          13,
  MEDIA_DATA_TRANSCRIPT:    17,
  CALENDAR_INVITE:          50,   // custom extension
};

const SESSION_STATE = { STARTED: 1, PAUSED: 2, RESUMED: 3, STOPPED: 4 };
const STREAM_STATUS = { ACTIVE: 1, INACTIVE: 2, TERMINATED: 3 };

// ─── Auth token store (in-memory) ────────────────────────────────────────────
// token → { client_id, created_at }
const authTokens = new Map();
const TOKEN_TTL_MS = 3600 * 1000; // 1 hour

function issueToken(clientId) {
  const token = crypto.randomBytes(24).toString('hex');
  authTokens.set(token, { client_id: clientId, created_at: Date.now() });
  return token;
}

function validateToken(authHeader) {
  if (!authHeader) return null;
  const token = authHeader.replace(/^Bearer\s+/i, '').trim();
  const record = authTokens.get(token);
  if (!record) return null;
  if (Date.now() - record.created_at > TOKEN_TTL_MS) {
    authTokens.delete(token);
    return null;
  }
  return record;
}

// ─── Touchpoint → meeting_uuid map ───────────────────────────────────────────
// These are the "valid_id" values clients put in the subscribe URL
const TOUCHPOINTS = [
  { touchpoint_id: 'tp_q2_planning',   meeting_uuid: 'TNhvT3WEBT6Srse3TgWRGr', label: 'Q2 Product Planning' },
  { touchpoint_id: 'tp_sprint_review', meeting_uuid: 'KLhvT3WEBT6Srse3TgWRGs', label: 'Engineering Sprint Review — Sprint 24' },
  { touchpoint_id: 'tp_cs_qbr',        meeting_uuid: 'PLhvT3WEBT6Srse3TgWRGt', label: 'Customer Success QBR' },
];

function resolveTouchpoint(id) {
  // Accept both the short touchpoint_id and the raw meeting_uuid
  return TOUCHPOINTS.find(t => t.touchpoint_id === id || t.meeting_uuid === id) || null;
}

// ─── Helpers ──────────────────────────────────────────────────────────────────
function generateSessionId() {
  return crypto.randomBytes(8).toString('hex');
}

function send(ws, payload) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(payload));
  }
}

function pickSession(meetingUuid) {
  if (meetingUuid) {
    return TRANSCRIPTS.sessions.find(s => s.meeting_uuid === meetingUuid) || null;
  }
  // Round-robin through available sessions if no UUID given
  const idx = Math.floor(Math.random() * TRANSCRIPTS.sessions.length);
  return TRANSCRIPTS.sessions[idx];
}

// ─── Express app (HTTP) ───────────────────────────────────────────────────────
const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// CORS — open for local dev
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

// ── POST /auth ── exchange credentials for a bearer token ────────────────────
app.post('/auth', (req, res) => {
  const { client_id, client_secret } = req.body || {};
  if (!client_id || !client_secret) {
    return res.status(400).json({ error: 'client_id and client_secret are required' });
  }
  // In mock mode we accept any non-empty credentials
  const token = issueToken(client_id);
  console.log(`[AUTH] Token issued for client_id=${client_id}`);
  res.json({
    access_token: token,
    token_type:   'Bearer',
    expires_in:   3600,
    scope:        'meeting:transcript meeting:invite',
  });
});

app.get('/health', (_req, res) => res.json({ status: 'ok', timestamp: new Date().toISOString() }));

// ── GET /touchpoints ─────────────────────────────────────────────────────────
app.get('/touchpoints', (_req, res) => {
  res.json({ touchpoints: TOUCHPOINTS });
});

app.get('/meetings', (_req, res) => {
  res.json({
    meetings: TRANSCRIPTS.sessions.map(s => ({
      meeting_uuid:  s.meeting_uuid,
      meeting_topic: s.meeting_topic,
      start_time:    s.start_time,
      participants:  s.participants,
    })),
  });
});

app.get('/transcripts', (_req, res) => res.json(TRANSCRIPTS));

app.get('/transcripts/:meetingUuid', (req, res) => {
  const session = TRANSCRIPTS.sessions.find(s => s.meeting_uuid === req.params.meetingUuid);
  if (!session) return res.status(404).json({ error: 'Meeting not found' });
  res.json(session);
});

app.get('/calendar-invites', (_req, res) => res.json(CALENDAR_DATA));

app.get('/calendar-invites/:identifier', (req, res) => {
  const id = req.params.identifier;
  const invite = CALENDAR_DATA.calendar_invites.find(
    c => c.id === id || c.meeting_uuid === id
  );
  if (!invite) return res.status(404).json({ error: 'Invite not found' });
  res.json(invite);
});

// ─── HTTP server ──────────────────────────────────────────────────────────────
const httpServer = http.createServer(app);

// ─── WebSocket servers (noServer — routing handled in 'upgrade') ──────────────
const wssSignaling  = new WebSocket.Server({ noServer: true }); // /signaling
const wssSubscribe  = new WebSocket.Server({ noServer: true }); // /subscribe

httpServer.on('upgrade', (req, socket, head) => {
  const url  = new URL(req.url, `http://${req.headers.host}`);
  if (url.pathname === '/signaling') {
    wssSignaling.handleUpgrade(req, socket, head, ws => wssSignaling.emit('connection', ws, req));
  } else if (url.pathname === '/subscribe') {
    wssSubscribe.handleUpgrade(req, socket, head, ws => wssSubscribe.emit('connection', ws, req));
  } else {
    socket.destroy();
  }
});

// alias so existing code below still works
const wss = wssSignaling;

wss.on('connection', (ws, req) => {
  const remoteAddr = req.socket.remoteAddress;
  console.log(`[WS] Client connected from ${remoteAddr}`);

  ws.sessionId     = generateSessionId();
  ws.authenticated = false;
  ws.meetingUuid   = null;
  ws.keepAliveTimer = null;
  ws.transcriptTimer = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); }
    catch { return; }
    handleMessage(ws, msg);
  });

  ws.on('close', () => {
    console.log(`[WS] Client disconnected (session: ${ws.sessionId})`);
    clearTimers(ws);
  });

  ws.on('error', (err) => {
    console.error(`[WS] Error on session ${ws.sessionId}:`, err.message);
    clearTimers(ws);
  });
});

// ─── Message dispatcher ───────────────────────────────────────────────────────
function handleMessage(ws, msg) {
  console.log(`[WS][${ws.sessionId}] → msg_type=${msg.msg_type}`);

  switch (msg.msg_type) {
    case MSG.SIGNALING_HAND_SHAKE_REQ:
      handleHandshake(ws, msg);
      break;

    case MSG.EVENT_SUBSCRIPTION:
      handleEventSubscription(ws, msg);
      break;

    case MSG.SESSION_STATE_UPDATE:
      handleSessionStateUpdate(ws, msg);
      break;

    case MSG.KEEP_ALIVE_RESP:
      // Client acknowledged our keep-alive ping — nothing to do
      break;

    default:
      console.log(`[WS][${ws.sessionId}] Unknown msg_type: ${msg.msg_type}`);
  }
}

// ─── Handshake ────────────────────────────────────────────────────────────────
function handleHandshake(ws, msg) {
  const { meeting_uuid, rtms_stream_id, signature } = msg;

  // Lightweight validation: we accept any non-empty signature in mock mode
  if (!meeting_uuid || !rtms_stream_id || !signature) {
    send(ws, {
      msg_type:   MSG.SIGNALING_HAND_SHAKE_RESP,
      session_id: ws.sessionId,
      status_code: 401,
      status_message: 'Missing required handshake fields',
    });
    ws.close();
    return;
  }

  ws.authenticated = true;
  ws.meetingUuid   = meeting_uuid;

  console.log(`[WS][${ws.sessionId}] Handshake OK — meeting: ${meeting_uuid}`);

  // 1. Accept handshake
  send(ws, {
    msg_type:     MSG.SIGNALING_HAND_SHAKE_RESP,
    session_id:   ws.sessionId,
    status_code:  200,
    status_message: 'Handshake accepted',
    media_server: {
      host: `ws://localhost:${PORT}/signaling`,
      signature: crypto.randomBytes(16).toString('hex'),
    },
  });

  // 2. Notify session started
  send(ws, {
    msg_type:   MSG.SESSION_STATE_UPDATE,
    session_id: ws.sessionId,
    state:      SESSION_STATE.STARTED,
    timestamp:  Date.now(),
  });

  // 3. Push calendar invite for this meeting (if one exists)
  const invite = CALENDAR_DATA.calendar_invites.find(c => c.meeting_uuid === meeting_uuid);
  if (invite) {
    send(ws, {
      msg_type:   MSG.CALENDAR_INVITE,
      session_id: ws.sessionId,
      timestamp:  Date.now(),
      content:    invite,
    });
  }

  // 4. Start keep-alive loop
  startKeepAlive(ws);
}

// ─── Event subscription ───────────────────────────────────────────────────────
function handleEventSubscription(ws, msg) {
  if (!ws.authenticated) return;

  const events = msg.event_type || [];
  console.log(`[WS][${ws.sessionId}] Subscribed to events:`, events);

  // Acknowledge subscription
  send(ws, {
    msg_type:   MSG.EVENT_UPDATE,
    session_id: ws.sessionId,
    timestamp:  Date.now(),
    content: { message: 'Subscription confirmed', events },
  });

  // Start streaming transcripts once the client subscribes
  startTranscriptStream(ws);
}

// ─── Session state update ─────────────────────────────────────────────────────
function handleSessionStateUpdate(ws, msg) {
  if (msg.state === SESSION_STATE.STOPPED) {
    console.log(`[WS][${ws.sessionId}] Session stop requested`);
    clearTimers(ws);
    send(ws, {
      msg_type:   MSG.SESSION_STATE_UPDATE,
      session_id: ws.sessionId,
      state:      SESSION_STATE.STOPPED,
      timestamp:  Date.now(),
    });
    ws.close();
  }
}

// ─── Transcript streaming ─────────────────────────────────────────────────────
function startTranscriptStream(ws) {
  const session = pickSession(ws.meetingUuid);
  if (!session) {
    console.log(`[WS][${ws.sessionId}] No transcript session found for ${ws.meetingUuid}`);
    return;
  }

  const entries  = session.transcript_entries;
  let   index    = 0;

  // Use each entry's timestamp_ms as the delay from the previous entry
  function scheduleNext() {
    if (index >= entries.length) {
      // All entries sent — signal session stopped
      send(ws, {
        msg_type:   MSG.SESSION_STATE_UPDATE,
        session_id: ws.sessionId,
        state:      SESSION_STATE.STOPPED,
        stop_reason: 'STOP_BC_MEETING_ENDED',
        timestamp:  Date.now(),
      });
      return;
    }

    const entry     = entries[index];
    const prevTs    = index === 0 ? 0 : entries[index - 1].timestamp_ms;
    const delay     = Math.max(entry.timestamp_ms - prevTs, 500); // min 500ms gap
    index++;

    ws.transcriptTimer = setTimeout(() => {
      if (ws.readyState !== WebSocket.OPEN) return;

      send(ws, {
        msg_type:   MSG.MEDIA_DATA_TRANSCRIPT,
        session_id: ws.sessionId,
        timestamp:  Date.now(),
        content: {
          user_id:      entry.user_id,
          display_name: entry.display_name,
          language:     entry.language,
          data:         entry.text,
          is_final:     entry.is_final,
          sequence:     index,
        },
      });

      scheduleNext();
    }, delay);
  }

  scheduleNext();
}

// ─────────────────────────────────────────────────────────────────────────────
// /subscribe  — Meeting-assistant style WebSocket
// wss://meeting-assistant.example.com/subscribe?touchpoint={valid_id}
// ─────────────────────────────────────────────────────────────────────────────
wssSubscribe.on('connection', (ws, req) => {
  const url        = new URL(req.url, `http://${req.headers.host}`);
  const touchpointId = url.searchParams.get('touchpoint');

  ws.sessionId      = generateSessionId();
  ws.authenticated  = false;
  ws.transcriptTimer = null;
  ws.pingTimer       = null;

  console.log(`[SUB][${ws.sessionId}] Connection — touchpoint=${touchpointId}`);

  // Resolve the touchpoint immediately so we can reject early
  const tp = resolveTouchpoint(touchpointId);
  if (!tp) {
    ws.send(JSON.stringify({ type: 'error', message: `Unknown touchpoint: ${touchpointId}` }));
    ws.close(4004, 'Unknown touchpoint');
    return;
  }
  ws.touchpoint   = tp;
  ws.meetingUuid  = tp.meeting_uuid;

  ws.on('message', raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    handleSubscribeMessage(ws, msg);
  });

  ws.on('close', () => {
    console.log(`[SUB][${ws.sessionId}] Disconnected`);
    clearSubscribeTimers(ws);
  });

  ws.on('error', err => {
    console.error(`[SUB][${ws.sessionId}] Error:`, err.message);
    clearSubscribeTimers(ws);
  });
});

function handleSubscribeMessage(ws, msg) {
  console.log(`[SUB][${ws.sessionId}] → type=${msg.type}`);

  if (!ws.authenticated) {
    // Only message allowed before auth is the auth message itself
    if (msg.type !== 'auth') {
      ws.send(JSON.stringify({ type: 'auth_err', message: 'Send auth message first' }));
      ws.close(4001, 'Unauthenticated');
      return;
    }
    const record = validateToken(msg.token);
    if (!record) {
      ws.send(JSON.stringify({ type: 'auth_err', message: 'Invalid or expired token' }));
      ws.close(4001, 'Invalid token');
      return;
    }
    ws.authenticated = true;
    ws.clientId      = record.client_id;
    console.log(`[SUB][${ws.sessionId}] Auth OK — client=${record.client_id} touchpoint=${ws.touchpoint.touchpoint_id}`);

    // 1. Confirm auth
    ws.send(JSON.stringify({
      type:        'auth_ok',
      session_id:  ws.sessionId,
      touchpoint:  ws.touchpoint.touchpoint_id,
      meeting_uuid: ws.meetingUuid,
    }));

    // 2. Push matching calendar invite
    const invite = CALENDAR_DATA.calendar_invites.find(c => c.meeting_uuid === ws.meetingUuid);
    if (invite) {
      ws.send(JSON.stringify({ type: 'invite', ...invite }));
    }

    // 3. Start ping loop
    startSubscribePing(ws);

    // 4. Start transcript stream
    startSubscribeTranscriptStream(ws);
    return;
  }

  if (msg.type === 'ping') {
    ws.send(JSON.stringify({ type: 'pong', timestamp: Date.now() }));
  }
}

function startSubscribeTranscriptStream(ws) {
  const session = pickSession(ws.meetingUuid);
  if (!session) return;

  const entries = session.transcript_entries;
  let   index   = 0;

  function scheduleNext() {
    if (index >= entries.length) {
      ws.send(JSON.stringify({ type: 'session_end', reason: 'meeting_ended', timestamp: Date.now() }));
      return;
    }
    const entry  = entries[index];
    const prevTs = index === 0 ? 0 : entries[index - 1].timestamp_ms;
    const delay  = Math.max(entry.timestamp_ms - prevTs, 500);
    index++;

    ws.transcriptTimer = setTimeout(() => {
      if (ws.readyState !== WebSocket.OPEN) return;
      ws.send(JSON.stringify({
        type:         'transcript',
        user_id:      entry.user_id,
        display_name: entry.display_name,
        language:     entry.language,
        text:         entry.text,
        is_final:     entry.is_final,
        sequence:     index,
        timestamp:    Date.now(),
      }));
      scheduleNext();
    }, delay);
  }

  scheduleNext();
}

function startSubscribePing(ws) {
  ws.pingTimer = setInterval(() => {
    if (ws.readyState !== WebSocket.OPEN) { clearInterval(ws.pingTimer); return; }
    ws.send(JSON.stringify({ type: 'ping', timestamp: Date.now() }));
  }, 10000);
}

function clearSubscribeTimers(ws) {
  if (ws.pingTimer)       clearInterval(ws.pingTimer);
  if (ws.transcriptTimer) clearTimeout(ws.transcriptTimer);
}

// ─── Keep-alive ───────────────────────────────────────────────────────────────
function startKeepAlive(ws) {
  ws.keepAliveTimer = setInterval(() => {
    if (ws.readyState !== WebSocket.OPEN) {
      clearInterval(ws.keepAliveTimer);
      return;
    }
    send(ws, {
      msg_type:   MSG.KEEP_ALIVE_REQ,
      session_id: ws.sessionId,
      timestamp:  Date.now(),
    });
  }, 10000); // every 10 s
}

// ─── Clean up timers ──────────────────────────────────────────────────────────
function clearTimers(ws) {
  if (ws.keepAliveTimer)   clearInterval(ws.keepAliveTimer);
  if (ws.transcriptTimer)  clearTimeout(ws.transcriptTimer);
}

// ─── Start ────────────────────────────────────────────────────────────────────
httpServer.listen(PORT, () => {
  console.log('');
  console.log('╔══════════════════════════════════════════════════╗');
  console.log(`║  Mock RTMS Data WebSocket Server                 ║`);
  console.log(`║  HTTP  → http://localhost:${PORT}                   ║`);
  console.log(`║  WS    → ws://localhost:${PORT}/signaling           ║`);
  console.log(`║  WS    → ws://localhost:${PORT}/subscribe?touchpoint ║`);
  console.log('╠══════════════════════════════════════════════════╣');
  console.log(`║  POST /auth                                      ║`);
  console.log(`║  GET  /health                                    ║`);
  console.log(`║  GET  /touchpoints                               ║`);
  console.log(`║  GET  /meetings                                  ║`);
  console.log(`║  GET  /transcripts                               ║`);
  console.log(`║  GET /transcripts/:meetingUuid                   ║`);
  console.log(`║  GET /calendar-invites                           ║`);
  console.log(`║  GET /calendar-invites/:id                       ║`);
  console.log('╚══════════════════════════════════════════════════╝');
  console.log('');
  console.log(`Loaded ${TRANSCRIPTS.sessions.length} transcript sessions`);
  console.log(`Loaded ${CALENDAR_DATA.calendar_invites.length} calendar invites`);
  console.log('');
});

module.exports = { app, wss, httpServer };
