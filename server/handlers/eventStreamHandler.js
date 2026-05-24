'use strict';

/**
 * EventStreamHandler — /events WebSocket
 *
 * When the app's Event Subscriber connects, this handler automatically
 * fires the full Zoom meeting lifecycle sequence with realistic delays:
 *
 *   meeting.started
 *   meeting.participant_joined
 *   meeting.rtms_started   ← includes the /rtms-transcript URL
 *   meeting.rtms_stopped
 *   meeting.ended
 *
 * The app only needs to open the WebSocket; no trigger is required.
 */

const WebSocket = require('ws');
const CONFIG = require('../config/serverConfig');
const CredentialsManager = require('../utils/credentialsManager');

// Session length before wrapping up (ms)
const SESSION_MS = 90_000;

// Delays (ms) from connection open
const DELAY = {
  MEETING_STARTED:      1_000,
  PARTICIPANT_JOINED:   3_000,
  RTMS_STARTED:         5_000,
  RTMS_STOPPED:         SESSION_MS,
  MEETING_ENDED:        SESSION_MS + 2_000,
};

class EventStreamHandler {
  /**
   * @param {WebSocket} ws
   * @param {import('http').IncomingMessage} _req
   */
  static handleConnection(ws, _req) {
    console.log('[EventStream] client connected');

    const creds      = CredentialsManager.loadCredentials();
    const meeting    = pickRandom(creds.stream_meeting_info);
    const credential = creds.auth_credentials[0];

    const meetingObj = {
      id:         deriveMeetingId(meeting.meeting_uuid),
      uuid:       meeting.meeting_uuid,
      host_id:    credential.userID,
      topic:      'Mock Zoom Meeting',
      start_time: new Date().toISOString(),
    };

    const timers = [];

    const at = (delayMs, buildPayload) => {
      const t = setTimeout(() => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify(buildPayload()));
        }
      }, delayMs);
      timers.push(t);
    };

    // ── meeting.started ────────────────────────────────────────────────────
    at(DELAY.MEETING_STARTED, () => ({
      event:    'meeting.started',
      event_ts: Date.now(),
      payload:  { object: { ...meetingObj } },
    }));

    // ── meeting.participant_joined ─────────────────────────────────────────
    at(DELAY.PARTICIPANT_JOINED, () => ({
      event:    'meeting.participant_joined',
      event_ts: Date.now(),
      payload:  {
        object: {
          ...meetingObj,
          participant: {
            user_id:   'usr001',
            user_name: 'Alice Chen',
            email:     'alice@example.com',
            join_time: new Date().toISOString(),
          },
        },
      },
    }));

    // ── meeting.rtms_started ───────────────────────────────────────────────
    // Tells the app where to connect for live transcript data
    at(DELAY.RTMS_STARTED, () => {
      const host = resolveHost();
      return {
        event:    'meeting.rtms_started',
        event_ts: Date.now(),
        payload:  {
          object: {
            ...meetingObj,
            rtms_stream_id: meeting.rtms_stream_id,
            server_urls:    `ws://${host}/rtms-transcript`,
          },
        },
      };
    });

    // ── meeting.rtms_stopped ───────────────────────────────────────────────
    at(DELAY.RTMS_STOPPED, () => ({
      event:    'meeting.rtms_stopped',
      event_ts: Date.now(),
      payload:  {
        object: {
          ...meetingObj,
          rtms_stream_id: meeting.rtms_stream_id,
          stop_reason:    'STOP_BC_MEETING_ENDED',
        },
      },
    }));

    // ── meeting.ended ──────────────────────────────────────────────────────
    at(DELAY.MEETING_ENDED, () => ({
      event:    'meeting.ended',
      event_ts: Date.now(),
      payload:  {
        object: { ...meetingObj, end_time: new Date().toISOString() },
      },
    }));

    ws.on('close', () => {
      console.log('[EventStream] client disconnected — clearing timers');
      timers.forEach(clearTimeout);
    });

    ws.on('error', (err) => {
      console.error('[EventStream] error:', err.message);
      timers.forEach(clearTimeout);
    });
  }
}

// ── helpers ──────────────────────────────────────────────────────────────────

function pickRandom(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

/** Stable numeric-style meeting ID derived from the uuid. */
function deriveMeetingId(uuid) {
  let h = 5381;
  for (let i = 0; i < uuid.length; i++) h = ((h << 5) + h) ^ uuid.charCodeAt(i);
  return String(Math.abs(h) % 900_000_000 + 100_000_000);
}

/**
 * Resolve the host:port to embed in the transcript WebSocket URL.
 * Prefers localhost for browser clients on the same machine.
 */
function resolveHost() {
  const port = CONFIG.HANDSHAKE_PORT;
  return `localhost:${port}`;
}

module.exports = EventStreamHandler;
