/**
 * mock_data_ws/test_client.js
 *
 * Quick CLI test: connects to the mock RTMS WebSocket, performs the handshake,
 * subscribes to events, then prints every transcript line and calendar invite
 * it receives until the session ends.
 *
 * Usage:
 *   node test_client.js [meetingUuid]
 *
 * Example:
 *   node test_client.js TNhvT3WEBT6Srse3TgWRGr
 */

'use strict';

const WebSocket = require('ws');
const crypto    = require('crypto');

const SERVER_URL   = 'ws://localhost:9095/signaling';
const meetingUuid  = process.argv[2] || 'TNhvT3WEBT6Srse3TgWRGr';
const streamId     = 'rtms_mock_stream_001';

// Message type map (mirrors server constants)
const MSG_NAMES = {
  1:  'SIGNALING_HAND_SHAKE_REQ',
  2:  'SIGNALING_HAND_SHAKE_RESP',
  5:  'EVENT_SUBSCRIPTION',
  6:  'EVENT_UPDATE',
  9:  'SESSION_STATE_UPDATE',
  12: 'KEEP_ALIVE_REQ',
  13: 'KEEP_ALIVE_RESP',
  17: 'MEDIA_DATA_TRANSCRIPT',
  50: 'CALENDAR_INVITE',
};

const SESSION_STATE_NAMES = { 1: 'STARTED', 2: 'PAUSED', 3: 'RESUMED', 4: 'STOPPED' };

function label(msgType) {
  return MSG_NAMES[msgType] || `UNKNOWN(${msgType})`;
}

function dim(s)    { return `\x1b[2m${s}\x1b[0m`; }
function cyan(s)   { return `\x1b[36m${s}\x1b[0m`; }
function green(s)  { return `\x1b[32m${s}\x1b[0m`; }
function yellow(s) { return `\x1b[33m${s}\x1b[0m`; }
function bold(s)   { return `\x1b[1m${s}\x1b[0m`; }

console.log(bold('\n── Mock RTMS WebSocket Test Client ──'));
console.log(`Connecting to ${cyan(SERVER_URL)}`);
console.log(`Meeting UUID : ${yellow(meetingUuid)}\n`);

const ws = new WebSocket(SERVER_URL);

ws.on('open', () => {
  console.log(green('✓ Connected'));

  // Step 1: Send handshake
  const handshake = {
    msg_type:       1,
    meeting_uuid:   meetingUuid,
    rtms_stream_id: streamId,
    // In production this would be HMAC-signed; any non-empty string works here
    signature: crypto.randomBytes(16).toString('hex'),
  };
  ws.send(JSON.stringify(handshake));
  console.log(dim(`→ ${label(1)}`));
});

ws.on('message', (raw) => {
  let msg;
  try { msg = JSON.parse(raw); }
  catch { return; }

  const name = label(msg.msg_type);

  switch (msg.msg_type) {
    // ── Handshake response ──────────────────────────────────────────────────
    case 2: {
      if (msg.status_code === 200) {
        console.log(green(`✓ Handshake accepted`) + dim(` (session: ${msg.session_id})`));
        // Step 2: Subscribe to transcript events
        ws.send(JSON.stringify({
          msg_type:   5,
          session_id: msg.session_id,
          event_type: ['transcript', 'chat'],
        }));
        console.log(dim(`→ EVENT_SUBSCRIPTION`));
      } else {
        console.error(`✗ Handshake rejected: ${msg.status_message}`);
        ws.close();
      }
      break;
    }

    // ── Session state ───────────────────────────────────────────────────────
    case 9: {
      const stateName = SESSION_STATE_NAMES[msg.state] || msg.state;
      const icon = msg.state === 4 ? '■' : '●';
      console.log(`\n${icon} Session ${bold(stateName)}${msg.stop_reason ? ` (${msg.stop_reason})` : ''}`);
      if (msg.state === 4) {
        console.log('\nStream finished. Closing connection.\n');
        ws.close();
      }
      break;
    }

    // ── Calendar invite ─────────────────────────────────────────────────────
    case 50: {
      const inv = msg.content;
      console.log('\n' + bold('📅 Calendar Invite'));
      console.log(`   Title    : ${cyan(inv.title)}`);
      console.log(`   Start    : ${inv.start_time}`);
      console.log(`   End      : ${inv.end_time}`);
      console.log(`   Organizer: ${inv.organizer.name} <${inv.organizer.email}>`);
      console.log(`   Attendees: ${inv.attendees.map(a => a.name).join(', ')}`);
      if (inv.conference) {
        console.log(`   Join URL : ${inv.conference.join_url}`);
      }
      console.log('');
      break;
    }

    // ── Transcript ──────────────────────────────────────────────────────────
    case 17: {
      const c = msg.content;
      const ts = new Date(msg.timestamp).toISOString().substr(11, 8);
      console.log(`[${dim(ts)}] ${bold(c.display_name)}: ${c.data}`);
      break;
    }

    // ── Keep-alive ──────────────────────────────────────────────────────────
    case 12: {
      ws.send(JSON.stringify({ msg_type: 13, session_id: msg.session_id, timestamp: Date.now() }));
      process.stdout.write(dim('.'));  // subtle heartbeat indicator
      break;
    }

    // ── Event update ────────────────────────────────────────────────────────
    case 6: {
      console.log(green(`✓ Subscribed`) + dim(` — events: ${(msg.content.events || []).join(', ')}`));
      console.log(dim('\n─── Transcript stream starting ───\n'));
      break;
    }

    default:
      console.log(dim(`← ${name}`));
  }
});

ws.on('close', () => {
  console.log(dim('\nConnection closed.'));
  process.exit(0);
});

ws.on('error', (err) => {
  console.error(`\nWebSocket error: ${err.message}`);
  console.error('Make sure the server is running: npm start');
  process.exit(1);
});
