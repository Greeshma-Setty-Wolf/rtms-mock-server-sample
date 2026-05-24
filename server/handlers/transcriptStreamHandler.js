'use strict';

/**
 * TranscriptStreamHandler — /rtms-transcript WebSocket
 *
 * When the app's Meeting Handler connects, it immediately starts receiving
 * transcript chunks every ~3 seconds until the client disconnects.
 *
 * Mix: ~85% normal statements, ~15% questions — gives the app's
 * question-detection pipeline realistic input to work with.
 */

const WebSocket = require('ws');

const INTERVAL_MS = 3_000;

// ── Transcript corpus ─────────────────────────────────────────────────────────

const STATEMENTS = [
  'The Q2 revenue came in at 4.2 million, ahead of our original target.',
  'We have seen a 12% increase in customer retention this quarter.',
  'The new onboarding flow reduced drop-off by 8 percentage points.',
  'Engineering velocity improved significantly after the platform refactor.',
  'The partnership with Acme Corp is now fully live in production.',
  'Support ticket volume dropped 20% after the self-service portal launched.',
  'Infrastructure costs are trending down after the cloud cost review.',
  'The mobile app update shipped to both iOS and Android stores yesterday.',
  'We are tracking 94% uptime across all critical services this month.',
  'The design team has finalised the new dashboard mockups.',
  'Beta program feedback has been overwhelmingly positive so far.',
  'Sales pipeline for Q3 looks strong based on current projections.',
  'The data migration to the new warehouse is 80% complete.',
  'We hit 10,000 daily active users last Tuesday.',
  'The compliance audit completed last week with no major findings.',
];

const QUESTIONS = [
  'What is the SLA for incident response?',
  'Can you share the updated roadmap timeline?',
  'Has the compliance audit been fully completed?',
  'What is driving the spike in support tickets this week?',
  'Who is the DRI for the infrastructure migration?',
  'What is the Q2 revenue target we committed to the board?',
  'Can we move the release date up by two weeks?',
  'Are we on track to hit the annual headcount plan?',
];

const SPEAKERS = [
  { speaker_name: 'Kishor Patel',   speaker_id: 'def456' },
  { speaker_name: 'Alice Chen',     speaker_id: 'usr001' },
  { speaker_name: 'Bob Kumar',      speaker_id: 'usr002' },
  { speaker_name: 'Sarah Williams', speaker_id: 'usr003' },
];

class TranscriptStreamHandler {
  /**
   * @param {WebSocket} ws
   * @param {import('http').IncomingMessage} _req
   */
  static handleConnection(ws, _req) {
    console.log('[TranscriptStream] client connected');

    const timer = setInterval(() => {
      if (ws.readyState !== WebSocket.OPEN) {
        clearInterval(timer);
        return;
      }

      const chunk = buildChunk();
      console.log(`[TranscriptStream] → "${chunk.text.substring(0, 55)}…"`);
      ws.send(JSON.stringify(chunk));
    }, INTERVAL_MS);

    ws.on('close', () => {
      console.log('[TranscriptStream] client disconnected');
      clearInterval(timer);
    });

    ws.on('error', (err) => {
      console.error('[TranscriptStream] error:', err.message);
      clearInterval(timer);
    });
  }
}

// ── helpers ───────────────────────────────────────────────────────────────────

function buildChunk() {
  const isQuestion = Math.random() < 0.15;
  const text       = pickRandom(isQuestion ? QUESTIONS : STATEMENTS);
  const speaker    = pickRandom(SPEAKERS);

  return {
    text,
    speaker_name:  speaker.speaker_name,
    speaker_id:    speaker.speaker_id,
    language:      'en',
    timestamp_ms:  Date.now(),
    is_final:      true,
  };
}

function pickRandom(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

module.exports = TranscriptStreamHandler;
