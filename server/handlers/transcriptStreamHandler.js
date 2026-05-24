'use strict';

/**
 * TranscriptStreamHandler — /rtms-transcript WebSocket
 *
 * Protocol (all messages are JSON, routed by the "type" field):
 *
 * Outgoing (mock → client)
 *   session.sync          bulk-load existing events (sent immediately on connect)
 *   session.sync_complete switch to real-time mode
 *   transcript            one transcript chunk
 *   question.detected     a question was spotted in the transcript
 *   question.updated      question was answered by mock AI
 *   ack                   acknowledge a client request
 *
 * Incoming (client → mock)
 *   subscribe             start streaming  { data: { stream_id, meeting_uuid } }
 *   question.answer.request  { id, data: { question_id, content: { text } } }
 *   question.edit.request    { id, data: { question_id, text } }
 *   question.hide.request    { id, data: { question_id, hide } }
 */

const WebSocket = require('ws');

const CHUNK_INTERVAL_MS         = 3_000;
const QUESTION_ANSWER_DELAY_MS  = 2_500;

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

// Canned AI answers keyed by question text (partial match fallback used)
const ANSWERS = {
  'sla for incident response':        'P1 incidents require response within 1 hour and resolution within 4 hours per our current SLA.',
  'roadmap timeline':                 'Q3 focuses on platform stability, Q4 on growth features. Full deck linked in Confluence under /product/roadmap.',
  'compliance audit':                 'The audit closed last week with zero critical findings. Report is in SharePoint under Legal/Compliance.',
  'spike in support tickets':         'Root cause traced to a broken validation in the v2.4 release. A hotfix is scheduled for tomorrow.',
  'dri for the infrastructure':       'Priya Nair is the DRI. She can be reached on #infra-migration in Slack.',
  'q2 revenue target':                'The board commitment is 4.0 million. We came in at 4.2 million — 105% of target.',
  'move the release date':            'Engineering estimates a 10-day pull-forward is feasible if we drop two lower-priority items from the milestone.',
  'annual headcount plan':            'We are at 87% of plan. Three open reqs are in final interview rounds and expected to close this month.',
};

let _questionSeq = 0;

class TranscriptStreamHandler {
  /**
   * @param {WebSocket} ws
   * @param {import('http').IncomingMessage} _req
   */
  static handleConnection(ws, _req) {
    console.log('[TranscriptStream] client connected');

    let streamTimer  = null;
    let isSubscribed = false;

    // ── Send session.sync + session.sync_complete immediately ─────────────
    send(ws, { type: 'session.sync', data: { events: [] } });
    send(ws, { type: 'session.sync_complete' });

    // ── Incoming message dispatcher ───────────────────────────────────────
    ws.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw); } catch { return; }

      console.log(`[TranscriptStream] ← type=${msg.type}`);

      switch (msg.type) {
        case 'subscribe':
          handleSubscribe(msg);
          break;
        case 'question.answer.request':
          handleAnswerRequest(msg);
          break;
        case 'question.edit.request':
          handleEditRequest(msg);
          break;
        case 'question.hide.request':
          handleHideRequest(msg);
          break;
        default:
          console.log(`[TranscriptStream] unknown type: ${msg.type}`);
      }
    });

    // ── subscribe ─────────────────────────────────────────────────────────
    function handleSubscribe(msg) {
      const { stream_id, meeting_uuid } = (msg.data || {});
      console.log(`[TranscriptStream] subscribe  stream=${stream_id}  meeting=${meeting_uuid}`);
      if (isSubscribed) return;
      isSubscribed = true;

      streamTimer = setInterval(() => {
        if (ws.readyState !== WebSocket.OPEN) { clearInterval(streamTimer); return; }

        const isQuestion = Math.random() < 0.15;
        const text       = pickRandom(isQuestion ? QUESTIONS : STATEMENTS);
        const speaker    = pickRandom(SPEAKERS);
        const now        = Date.now();
        const trId       = `tr-${now}`;

        // Always send transcript
        send(ws, {
          type: 'transcript',
          id:   trId,
          data: {
            text,
            speaker_name:  speaker.speaker_name,
            speaker_id:    speaker.speaker_id,
            language:      'en',
            timestamp_ms:  now,
            is_final:      true,
          },
        });

        // If it's a question, also emit question.detected
        if (isQuestion) {
          const qId = `q-${++_questionSeq}-${now}`;
          send(ws, {
            type: 'question.detected',
            id:   qId,
            data: {
              text,
              speaker_name:  speaker.speaker_name,
              speaker_id:    speaker.speaker_id,
              timestamp_ms:  now,
              status:        'unanswered',
            },
          });
        }
      }, CHUNK_INTERVAL_MS);
    }

    // ── question.answer.request ───────────────────────────────────────────
    function handleAnswerRequest(msg) {
      const requestId  = msg.id;
      const questionId = msg.data && msg.data.question_id;
      const text       = (msg.data && msg.data.content && msg.data.content.text) || '';

      // Immediate ack
      send(ws, { type: 'ack', request_id: requestId, status: 'ok' });

      // Simulate AI thinking, then send answer
      setTimeout(() => {
        if (ws.readyState !== WebSocket.OPEN) return;
        send(ws, {
          type: 'question.updated',
          id:   questionId || requestId,
          data: {
            answer:     generateAnswer(text),
            status:     'answered',
            updated_at: Date.now(),
          },
        });
      }, QUESTION_ANSWER_DELAY_MS);
    }

    // ── question.edit.request ─────────────────────────────────────────────
    function handleEditRequest(msg) {
      send(ws, { type: 'ack', request_id: msg.id, status: 'ok' });
    }

    // ── question.hide.request ─────────────────────────────────────────────
    function handleHideRequest(msg) {
      send(ws, { type: 'ack', request_id: msg.id, status: 'ok' });
    }

    ws.on('close', () => {
      console.log('[TranscriptStream] client disconnected');
      if (streamTimer) clearInterval(streamTimer);
    });

    ws.on('error', (err) => {
      console.error('[TranscriptStream] error:', err.message);
      if (streamTimer) clearInterval(streamTimer);
    });
  }
}

// ── helpers ───────────────────────────────────────────────────────────────────

function send(ws, payload) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(payload));
  }
}

function pickRandom(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function generateAnswer(questionText) {
  const lower = questionText.toLowerCase();
  for (const [key, answer] of Object.entries(ANSWERS)) {
    if (lower.includes(key)) return answer;
  }
  return 'This question has been logged. Our team will follow up with a detailed response shortly.';
}

module.exports = TranscriptStreamHandler;
