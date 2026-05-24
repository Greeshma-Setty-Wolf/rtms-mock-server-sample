'use strict';

/**
 * meetingAssistant.js
 *
 * Implements the Meeting Assistant protocol on top of two WebSockets:
 *
 *   /events            lifecycle events (meeting.started, rtms_started, …)
 *   /rtms-transcript   full message protocol (subscribe, transcript,
 *                      question.detected, question.updated, ack, …)
 *
 * Outgoing → server
 *   subscribe                { type, data: { stream_id, meeting_uuid } }
 *   question.answer.request  { type, id, data: { question_id, content: { text } } }
 *   question.edit.request    { type, id, data: { question_id, text } }
 *   question.hide.request    { type, id, data: { question_id, hide } }
 *
 * Incoming ← server (all routed by msg.type)
 *   session.sync          bulk-load (initial state)
 *   session.sync_complete switch to live mode
 *   transcript            append to transcript feed
 *   question.detected     add card to questions panel
 *   question.updated      update existing question card
 *   ack                   resolve pending request
 */

(function () {

  // ── State ─────────────────────────────────────────────────────────────────
  const state = {
    token:           null,
    eventsWs:        null,
    transcriptWs:    null,
    eventCount:      0,
    transcriptCount: 0,
    questionCount:   0,
    // From meeting.rtms_started — sent with subscribe
    rtmsStreamId:    null,
    meetingUuid:     null,
    // question id → { id, text, speaker_name, timestamp_ms, status, answer? }
    questions:       new Map(),
  };

  // ── DOM refs ──────────────────────────────────────────────────────────────
  let dom = {};

  // ── Authenticate ──────────────────────────────────────────────────────────

  async function authenticate() {
    const clientId     = dom.clientId.value.trim();
    const clientSecret = dom.clientSecret.value.trim();
    if (!clientId || !clientSecret) { setTokenStatus('Fill in both fields', 'error'); return; }

    dom.authBtn.disabled = true;
    setTokenStatus('Requesting…', 'muted');

    try {
      const res  = await fetch('/oauth/token', {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify({ grant_type: 'client_credentials', client_id: clientId, client_secret: clientSecret }),
      });
      const data = await res.json();
      if (!res.ok || !data.access_token) throw new Error(data.error || 'Token request failed');

      state.token = data.access_token;
      setTokenStatus('Token acquired ✓', 'success');
      dom.connectBtn.disabled = false;
      appendRaw('in', 'POST /oauth/token', { token_type: data.token_type, expires_in: data.expires_in });
    } catch (err) {
      setTokenStatus(err.message, 'error');
    } finally {
      dom.authBtn.disabled = false;
    }
  }

  // ── Connect / Disconnect ──────────────────────────────────────────────────

  function connect() {
    const base = (dom.serverUrl.value || `ws://${window.location.host}`).replace(/\/$/, '');
    openEventsWs(base + '/events');
  }

  function disconnect() {
    silentClose('events');
    silentClose('transcript');
    dom.connectBtn.disabled    = false;
    dom.disconnectBtn.disabled = true;
    setDot('events',     'disconnected');
    setDot('transcript', 'disconnected');
    setBadge('events-status-badge',     'Idle', 'idle');
    setBadge('transcript-status-badge', 'Idle', 'idle');
  }

  // ── Events WebSocket (/events) ────────────────────────────────────────────

  function openEventsWs(url) {
    silentClose('events');
    setDot('events', 'connecting');
    setBadge('events-status-badge', 'Connecting…', 'connecting');

    const ws = new WebSocket(url);
    state.eventsWs = ws;

    ws.onopen = () => {
      setDot('events', 'connected');
      setBadge('events-status-badge', 'Live', 'connected');
      dom.connectBtn.disabled    = true;
      dom.disconnectBtn.disabled = false;
      clearPlaceholder('ma-events-feed');
      appendRaw('sys', url, 'Events WebSocket connected');
    };

    ws.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data);
        handleLifecycleEvent(msg);
        appendRaw('in', '/events', msg);
      } catch (_) { /* ignore malformed */ }
    };

    ws.onclose = () => {
      setDot('events', 'disconnected');
      setBadge('events-status-badge', 'Closed', 'idle');
      if (!dom.connectBtn.disabled) return;
      dom.connectBtn.disabled    = false;
      dom.disconnectBtn.disabled = true;
    };

    ws.onerror = () => {
      setDot('events', 'error');
      setBadge('events-status-badge', 'Error', 'error');
      appendRaw('err', url, 'WebSocket error');
    };
  }

  // ── Transcript WebSocket (/rtms-transcript) ───────────────────────────────

  function openTranscriptWs(url) {
    silentClose('transcript');
    setDot('transcript', 'connecting');
    setBadge('transcript-status-badge', 'Connecting…', 'connecting');

    const ws = new WebSocket(url);
    state.transcriptWs = ws;

    ws.onopen = () => {
      setDot('transcript', 'connected');
      setBadge('transcript-status-badge', 'Connected', 'connected');
      clearPlaceholder('ma-transcript-feed');
      appendRaw('sys', url, 'Transcript WebSocket connected');

      // Send subscribe immediately — server starts streaming after this
      const subMsg = {
        type: 'subscribe',
        data: {
          stream_id:    state.rtmsStreamId || 'mock-stream',
          meeting_uuid: state.meetingUuid  || 'mock-meeting',
        },
      };
      ws.send(JSON.stringify(subMsg));
      appendRaw('out', '/rtms-transcript', subMsg);
    };

    ws.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data);
        routeTranscriptMessage(msg);
      } catch (_) { /* ignore */ }
    };

    ws.onclose = () => {
      setDot('transcript', 'disconnected');
      setBadge('transcript-status-badge', 'Closed', 'idle');
    };

    ws.onerror = () => {
      setDot('transcript', 'error');
      setBadge('transcript-status-badge', 'Error', 'error');
    };
  }

  function silentClose(type) {
    const key = type === 'events' ? 'eventsWs' : 'transcriptWs';
    if (state[key]) {
      state[key].onclose = null;
      state[key].close();
      state[key] = null;
    }
  }

  // ── Lifecycle event handler (/events messages) ────────────────────────────

  function handleLifecycleEvent(msg) {
    state.eventCount++;
    dom.statEvents.textContent = state.eventCount;
    renderEvent(msg);

    if (msg.event === 'meeting.rtms_started') {
      const obj = (msg.payload && msg.payload.object) || {};
      // Store for subscribe message
      state.rtmsStreamId = obj.rtms_stream_id || null;
      state.meetingUuid  = obj.uuid           || null;
      const transcriptUrl = obj.server_urls;
      if (transcriptUrl) openTranscriptWs(transcriptUrl);
    }

    if (msg.event === 'meeting.ended') {
      silentClose('transcript');
      setDot('transcript', 'disconnected');
      setBadge('transcript-status-badge', 'Ended', 'idle');
    }
  }

  // ── Transcript protocol router (/rtms-transcript messages) ───────────────

  function routeTranscriptMessage(msg) {
    // Log everything except high-frequency transcript chunks (too noisy)
    if (msg.type !== 'transcript') {
      appendRaw('in', '/rtms-transcript', msg);
    }

    switch (msg.type) {
      case 'session.sync':
        handleSessionSync(msg);
        break;
      case 'session.sync_complete':
        setBadge('transcript-status-badge', 'Streaming', 'connected');
        break;
      case 'transcript':
        handleTranscript(msg);
        break;
      case 'question.detected':
        handleQuestionDetected(msg);
        break;
      case 'question.updated':
        handleQuestionUpdated(msg);
        break;
      case 'ack':
        handleAck(msg);
        break;
      default:
        console.log('[MA] unknown transcript msg type:', msg.type);
    }
  }

  function handleSessionSync(msg) {
    const events = (msg.data && msg.data.events) || [];
    console.log(`[MA] session.sync — ${events.length} historical events`);
  }

  function handleTranscript(msg) {
    const data = msg.data || {};
    state.transcriptCount++;
    dom.statTranscript.textContent = state.transcriptCount;
    renderTranscript(data);
  }

  function handleQuestionDetected(msg) {
    const q = { id: msg.id, ...msg.data };
    state.questions.set(msg.id, q);
    state.questionCount++;
    dom.statQuestions.textContent = state.questionCount;
    clearPlaceholder('ma-questions-feed');
    renderQuestionCard(msg.id);
  }

  function handleQuestionUpdated(msg) {
    const q = state.questions.get(msg.id);
    if (q) {
      Object.assign(q, msg.data);
      renderQuestionCard(msg.id);
    }
  }

  function handleAck(msg) {
    console.log(`[MA] ack  request_id=${msg.request_id}  status=${msg.status}`);
  }

  // ── Question actions (sent to server) ────────────────────────────────────

  function sendQuestionAction(type, questionId, extra) {
    const ws = state.transcriptWs;
    if (!ws || ws.readyState !== WebSocket.OPEN) return;

    const reqId = `req-${Date.now()}`;
    const msg   = { type, id: reqId, data: { question_id: questionId, ...extra } };
    ws.send(JSON.stringify(msg));
    appendRaw('out', '/rtms-transcript', msg);
  }

  // ── Render: meeting events ────────────────────────────────────────────────

  const EVENT_META = {
    'meeting.started':            { icon: 'fa-play-circle',    color: '#5fca5f' },
    'meeting.participant_joined': { icon: 'fa-user-plus',      color: '#2d8cff' },
    'meeting.rtms_started':       { icon: 'fa-satellite-dish', color: '#f0a500' },
    'meeting.rtms_stopped':       { icon: 'fa-satellite-dish', color: '#e05252' },
    'meeting.ended':              { icon: 'fa-stop-circle',    color: '#e05252' },
  };

  function renderEvent(msg) {
    const feed = document.getElementById('ma-events-feed');
    const meta = EVENT_META[msg.event] || { icon: 'fa-circle', color: '#666' };
    const ts   = new Date().toLocaleTimeString();
    const obj  = (msg.payload && msg.payload.object) || {};

    let detail = '';
    if (msg.event === 'meeting.rtms_started' && obj.server_urls)
      detail = `<span class="event-detail">→ <code>${esc(obj.server_urls)}</code></span>`;
    else if (msg.event === 'meeting.participant_joined' && obj.participant)
      detail = `<span class="event-detail">${esc(obj.participant.user_name || '')}</span>`;
    else if (obj.topic)
      detail = `<span class="event-detail">${esc(obj.topic)}</span>`;

    const row = document.createElement('div');
    row.className = 'event-row';
    row.innerHTML =
      `<span class="event-ts">${ts}</span>` +
      `<span class="event-icon"><i class="fas ${meta.icon}" style="color:${meta.color}"></i></span>` +
      `<span class="event-name">${esc(msg.event)}</span>` + detail;
    feed.appendChild(row);
    feed.scrollTop = feed.scrollHeight;
  }

  // ── Render: transcript ────────────────────────────────────────────────────

  function renderTranscript(data) {
    const feed = document.getElementById('ma-transcript-feed');
    const ts   = new Date().toLocaleTimeString();
    const isQ  = data.text && data.text.trim().endsWith('?');

    const row = document.createElement('div');
    row.className = 'transcript-row' + (isQ ? ' transcript-row--question' : '');
    row.innerHTML =
      `<span class="tr-ts">${ts}</span>` +
      (isQ ? `<span class="tr-q-badge">Q</span>` : '') +
      `<span class="tr-speaker">${esc(data.speaker_name)}:</span>` +
      `<span class="tr-text">${esc(data.text)}</span>`;
    feed.appendChild(row);
    feed.scrollTop = feed.scrollHeight;
  }

  // ── Render: question card ─────────────────────────────────────────────────

  function renderQuestionCard(id) {
    const q    = state.questions.get(id);
    if (!q) return;

    const feed = document.getElementById('ma-questions-feed');
    let card   = document.getElementById('qcard-' + id);
    const isNew = !card;

    if (isNew) {
      card = document.createElement('div');
      card.id        = 'qcard-' + id;
      card.className = 'qcard';
      feed.prepend(card); // newest question at the top
    }

    const isAnswered = q.status === 'answered';
    const ts = new Date(q.timestamp_ms).toLocaleTimeString();

    card.innerHTML =
      `<div class="qcard-header">` +
        `<span class="qcard-speaker">${esc(q.speaker_name)}</span>` +
        `<span class="qcard-ts">${ts}</span>` +
        `<span class="badge ${isAnswered ? 'badge-connected' : 'badge-connecting'}">${isAnswered ? 'answered' : 'unanswered'}</span>` +
      `</div>` +
      `<div class="qcard-text">${esc(q.text)}</div>` +
      (isAnswered && q.answer
        ? `<div class="qcard-answer"><i class="fas fa-robot qcard-ai-icon"></i>${esc(q.answer)}</div>`
        : '') +
      `<div class="qcard-actions">` +
        (!isAnswered
          ? `<button class="btn btn-primary qcard-btn" data-action="answer" data-id="${esc(id)}" data-text="${esc(q.text)}">` +
              `<i class="fas fa-magic"></i> Request Answer` +
            `</button>`
          : '') +
        `<button class="btn btn-secondary qcard-btn" data-action="hide" data-id="${esc(id)}">` +
          `<i class="fas fa-eye-slash"></i> Hide` +
        `</button>` +
      `</div>`;
  }

  // ── Question button delegation ────────────────────────────────────────────

  function onQuestionFeedClick(e) {
    const btn = e.target.closest('.qcard-btn');
    if (!btn) return;

    const action = btn.dataset.action;
    const qId    = btn.dataset.id;

    if (action === 'answer') {
      sendQuestionAction('question.answer.request', qId, {
        content: { text: btn.dataset.text },
      });
      // Optimistically mark as "pending" so the button disappears
      const q = state.questions.get(qId);
      if (q) { q.status = 'pending'; renderQuestionCard(qId); }
    }

    if (action === 'hide') {
      sendQuestionAction('question.hide.request', qId, { hide: true });
      const card = document.getElementById('qcard-' + qId);
      if (card) card.style.opacity = '0.3';
    }
  }

  // ── Render: raw log ───────────────────────────────────────────────────────

  function appendRaw(dir, path, data) {
    const feed    = document.getElementById('ma-raw-feed');
    const ts      = new Date().toLocaleTimeString();
    const icon    = dir === 'out' ? '→' : dir === 'in' ? '←' : dir === 'err' ? '✗' : '·';
    const body    = typeof data === 'object' ? JSON.stringify(data) : String(data);
    const preview = body.length > 110 ? body.slice(0, 110) + '…' : body;

    const row = document.createElement('div');
    row.className = 'raw-row';
    row.innerHTML =
      `<span class="raw-ts">${ts}</span>` +
      `<span class="raw-dir">${icon}</span>` +
      `<span class="raw-path">${esc(path)}</span>` +
      `<span class="raw-body">${esc(preview)}</span>`;
    feed.appendChild(row);
    feed.scrollTop = feed.scrollHeight;
  }

  // ── Status / UI helpers ───────────────────────────────────────────────────

  function setDot(id, s) {
    const el = document.getElementById(id + '-dot');
    if (!el) return;
    el.style.background = { connected: '#5fca5f', connecting: '#f0a500', disconnected: '#333', error: '#e05252' }[s] || '#333';
    el.title = s;
  }

  function setBadge(id, label, type) {
    const el = document.getElementById(id);
    if (!el) return;
    el.textContent = label;
    el.className   = 'badge badge-' + type;
  }

  function setTokenStatus(msg, type) {
    const el = document.getElementById('ma-token-status');
    if (!el) return;
    el.textContent = msg;
    el.className   = 'status-text status-text--' + type;
  }

  function clearPlaceholder(id) {
    const el = document.getElementById(id);
    if (!el) return;
    const ph = el.querySelector('.feed-placeholder');
    if (ph) ph.remove();
  }

  function esc(s) {
    return String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }

  // ── Init ──────────────────────────────────────────────────────────────────

  function init() {
    dom = {
      clientId:      document.getElementById('ma-client-id'),
      clientSecret:  document.getElementById('ma-client-secret'),
      authBtn:       document.getElementById('ma-auth-btn'),
      serverUrl:     document.getElementById('ma-server-url'),
      connectBtn:    document.getElementById('ma-connect-btn'),
      disconnectBtn: document.getElementById('ma-disconnect-btn'),
      statEvents:    document.getElementById('stat-events'),
      statTranscript: document.getElementById('stat-transcript'),
      statQuestions:  document.getElementById('stat-questions'),
    };

    if (dom.serverUrl) dom.serverUrl.value = `ws://${window.location.host}`;

    dom.authBtn.addEventListener('click', authenticate);
    dom.connectBtn.addEventListener('click', connect);
    dom.disconnectBtn.addEventListener('click', disconnect);

    document.getElementById('ma-clear-raw').addEventListener('click', () => {
      document.getElementById('ma-raw-feed').innerHTML = '';
    });

    // Event delegation for question action buttons
    document.getElementById('ma-questions-feed').addEventListener('click', onQuestionFeedClick);
  }

  document.addEventListener('DOMContentLoaded', init);

})();
