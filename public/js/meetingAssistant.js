'use strict';

/**
 * meetingAssistant.js
 *
 * Self-contained module for the Meeting Assistant UI.
 *
 * Flow:
 *   1. User fills client_id + client_secret → clicks "Get Token"
 *      → POST /oauth/token → stores access_token, enables Connect
 *
 *   2. User clicks "Connect"
 *      → opens WebSocket ws://{server}/events
 *      → server fires lifecycle events every few seconds
 *
 *   3. On meeting.rtms_started event:
 *      → automatically opens WebSocket to server_urls in the event payload
 *      → server streams transcript chunks every 3 s
 *
 *   4. User clicks "Disconnect" (or meeting.ended arrives)
 *      → closes both WebSocket connections
 */

(function () {

  // ── State ─────────────────────────────────────────────────────────────────
  const state = {
    token:           null,
    eventsWs:        null,
    transcriptWs:    null,
    eventCount:      0,
    transcriptCount: 0,
  };

  // ── DOM refs (populated in init) ──────────────────────────────────────────
  let dom = {};

  // ── Authenticate ──────────────────────────────────────────────────────────

  async function authenticate() {
    const clientId     = dom.clientId.value.trim();
    const clientSecret = dom.clientSecret.value.trim();

    if (!clientId || !clientSecret) {
      setTokenStatus('Fill in both fields', 'error');
      return;
    }

    dom.authBtn.disabled = true;
    setTokenStatus('Requesting…', 'muted');

    try {
      const res  = await fetch('/oauth/token', {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify({
          grant_type:    'client_credentials',
          client_id:     clientId,
          client_secret: clientSecret,
        }),
      });

      const data = await res.json();

      if (!res.ok || !data.access_token) {
        throw new Error(data.error || 'Token request failed');
      }

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
      // Re-enable Connect only if it was closed unexpectedly
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
      setBadge('transcript-status-badge', 'Streaming', 'connected');
      clearPlaceholder('ma-transcript-feed');
      appendRaw('sys', url, 'Transcript WebSocket connected');
    };

    ws.onmessage = (e) => {
      try {
        const chunk = JSON.parse(e.data);
        handleTranscriptChunk(chunk);
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
      state[key].onclose = null; // suppress default handler
      state[key].close();
      state[key] = null;
    }
  }

  // ── Message handlers ──────────────────────────────────────────────────────

  function handleLifecycleEvent(msg) {
    state.eventCount++;
    dom.statEvents.textContent = state.eventCount;
    renderEvent(msg);

    // Auto-connect transcript stream when RTMS starts
    if (msg.event === 'meeting.rtms_started') {
      const transcriptUrl = msg.payload && msg.payload.object && msg.payload.object.server_urls;
      if (transcriptUrl) {
        openTranscriptWs(transcriptUrl);
      }
    }

    // Clean up when the meeting ends
    if (msg.event === 'meeting.ended') {
      silentClose('transcript');
      setDot('transcript', 'disconnected');
      setBadge('transcript-status-badge', 'Ended', 'idle');
    }
  }

  function handleTranscriptChunk(chunk) {
    state.transcriptCount++;
    dom.statTranscript.textContent = state.transcriptCount;
    renderTranscript(chunk);
  }

  // ── Render: events ────────────────────────────────────────────────────────

  const EVENT_META = {
    'meeting.started':            { icon: 'fa-play-circle',     color: '#5fca5f' },
    'meeting.participant_joined': { icon: 'fa-user-plus',       color: '#2d8cff' },
    'meeting.rtms_started':       { icon: 'fa-satellite-dish',  color: '#f0a500' },
    'meeting.rtms_stopped':       { icon: 'fa-satellite-dish',  color: '#e05252' },
    'meeting.ended':              { icon: 'fa-stop-circle',     color: '#e05252' },
  };

  function renderEvent(msg) {
    const feed = document.getElementById('ma-events-feed');
    const meta = EVENT_META[msg.event] || { icon: 'fa-circle', color: '#666' };
    const ts   = new Date().toLocaleTimeString();
    const obj  = (msg.payload && msg.payload.object) || {};

    let detail = '';
    if (msg.event === 'meeting.rtms_started' && obj.server_urls) {
      detail = `<span class="event-detail">→ <code>${esc(obj.server_urls)}</code></span>`;
    } else if (msg.event === 'meeting.participant_joined' && obj.participant) {
      detail = `<span class="event-detail">${esc(obj.participant.user_name || '')}</span>`;
    } else if (obj.topic) {
      detail = `<span class="event-detail">${esc(obj.topic)}</span>`;
    }

    const row = document.createElement('div');
    row.className = 'event-row';
    row.innerHTML =
      `<span class="event-ts">${ts}</span>` +
      `<span class="event-icon"><i class="fas ${meta.icon}" style="color:${meta.color}"></i></span>` +
      `<span class="event-name">${esc(msg.event)}</span>` +
      detail;

    feed.appendChild(row);
    feed.scrollTop = feed.scrollHeight;
  }

  // ── Render: transcript ────────────────────────────────────────────────────

  function renderTranscript(chunk) {
    const feed = document.getElementById('ma-transcript-feed');
    const ts   = new Date().toLocaleTimeString();
    const isQ  = chunk.text && chunk.text.trim().endsWith('?');

    const row = document.createElement('div');
    row.className = 'transcript-row' + (isQ ? ' transcript-row--question' : '');
    row.innerHTML =
      `<span class="tr-ts">${ts}</span>` +
      (isQ ? `<span class="tr-q-badge">Q</span>` : '') +
      `<span class="tr-speaker">${esc(chunk.speaker_name)}:</span>` +
      `<span class="tr-text">${esc(chunk.text)}</span>`;

    feed.appendChild(row);
    feed.scrollTop = feed.scrollHeight;
  }

  // ── Render: raw log ───────────────────────────────────────────────────────

  function appendRaw(dir, path, data) {
    const feed = document.getElementById('ma-raw-feed');
    const ts   = new Date().toLocaleTimeString();
    const icon = dir === 'in' ? '←' : dir === 'err' ? '✗' : '·';
    const body = typeof data === 'object' ? JSON.stringify(data) : String(data);
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

  function setDot(id, statusState) {
    const el = document.getElementById(id + '-dot');
    if (!el) return;
    const colors = {
      connected:    '#5fca5f',
      connecting:   '#f0a500',
      disconnected: '#333',
      error:        '#e05252',
    };
    el.style.background = colors[statusState] || '#333';
    el.title = statusState;
  }

  function setBadge(id, label, type) {
    const el = document.getElementById(id);
    if (!el) return;
    el.textContent = label;
    el.className = 'badge badge-' + type;
  }

  function setTokenStatus(msg, type) {
    const el = document.getElementById('ma-token-status');
    if (!el) return;
    el.textContent = msg;
    el.className = 'status-text status-text--' + type;
  }

  function clearPlaceholder(id) {
    const el = document.getElementById(id);
    if (!el) return;
    const ph = el.querySelector('.feed-placeholder');
    if (ph) ph.remove();
  }

  function esc(s) {
    return String(s || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
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
    };

    // Pre-fill server URL from the page's own host
    if (dom.serverUrl) {
      dom.serverUrl.value = `ws://${window.location.host}`;
    }

    dom.authBtn.addEventListener('click', authenticate);
    dom.connectBtn.addEventListener('click', connect);
    dom.disconnectBtn.addEventListener('click', disconnect);

    document.getElementById('ma-clear-raw').addEventListener('click', () => {
      document.getElementById('ma-raw-feed').innerHTML = '';
    });
  }

  document.addEventListener('DOMContentLoaded', init);

})();
