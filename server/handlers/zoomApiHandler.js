'use strict';

/**
 * Zoom API Mock Router
 *
 * Two HTTP endpoints the app calls against "Zoom":
 *
 *   POST /oauth/token
 *     Accepts any client_credentials request → returns a valid-looking
 *     { access_token, refresh_token, token_type, expires_in, scope }
 *
 *   PATCH /v2/live_meetings/:meetingId/rtms_app/status
 *     Acknowledges the RTMS start request → returns { stream_id, status }
 *
 * Both always succeed so the app's auth and RTMS-start flows work in testing.
 */

const express = require('express');
const crypto  = require('crypto');
const CredentialsManager = require('../utils/credentialsManager');

const router = express.Router();

// ── POST /oauth/token ─────────────────────────────────────────────────────────

router.post('/oauth/token', (req, res) => {
  const creds      = CredentialsManager.loadCredentials();
  const credential = creds.auth_credentials[0];

  const accessToken  = `mock_at_${credential.accountId}_${crypto.randomBytes(16).toString('hex')}`;
  const refreshToken = `mock_rt_${credential.accountId}_${crypto.randomBytes(16).toString('hex')}`;

  console.log(`[ZoomAPI] POST /oauth/token  account=${credential.accountId}`);

  res.json({
    access_token:  accessToken,
    refresh_token: refreshToken,
    token_type:    'bearer',
    expires_in:    3600,
    scope:         'meeting:read meeting:write rtms',
  });
});

// ── PATCH /v2/live_meetings/:meetingId/rtms_app/status ────────────────────────

router.patch('/v2/live_meetings/:meetingId/rtms_app/status', (req, res) => {
  const { meetingId } = req.params;

  const creds      = CredentialsManager.loadCredentials();
  const meetingInfo =
    creds.stream_meeting_info.find(m => m.meeting_uuid === meetingId)
    || creds.stream_meeting_info[0];

  console.log(`[ZoomAPI] PATCH /v2/live_meetings/${meetingId}/rtms_app/status  stream=${meetingInfo.rtms_stream_id}`);

  res.json({
    stream_id: meetingInfo.rtms_stream_id,
    status:    'started',
  });
});

module.exports = router;
