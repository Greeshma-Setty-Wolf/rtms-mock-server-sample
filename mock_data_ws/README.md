# mock_data_ws

A self-contained WebSocket + HTTP server that **mimics Zoom RTMS behaviour** but streams data entirely from local JSON test fixtures. Use this instead of a live Zoom account during development and testing.

---

## Quick start

```bash
cd mock_data_ws
npm install
npm start
# Server listens on http://localhost:9095
```

---

## Architecture

```
mock_data_ws/
├── server.js                   ← Express + ws server
└── test_data/
    ├── transcripts.json        ← 3 mocked meeting transcript sessions
    └── calendar_invites.json   ← 5 mocked calendar invites
```

---

## HTTP endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Liveness check |
| GET | `/meetings` | List all mock meeting UUIDs and topics |
| GET | `/transcripts` | All transcript sessions |
| GET | `/transcripts/:meetingUuid` | Transcript for one meeting |
| GET | `/calendar-invites` | All calendar invites |
| GET | `/calendar-invites/:id` | Single invite (by `id` or `meeting_uuid`) |

---

## WebSocket endpoint

```
ws://localhost:9095/signaling
```

### Connection flow (mirrors Zoom RTMS protocol)

```
Client                              Server
  │                                   │
  │── SIGNALING_HAND_SHAKE_REQ ───────▶│  msg_type: 1
  │◀─ SIGNALING_HAND_SHAKE_RESP ───────│  msg_type: 2  (status_code: 200)
  │◀─ SESSION_STATE_UPDATE ────────────│  msg_type: 9  state: STARTED
  │◀─ CALENDAR_INVITE ─────────────────│  msg_type: 50 (if matching invite exists)
  │                                   │
  │── EVENT_SUBSCRIPTION ─────────────▶│  msg_type: 5
  │◀─ EVENT_UPDATE ────────────────────│  msg_type: 6  (subscription confirmed)
  │                                   │
  │◀─ MEDIA_DATA_TRANSCRIPT ───────────│  msg_type: 17 (one per entry, timed)
  │◀─ MEDIA_DATA_TRANSCRIPT ───────────│  ...
  │◀─ SESSION_STATE_UPDATE ────────────│  msg_type: 9  state: STOPPED (end of stream)
  │                                   │
  │◀─ KEEP_ALIVE_REQ ──────────────────│  msg_type: 12 (every 10 s)
  │── KEEP_ALIVE_RESP ─────────────────▶│  msg_type: 13
```

### Message type reference

| Value | Name | Direction |
|-------|------|-----------|
| 1 | `SIGNALING_HAND_SHAKE_REQ` | Client → Server |
| 2 | `SIGNALING_HAND_SHAKE_RESP` | Server → Client |
| 5 | `EVENT_SUBSCRIPTION` | Client → Server |
| 6 | `EVENT_UPDATE` | Server → Client |
| 9 | `SESSION_STATE_UPDATE` | Both |
| 12 | `KEEP_ALIVE_REQ` | Server → Client |
| 13 | `KEEP_ALIVE_RESP` | Client → Server |
| 17 | `MEDIA_DATA_TRANSCRIPT` | Server → Client |
| 50 | `CALENDAR_INVITE` *(custom)* | Server → Client |

---

## Example handshake message (client sends)

```json
{
  "msg_type": 1,
  "meeting_uuid": "TNhvT3WEBT6Srse3TgWRGr",
  "rtms_stream_id": "rtms_TN3WEBT6SrTgWRGr_001",
  "signature": "any-non-empty-string"
}
```

Pass any `meeting_uuid` from the list returned by `GET /meetings`. The server will:
1. Validate fields are present
2. Respond with a session ID
3. Push the matching calendar invite (if one exists)
4. Start streaming transcript entries at their original timing

---

## Transcript message format

```json
{
  "msg_type": 17,
  "session_id": "a1b2c3d4e5f6a7b8",
  "timestamp": 1716123456789,
  "content": {
    "user_id": 101,
    "display_name": "Alice Chen",
    "language": "en-US",
    "data": "Good morning everyone, let's get started.",
    "is_final": true,
    "sequence": 1
  }
}
```

## Calendar invite message format

```json
{
  "msg_type": 50,
  "session_id": "a1b2c3d4e5f6a7b8",
  "timestamp": 1716123456789,
  "content": {
    "id": "cal_invite_001",
    "meeting_uuid": "TNhvT3WEBT6Srse3TgWRGr",
    "title": "Q2 Product Planning",
    "start_time": "2026-05-19T09:00:00Z",
    "end_time": "2026-05-19T10:00:00Z",
    "organizer": { "name": "Alice Chen", "email": "alice.chen@example.com" },
    "attendees": [ ... ],
    "conference": { "type": "zoom", "join_url": "https://zoom.us/j/..." }
  }
}
```

---

## Available mock meetings

| `meeting_uuid` | Topic |
|----------------|-------|
| `TNhvT3WEBT6Srse3TgWRGr` | Q2 Product Planning |
| `KLhvT3WEBT6Srse3TgWRGs` | Engineering Sprint Review — Sprint 24 |
| `PLhvT3WEBT6Srse3TgWRGt` | Customer Success — Quarterly Business Review |

---

## Connecting from the existing test_client

Set these env vars in `test_client/.env`:

```env
ZM_CLIENT_ID=XkWfgHHASGOQC9b95AkIxB
ZM_CLIENT_SECRET=YZnKVUufg7N18Oej6gHHqNWc7CG5jQ6N
```

Then modify `connectToSignalingWebSocket` to point at `ws://localhost:9095/signaling` instead of the Zoom-provided URL.
