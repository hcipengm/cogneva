# A2A interoperability guide

This page is for someone building an **external agent** that talks to a Cogneva
deployment over [A2A](https://a2a-protocol.org/). It describes what this
platform actually serves today — not what the specification permits.

Everything here was read off a running gateway, not off a design document. If
the two ever disagree, the gateway wins and this page is the bug.

## 1. Discovery

```
GET /api/v1/protocol/a2a/agent-card
```

The agent card is **public**: it answers without a credential, because a peer
that has never talked to you cannot have one yet, and the card is what tells it
which credential to get. The card carries no business data. Every endpoint it
names does require authentication.

```
$ curl -s https://<host>/api/v1/protocol/a2a/agent-card
{
  "name": "cogneva",
  "description": "Cogneva autonomous multi-agent collaboration platform",
  "version": "0.5.10",
  "url": "https://<host>/api/v1/protocol/a2a",
  "authentication": { "schemes": ["jwt"], "credentials": null },
  "capabilities": {
    "streaming": true,
    "push_notifications": false,
    "state_transition_history": true
  },
  "skills": [ ... ]
}
```

Two things to read carefully:

- **`url`** is the address the *caller* reached the platform at, resolved from
  the request's `Host` and `X-Forwarded-Proto`. That is the endpoint to call —
  not a bind address and not a fixed string.
- The card is sent with `Cache-Control: public, max-age=300`. Re-fetch it at
  most every five minutes; an address or capability change shows up within that
  window.

`capabilities.push_notifications` is `false` on purpose: this platform has no
endpoint that accepts a webhook the server would call back. Do not configure
one; nothing will arrive.

## 2. Authentication

The card declares `jwt`. The business endpoints (`message/send`,
`message/stream`, `tasks/get`, `tasks/cancel`) sit behind the platform's
operator role and answer `401`/`403` to a caller without a token.

Obtain a token the same way a human client does:

```
POST /api/v1/auth/login
Content-Type: application/json

{ "username": "<user>", "password": "<password>" }
```

or through the deployment's platform OAuth provider. Send it as:

```
Authorization: Bearer <token>
```

A deployment may also accept an `x-session-token` header for browser sessions;
a machine-to-machine peer does not need one.

## 3. One address, the method in the body

A2A dispatches on the JSON-RPC `method`, so the card's `url` serves all four
methods:

```
POST /api/v1/protocol/a2a
Content-Type: application/json
Authorization: Bearer <token>

{ "jsonrpc": "2.0", "id": 1, "method": "message/send", "params": { ... } }
```

`message/stream` is additionally served on its own path,
`/api/v1/protocol/a2a/message/stream`, because the card's `streaming` flag names
it. Both spellings behave the same.

| method | params | result |
|---|---|---|
| `message/send` | `{ "message": { "role": "user", "parts": [{"kind":"text","text":"..."}] } }` | the task row as accepted |
| `message/stream` | same | `text/event-stream` of frames |
| `tasks/get` | `{ "id": "<task id>" }` | the task row as it stands now |
| `tasks/cancel` | `{ "id": "<task id>" }` | the task row after the stop |

Parts of `kind: "text"` (older drafts use `type: "text"`) are joined into the
goal. A message with no non-empty text part is refused — it would otherwise
create a goal with nothing in it.

## 4. The task object

```json
{
  "id": "0e6a...",
  "status": "submitted",
  "messages": [],
  "artifacts": [],
  "created_at": "2026-10-23T00:05:00Z",
  "updated_at": "2026-10-23T00:05:00Z"
}
```

`status` is one of `submitted`, `working`, `input-required`, `completed`,
`canceled`, `failed`. The platform's internal states map onto these:

| platform state | A2A status |
|---|---|
| pending, scheduled | `submitted` |
| running | `working` |
| completed | `completed` |
| failed | `failed` |
| cancelled | `canceled` |

Artifacts appear only on terminal rows:

- `name: "result"` — one text part containing the run's output as JSON;
- `name: "error"` — one text part containing the failure reason.

Both may be present; read them by name, not by position.

## 5. Streaming

`message/stream` answers immediately with `200` and `content-type:
text/event-stream` (`cache-control: no-cache`, `x-accel-buffering: no`). Frames
use the event name `message`, and each `data:` line is a JSON-RPC message:

- the first frame is a **snapshot**: `kind: "task"` — the row as accepted;
- later frames are **status updates**: `kind: "status-update"`,
  `{ "id": ..., "status": ..., "final": bool }`, sent only when the state
  changes. The terminal frame is `final: true` and carries `artifacts`;
- the stream ends when the row reaches a terminal state, or after the
  deployment's maximum stream lifetime (default 30 minutes);
- if the row never becomes readable within that lifetime, the stream sends one
  JSON-RPC error frame (`-32001`) **before** closing, so "nothing happened" is
  distinguishable from "the run produced nothing".

If acceptance itself fails, the error frame is sent as the first and only frame
and the status is still `200` — a rejected goal and an empty stream must not
look alike.

## 6. Errors

RPC-level failures come back as HTTP `200` with a JSON-RPC error envelope, so a
peer must read the body, not just the status:

| code | meaning |
|---|---|
| `-32601` | unknown method; the message names what is served |
| `-32602` | a required param is missing (e.g. `params.id`) |
| `-32603` | the goal was **not accepted**; the message carries the reason |
| `-32001` | no task with that id (also used for a stream whose row never appeared) |

A request that cannot be parsed at all is refused differently by method:
`message/send` answers `-32602`; `message/stream` (on either address) answers
HTTP `400` with `{"error": "invalid_request", "message": "..."}`, because a
stream that never starts has no envelope to put the error in.

`-32603` is the important one for retry logic: the platform refuses up front
rather than handing back an id it cannot serve. Retry with backoff and surface
the reason — a refusal such as `upstream_unavailable` is transient; a malformed
message is not.

## 7. What is not supported

- Push notifications (`capabilities.push_notifications: false`).
- `tasks/resubscribe` and other methods outside the four above — they answer
  `-32601` and name what is served.
- Multiple text parts are joined with newlines into a single goal; there is no
  per-part task.

## 8. A minimal round trip

```bash
HOST=https://cogneva.example
TOKEN=$(curl -s -X POST "$HOST/api/v1/auth/login" \
  -H 'Content-Type: application/json' \
  -d '{"username":"peer","password":"..."}' | jq -r .access_token)

ID=$(curl -s -X POST "$HOST/api/v1/protocol/a2a" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"message/send",
       "params":{"message":{"role":"user",
                "parts":[{"kind":"text","text":"summarize the open incidents"}]}}}' \
  | jq -r '.result.id')

curl -s -X POST "$HOST/api/v1/protocol/a2a" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tasks/get\",\"params\":{\"id\":\"$ID\"}}"
```
