# A2A interoperability guide

This page is for someone building an **external agent** that talks to a Cogneva
deployment over [A2A](https://a2a-protocol.org/). It describes what this
platform actually serves today — not what the specification permits.

Everything here was read off a running gateway, not off a design document. If
the two ever disagree, the gateway wins and this page is the bug.

## 0. Versions

The platform serves **A2A 0.3 and A2A 1.0** on the same endpoints.

Declare the version you speak either way:

```
A2A-Version: 1.0
```

```
POST /api/v1/protocol/a2a?a2a-version=1.0
```

The header wins over the query parameter. **An absent or empty version means
0.3** — that is the specification's own default, not a Cogneva choice, so a
peer that sends no version is a 0.3 peer by definition.

A version this platform does not serve (for example `1.2`) is refused with
`-32009`, naming the versions that are served. It is never silently rounded
down to a different version: an answer in the wrong shape reads to the caller
as "this peer behaves oddly" instead of "this peer does not speak my version".

The two versions differ in shape, not in capability. Pick one and stay in it —
each request declares its own version, and every response is rendered in the
version that request declared.

## 1. Discovery

```
GET /.well-known/agent-card.json    # the path the specification names
GET /api/v1/protocol/a2a/agent-card # the same card, at our own path
```

A peer finds an agent at the well-known path; nothing tells it our private one
first. Both paths serve the same card and honour the same `A2A-Version`
negotiation, with one difference in the default: the well-known path defaults to
**1.0**, because that URI is defined by the 1.0 specification and a 1.0 client
needs `supportedInterfaces` to choose a transport at all. The private path keeps
the 0.3 default that the specification assigns to a request that names no
version. Say `A2A-Version: 0.3` and either path returns the 0.3 card.

The agent card is **public**: it answers without a credential, because a peer
that has never talked to you cannot have one yet, and the card is what tells it
which credential to get. The card carries no business data. Every endpoint it
names does require authentication.

Without a version header (0.3):

```json
{
  "protocolVersion": "0.3",
  "name": "cogneva",
  "description": "Cogneva autonomous multi-agent collaboration platform",
  "url": "https://<host>/api/v1/protocol/a2a",
  "version": "0.5.10",
  "capabilities": {
    "streaming": true,
    "pushNotifications": false,
    "stateTransitionHistory": true
  },
  "defaultInputModes": ["text/plain"],
  "defaultOutputModes": ["text/plain"],
  "skills": [ ... ],
  "securitySchemes": {
    "jwt": { "type": "http", "scheme": "bearer", "bearerFormat": "JWT" }
  },
  "security": [ { "jwt": [] } ]
}
```

With `A2A-Version: 1.0` the card has no top-level `url`: endpoints move into
`supportedInterfaces`, and each entry carries its own version. Listing both is
how a 1.0 peer learns it may also use 0.3 against the same deployment.

```json
{
  "name": "cogneva",
  "description": "Cogneva autonomous multi-agent collaboration platform",
  "version": "0.5.10",
  "supportedInterfaces": [
    { "url": "https://<host>/api/v1/protocol/a2a",
      "protocolBinding": "JSONRPC", "protocolVersion": "1.0" },
    { "url": "https://<host>/api/v1/protocol/a2a",
      "protocolBinding": "JSONRPC", "protocolVersion": "0.3" }
  ],
  "capabilities": { "streaming": true, "pushNotifications": false,
                    "stateTransitionHistory": true },
  "defaultInputModes": ["text/plain"],
  "defaultOutputModes": ["text/plain"],
  "skills": [ ... ],
  "securitySchemes": {
    "jwt": { "httpAuthSecurityScheme": { "scheme": "Bearer", "bearerFormat": "JWT" } }
  }
}
```

Two things to read carefully:

- **The endpoint URL** is the address the *caller* reached the platform at,
  resolved from the request's `Host` and `X-Forwarded-Proto`. That is the
  endpoint to call — not a bind address and not a fixed string.
- The card is sent with `Cache-Control: public, max-age=300`. Re-fetch it at
  most every five minutes; an address or capability change shows up within that
  window.

`capabilities.pushNotifications` is `false` on purpose: this platform has no
endpoint that accepts a webhook the server would call back. Do not configure
one; nothing will arrive.

## 2. Authentication

The card declares a bearer JWT. The business endpoints (`message/send`,
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

A2A dispatches on the JSON-RPC `method`, so the card's endpoint serves all four
methods. Both spellings are accepted on the wire; a 1.0 peer should use the
1.0 column and a 0.3 peer the 0.3 column.

| operation | 0.3 method | 1.0 method | params | result |
|---|---|---|---|---|
| send | `message/send` | `SendMessage` | `{ "message": { ... } }` | the task row as accepted (1.0 wraps it: `{ "task": ... }`) |
| stream | `message/stream` | `SendStreamingMessage` | same | `text/event-stream` of frames |
| read | `tasks/get` | `GetTask` | `{ "id": "<task id>" }` | the task row as it stands now |
| cancel | `tasks/cancel` | `CancelTask` | `{ "id": "<task id>" }` | the task row after the stop |

`message/stream` is additionally served on its own path,
`/api/v1/protocol/a2a/message/stream`, because the card's `streaming` flag names
it. Both addresses behave the same.

Message parts are read in all three spellings: 0.3 `{"kind":"text","text":"..."}`,
early-draft `{"type":"text",...}`, and 1.0's member-name form `{"text":"..."}`.
Text parts are joined with newlines into one goal. A message with no non-empty
text part is refused — it would otherwise create a goal with nothing in it.

### 1.0 `SendMessage` is blocking by default

The 1.0 specification makes `SendMessage` wait until the task reaches a terminal
or interrupted state unless the caller sets:

```json
{ "params": { "message": { ... }, "configuration": { "returnImmediately": true } } }
```

This platform honours that. With `returnImmediately: true` (or on any 0.3
`message/send`) the answer is the row **as accepted**, typically `submitted`:
poll `tasks/get` / `GetTask` for the outcome. Without it, the request is held
until the row is terminal, up to the stream's maximum lifetime (default 30
minutes); if that expires the answer is the row as it stands, still carrying
the id, so the caller can keep polling.

## 4. The task object

0.3 (a 1.0 response is the same object without `kind`, with `TASK_STATE_*`
status values, and — for `SendMessage` only — wrapped in `{"task": ...}`):

```json
{
  "kind": "task",
  "id": "0e6a...",
  "contextId": "0e6a...",
  "status": { "state": "submitted", "timestamp": "2026-10-23T00:05:00.000Z" },
  "artifacts": [],
  "history": []
}
```

One message creates one task, so `contextId` equals the task id: the task is
its own context.

`status.state` is one of `submitted`, `working`, `input-required`,
`auth-required`, `completed`, `canceled`, `failed`, `rejected` (1.0 spells them
`TASK_STATE_*`). This platform produces only the first two and `completed` /
`failed` / `canceled`; the others are read so that a peer's own vocabulary never
fails to parse. The platform's internal states map like this:

| platform state | A2A status |
|---|---|
| pending, scheduled | `submitted` |
| running | `working` |
| completed | `completed` |
| failed | `failed` |
| cancelled | `canceled` |

A non-terminal row may still carry a note in `status.message` — queueing
information such as "put back in line". It is not a failure; only a terminal
state makes it one.

Artifacts appear on terminal rows:

- `name: "result"` — one text part containing the run's output as JSON;
- `name: "error"` — one text part containing the failure reason.

Both may be present; read them by name, not by position.

## 5. Streaming

`message/stream` answers immediately with `200` and `content-type:
text/event-stream` (`cache-control: no-cache`, `x-accel-buffering: no`). Frames
use the event name `message`, and each `data:` line is a JSON-RPC message whose
`result` is one event.

0.3 events:

- **snapshot** — `kind: "task"`, the row as accepted;
- **status update** — `kind: "status-update"`,
  `{ "taskId": ..., "contextId": ..., "status": {...}, "final": bool }`, sent
  only when the state changes;
- **artifact update** — `kind: "artifact-update"`,
  `{ "taskId": ..., "contextId": ..., "artifact": {...} }`, sent for each
  artifact just before the terminal status frame.

1.0 events are the `StreamResponse` oneof — `{ "task": ... }`,
  `{ "statusUpdate": { "taskId": ..., "contextId": ..., "status": {...} } }`,
  `{ "artifactUpdate": { ... } }`. There is no `final` field in 1.0: the
  terminal state itself ends the stream.

The stream ends when the row reaches a terminal state, or after the
deployment's maximum stream lifetime (default 30 minutes). If the row never
becomes readable within that lifetime, the stream sends one JSON-RPC error
frame (`-32001`) **before** closing, so "nothing happened" is distinguishable
from "the run produced nothing".

If acceptance itself fails, the error frame is the first and only frame and the
HTTP status is still `200` — a rejected goal and an empty stream must not look
alike.

## 6. Errors

RPC-level failures come back as HTTP `200` with a JSON-RPC error envelope, so a
peer must read the body, not just the status:

| code | meaning |
|---|---|
| `-32009` | the declared `A2A-Version` is not served; the message names 0.3 and 1.0 |
| `-32004` | a method A2A defines but this platform does not serve (push config, resubscribe, list) |
| `-32601` | unknown method; the message names what is served |
| `-32602` | a required param is missing (e.g. `params.id`) |
| `-32603` | the goal was **not accepted**; the message carries the reason |
| `-32001` | no task with that id (also used for a stream whose row never appeared) |

A request that cannot be parsed at all is refused differently by method:
`message/send` answers `-32602`; `message/stream` (on either address) answers
HTTP `400` with `{"error": "invalid_request", "message": "..."}`, because a
stream that never starts has no envelope to put the error in. A card requested
with an unserved version answers HTTP `400` with
`{"error": "version_not_supported", ...}`.

`-32603` is the important one for retry logic: the platform refuses up front
rather than handing back an id it cannot serve. Retry with backoff and surface
the reason — a refusal such as `upstream_unavailable` is transient; a malformed
message is not.

## 7. What is not supported

- Push notifications (`capabilities.pushNotifications: false`), and the four
  `pushNotificationConfig` methods — they answer `-32004`.
- `tasks/resubscribe` / `SubscribeToTask` and `ListTasks` — `-32004`. Streams
  start from a message, not from an existing task; poll `tasks/get` instead.
- Extended agent cards (`GetExtendedAgentCard`) — `-32004`. There is one card.
- Versions older than 0.3 (`tasks/send` and friends) — `-32601`. Nothing in a
  current card can announce them, so they are not served.
- Multiple text parts become one goal joined with newlines; there is no
  per-part task.

## 8. A minimal round trip

```bash
HOST=https://cogneva.example
TOKEN=$(curl -s -X POST "$HOST/api/v1/auth/login" \
  -H 'Content-Type: application/json' \
  -d '{"username":"peer","password":"..."}' | jq -r .access_token)

ID=$(curl -s -X POST "$HOST/api/v1/protocol/a2a" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -H 'A2A-Version: 1.0' \
  -d '{"jsonrpc":"2.0","id":1,"method":"SendMessage",
       "params":{"message":{"role":"ROLE_USER",
                "parts":[{"text":"summarize the open incidents"}]},
                "configuration":{"returnImmediately":true}}}' \
  | jq -r '.result.task.id')

curl -s -X POST "$HOST/api/v1/protocol/a2a" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -H 'A2A-Version: 1.0' \
  -d "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"GetTask\",\"params\":{\"id\":\"$ID\"}}"
```

Drop the `A2A-Version` header and use `message/send` / `tasks/get` with
`{"kind":"text","text":...}` parts for the same round trip in 0.3.
