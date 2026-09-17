# StreamShare

StreamShare lets you pipe the output of any command into a live, shareable
web link. Anyone with the link can watch the output stream in their browser
in real time, and late joiners see recent history first.

```
some-long-command | streamshare
```

## How it works

```
 ┌────────────┐   1. GET /create    ┌─────────────────┐
 │ CLI (you)  │ ───────────────────>│                  │
 │            │ <─────────────────  │                  │
 │            │      room ID        │                  │
 │            │                     │      Server      │
 │            │  2. WS /stream      │                  │
 │            │ ═══════════════════>│  ┌────────────┐  │
 └────────────┘   pushes stdin      │  │ Ring Buffer│  │
                                    │  │ (per room) │  │
 ┌────────────┐  3. WS /subscribe   │  └────────────┘  │
 │  Browser   │<═══════════════════>│                  │
 │  viewer(s) │  history + live     └─────────────────┘
 └────────────┘     broadcast
```

1. The CLI pipes stdin into the program. It calls `GET /create` on the
   server, which allocates a `Room` (a ring buffer plus a set of connected
   viewers) and returns a unique room ID.
2. The CLI opens a WebSocket to `/stream?room=<id>` and forwards every chunk
   it reads from stdin. The server writes each chunk into that room's ring
   buffer and immediately broadcasts it to any connected viewers.
3. A browser (or any WebSocket client) opens `/subscribe?room=<id>`. On
   connect it is registered as a viewer first, then sent a snapshot of the
   room's buffered history, so it never misses a line and never double-reads
   one. From then on it receives new lines as they arrive.

## Components

| File | Responsibility |
|---|---|
| `cli/main.go` | Reads piped stdin, requests a room, streams data over WebSocket. |
| `server/main.go` | HTTP/WebSocket server: room lifecycle, streaming, broadcast, viewer keepalive, cleanup. |
| `server/buffer.go` | `RingBuffer`: fixed-size, thread-safe history buffer per room. |

## Design decisions worth knowing

- **Viewer registration happens before the history snapshot.** If it
  happened after, a message arriving in between could be skipped entirely:
  not part of the snapshot, and broadcast before the viewer was registered
  to receive it.
- **Each server-side connection has its own write mutex.** A room writes to
  a viewer from two places — the broadcast loop and the keepalive ping loop
  — and the underlying WebSocket library requires callers to serialize
  concurrent writes themselves.
- **Rooms are reclaimed automatically.** A background janitor deletes a
  room shortly after its streamer disconnects, or after a longer idle
  window if it was created but never streamed to. Without this, every
  invocation of the CLI would leak memory on the server indefinitely.
- **Dead viewer connections are pruned on write failure**, and a ping/pong
  keepalive detects viewers whose connection died without a clean close
  (e.g. a closed laptop lid), so they don't accumulate silently.
- **Room links are unguessable but not access-controlled.** Anyone with the
  link can view or, if they hit `/stream` directly, write to a room. This
  is an intentional trade-off for a share-by-link tool; add an auth token
  per room if that's not acceptable for your use case.

## Running locally

Requires Go 1.21+.

```
# terminal 1: start the server
cd server
go run .

# terminal 2: stream something
cd cli
echo "hello world" | STREAMSHARE_HOST=localhost:8080 go run .
```

Open the printed link in a browser to watch the stream.

### Configuration

| Variable | Where | Default | Purpose |
|---|---|---|---|
| `PORT` | server | `8080` | Port the HTTP/WebSocket server listens on. |
| `STREAMSHARE_HOST` | CLI | `localhost:8080` | Host:port of the server to connect to. |

## Known limitations

- No authentication on rooms beyond the unguessable ID.
- No persistence: history is lost when the server restarts or a room is
  reclaimed.
- No horizontal scaling story: rooms live in a single server process's
  memory, so this doesn't work behind multiple server instances without
  a shared state layer.
