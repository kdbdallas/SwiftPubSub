# SwiftPubSub

A small, Dockerized in-memory pub/sub service. It has a browser publisher at `/` and WebSocket/HTTP APIs for Swift or other clients.

Each subscription is keyed by the full pair **`(clientID, sessionID?)`**:

- `clientID=123, sessionID=777` receives only that exact session.
- `clientID=123, sessionID=888` is a different channel.
- `clientID=456, sessionID=555` is completely separate.
- A client-wide channel such as `clientID=123` (no session) is also separate from every one of client 123's sessions.

Messages are deliberately held in memory and delivered only to current subscribers. Restarting the container drops connections and history.

## Run it

```sh
docker compose up --build
```

Open `http://localhost:8080` in a browser (or `http://YOUR_HOST:8080` from outside the Docker host). Choose a Client ID and an optional Session ID, then send messages. Browsers and Swift clients on the same exact scope will receive them.

## APIs

All IDs must be 1–128 characters of letters, numbers, `_`, or `-`.

| Purpose | Endpoint |
| --- | --- |
| Health check | `GET /health` |
| Listen/send on client channel | `WS /ws/clients/{clientID}` |
| Listen/send on session channel | `WS /ws/clients/{clientID}/sessions/{sessionID}` |
| HTTP publish to client | `POST /api/clients/{clientID}/messages` |
| HTTP publish to session | `POST /api/clients/{clientID}/sessions/{sessionID}/messages` |

WebSocket publishers send:

```json
{"type":"publish","message":"Hello session 777"}
```

Subscribers receive a JSON event with `type`, `id`, `scope`, `message`, and `sentAt`. HTTP publishes accept `{"message":"..."}` and return that same event.

For production exposure, put the service behind an HTTPS reverse proxy so the browser and app use `wss://`; also add authentication/authorization that verifies a caller is allowed to use the requested client/session scope. The channel partitioning prevents accidental cross-talk, but it is not identity-based access control.

## SwiftUI client

Add the two files in [`swift-client`](swift-client) to an iOS 17+/macOS 14+ app. They use a `@MainActor @Observable` MVVM view model that owns a `URLSessionWebSocketTask`.

```swift
PubSubScreen(
    serverURL: URL(string: "ws://YOUR_HOST:8080")!,
    scope: PubSubScope(clientID: "123", sessionID: "777")
)
```

Use `sessionID: nil` to subscribe to the isolated client-wide channel instead. On physical iOS devices, use your machine's reachable LAN address rather than `localhost`; for a non-TLS development connection, configure an appropriate App Transport Security exception. Use TLS in production.
