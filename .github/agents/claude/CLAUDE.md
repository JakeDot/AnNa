# Claude Code conventions for AnNa

This file is read by Claude Code when working on this repository.
Fork maintainers: fill in the `Co-authored-by` trailer with your own
identity and drop this note.

---

## Commit style

### Focused commits
One logical change per commit. Server changes, client changes, and CI
changes are separate commits unless they are genuinely inseparable (e.g.
a new API endpoint and the client call that uses it may ship together, but
a refactor and a feature must not).

### Verbose messages
The commit body should explain **why** the change was made, not just what
it does. A future reader — including an AI assistant in a later session —
should be able to understand the motivation without opening the diff.

Structure:
```
<imperative subject line, ≤72 chars, no trailing period>

<one or more paragraphs or bullet points explaining motivation, decisions,
and tradeoffs. include anything non-obvious.>

Co-authored-by: Your Name <you@example.com>
```

### Example
```
Gate server uploads behind ?backup=true query parameter

Previously the server accepted any multipart POST to /api/upload and
stored the file permanently. This made the server a default sink, which
conflicts with the P2P-first design where files live on-device and the
server is only an explicit fallback.

Adding the ?backup=true gate means:
- Accidental or unauthenticated uploads are rejected with a clear error.
- The server's storage only grows when the user deliberately backs up.
- Integration tests can assert the gate is enforced (see tests/integration.rs).

Co-authored-by: Your Name <you@example.com>
```

---

## Project structure

```
AnNa/
├── server/          Rust — Axum HTTP server, SQLite, CDC chunking, QUIC
│   ├── src/lib.rs   All server logic (handlers, router, AppState)
│   └── src/main.rs  Entry point — calls anna_sync_server::run()
├── client/          React + TypeScript — Tauri 2.0 native app
│   ├── src/         React UI (components, hooks, API client)
│   └── src-tauri/   Tauri Rust backend (local storage, P2P HTTP server)
└── .github/
    └── workflows/ci.yml  Rust tests · TS build · Tauri desktop matrix
```

## Key invariants to preserve

- **Server never auto-stores uploads.** `POST /api/upload` requires
  `?backup=true`. Removing this gate breaks the P2P-first contract.
- **CDC parameters must match between server and Tauri client.**
  Both use FastCDC with min=256 KB / avg=1 MB / max=4 MB so chunk
  hashes are comparable across peers.
- **BLAKE3 everywhere.** File hashes and chunk hashes both use BLAKE3.
  Do not introduce SHA-256 for new code paths.
- **Integration tests are the source of truth for API contracts.**
  Any change to a handler's behaviour must be reflected in
  `server/tests/integration.rs` in the same commit.

## Running locally

```bash
# Server
cd server && cargo run

# Client (web, dev mode)
cd client && npm run dev

# Client (native desktop)
cd client && npm run tauri:dev

# Server tests (including integration)
cd server && cargo test
```
