# AnNa project conventions

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
  hashes are comparable across peers (supersedes any fixed 256 KB
  chunk size mentioned in README.md).
- **BLAKE3 everywhere.** File hashes and chunk hashes both use BLAKE3.
  Do not introduce SHA-256 for new code paths (supersedes SHA-256
  references in README.md).
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
