# ÃnN@sync

A high-performance peer-to-peer file synchronization platform built with Rust and React.

## Features

- 🚀 **High Performance**: Rust server for blazing-fast file operations
- 🔄 **P2P Transfer**: BitTorrent-inspired chunk-based file distribution
- 🗜️ **Smart Compression**: Automatic Brotli compression for compressible files
- 🔍 **Deduplication**: Content-addressable storage with SHA-256 hashing
- 📦 **Chunked Transfer**: 256KB chunks for efficient parallel downloads
- 🌐 **Modern Web Client**: React + TypeScript PWA with drag-and-drop upload
- 🔌 **WebSocket Signaling**: Real-time peer discovery and coordination
- 💾 **SQLite Database**: Fast and reliable metadata storage

## Architecture

### Server (Rust)
- **Axum** web framework with HTTP/2 support (HTTP/3 ready)
- **Tokio** async runtime for high concurrency
- **SQLite** for metadata storage
- **WebSocket** for P2P signaling
- **Brotli** compression for efficient storage

### Client (React + TypeScript)
- **Vite** for fast development and building
- **React** with hooks for state management
- **Socket.io** for WebSocket connections
- **Progressive Web App** (installable)

## Quick Start

### Prerequisites
- Rust 1.70+
- Node.js 18+
- Docker (optional)

### Running with Docker

```bash
docker-compose up --build
```

The application will be available at `http://localhost:3000` (dev) or [nn.ã.at]

### Running Locally

#### Server
```bash
cd server
cargo build --release
cargo run --release
```

#### Client (Development)
```bash
cd client
npm install
npm run dev
```

#### Client (Production Build)
```bash
cd client
npm run build
# Serve the dist folder with the Rust server
```

## API Endpoints

### File Operations
- `POST /api/upload` - Upload a file (multipart/form-data)
- `GET /api/files` - List all files
- `GET /api/files/check/:hash` - Check if file exists
- `GET /api/download/:hash` - Download a file
- `GET /api/chunk/:hash/:chunk_id` - Get a specific chunk

### P2P Operations
- `GET /api/peers` - List connected peers
- `WS /ws` - WebSocket endpoint for signaling

## Configuration

### Environment Variables

Server:
- `RUST_LOG` - Log level (default: `info`)
- `PORT` - Server port (default: `3000`)

Client:
- `VITE_API_URL` - API base URL (default: `http://localhost:3000`)
- `VITE_WS_URL` - WebSocket URL (default: `ws://localhost:3000`)

## Deployment

### Production Deployment

```bash
# Build and push Docker image
docker build -t annasync:latest .
docker tag annasync:latest registry.example.com/annasync:latest
docker push registry.example.com/annasync:latest

# Deploy to server
docker pull registry.example.com/annasync:latest
docker-compose up -d
```

## Development Roadmap

- [x] Rust server with file upload/download
- [x] SQLite metadata storage
- [x] Content-addressable deduplication
- [x] Brotli compression
- [x] Chunk-based transfers
- [x] WebSocket signaling
- [x] React web client
- [x] PWA support
- [ ] WebRTC P2P data channels
- [ ] Windows desktop client (Electron/Tauri)
- [ ] Android app (Kotlin/React Native)
- [ ] iOS app (Swift/React Native)
- [ ] End-to-end encryption
- [ ] HTTP/3 (QUIC) support
- [ ] TURN server for NAT traversal

## Performance

- **Upload Speed**: Limited by network bandwidth
- **Deduplication**: O(1) hash lookup
- **Compression**: ~2-5x reduction for text files
- **Chunk Size**: 256KB (optimized for network efficiency)
- **Concurrent Uploads**: Unlimited (async Rust)

## Security

- Content-addressable storage prevents tampering
- SHA-256 hashing for file integrity
- CORS protection enabled
- Rate limiting recommended for production
- Future: End-to-end encryption option

## Contributing

Contributions welcome! Please read CONTRIBUTING.md for guidelines.

## License

MIT License - see LICENSE file for details

## Acknowledgments

- Built with [Rust](https://www.rust-lang.org/), [Axum](https://github.com/tokio-rs/axum), [React](https://react.dev/), and [Vite](https://vite.dev/)
- Inspired by BitTorrent, IPFS, and Syncthing
- Icon from Flaticon

## Contact

For questions or support, please open an issue on GitHub.
