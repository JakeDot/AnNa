# Multi-stage build for Rust server
FROM rust:1.94-slim as rust-builder

WORKDIR /app/server

# Install dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy Cargo files
COPY server/Cargo.toml server/Cargo.lock* ./

# Copy source code
COPY server/src ./src

# Build release
RUN cargo build --release

# Build client
FROM node:20-slim as client-builder

WORKDIR /app/client

# Copy package files
COPY client/package*.json ./

# Install dependencies
RUN npm ci

# Copy source code
COPY client/ ./

# Build client
RUN npm run build

# Final runtime image
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy server binary
COPY --from=rust-builder /app/server/target/release/anna-sync-server /app/server

# Copy client build
COPY --from=client-builder /app/client/dist /app/public

# Copy PWA files
COPY public/manifest.json public/sw.js /app/public/

# Create data directory
RUN mkdir -p /app/data/uploads

# Expose port
EXPOSE 3000

# Set environment variables
ENV RUST_LOG=info
ENV NODE_ENV=production

# Run server
CMD ["/app/server"]
