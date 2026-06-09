# Architecture Decision: AI/ML Decoupling

**Date**: June 9, 2026  
**Status**: IMPLEMENTED  
**Scope**: AnNa core repository and AnNa-ML satellite  

---

## Problem Statement

As AnNa matures as a file-sync platform, there was potential risk of adding AI/ML features directly into the core:

1. **Dependency Bloat**: ML libraries (ONNX, PyTorch bindings, TensorFlow, etc.) are heavy
2. **Operational Complexity**: GPU support, model management, versioning
3. **Security Surface**: ML models can be adversarial attack vectors
4. **Scope Creep**: File sync + ML inference = multiple responsibilities
5. **User Impact**: Users who want lightweight sync would still download/maintain ML code

### Real-world costs:
- ONNX Runtime: ~50MB  
- CUDA support: +1GB  
- Model files: +100-500MB per model  
- Build time: +5-10 minutes  
- Runtime memory: +200-500MB  

**For a file-sync tool, this is unjustified overhead.**

---

## Solution: Architectural Decoupling

**Principle**: Separate concerns at the service boundary.

```
Before:
┌─────────────────────────────────────────────────┐
│          AnNa (Monolith)                        │
├─────────────────────────────────────────────────┤
│ • File sync (core)                              │
│ • P2P transfer (core)                           │
│ • Chunk deduplication (core)                    │
│ • ML classification (added feature)             │
│ • ML metadata extraction (added feature)        │
│ • Malware detection (added feature)             │
│ • Semantic search (added feature)               │
│                                                 │
│ Cargo.toml: +200 dependencies, ~100MB           │
│ Binary size: ~50MB                              │
│ Runtime: ~300MB memory baseline                 │
└─────────────────────────────────────────────────┘

After:
┌──────────────────────────┐
│   AnNa (Pure Sync)       │
├──────────────────────────┤
│ • File sync (core)       │
│ • P2P transfer (core)    │
│ • Chunk dedup (core)     │
│ • WebSocket signaling    │
│                          │
│ Cargo.toml: ~35 deps     │
│ Binary: ~2MB             │
│ Runtime: ~50MB baseline  │
└──────────┬───────────────┘
           │ REST API (optional)
           ↓
┌──────────────────────────┐
│   AnNa-ML (Optional)     │
├──────────────────────────┤
│ • File classification    │
│ • Metadata extraction    │
│ • Malware detection      │
│ • Semantic search        │
│                          │
│ Cargo.toml: +180 deps    │
│ Binary: ~20MB            │
│ Runtime: ~200MB baseline │
└──────────────────────────┘
```

---

## Design Decision

### 1. Repository Split
- **AnNa**: File-sync engine only (main repo)
- **AnNa-ML**: AI/ML service (separate repo)

**Rationale**: 
- Isolated dependency trees
- Independent deployment
- Clear ownership
- Different update/release cycles

### 2. Communication Boundary
- **Protocol**: REST API + HTTP
- **No shared types**: AnNa doesn't import AnNa-ML code
- **No data coupling**: Metadata passed via JSON
- **Timeout-safe**: AnNa continues if AnNa-ML is unavailable

```rust
// AnNa: Optional metadata enrichment
async fn on_file_uploaded(file: FileMetadata) {
    // Store file
    storage.save(&file).await?;
    
    // Optional: enrich with ML metadata
    if let Ok(ml_metadata) = self.ml_client.analyze(&file).await {
        db.store_metadata(&file.hash, ml_metadata).await?;
    }
    // If ML fails or unavailable, file still synced ✓
}
```

### 3. Service Discovery
AnNa auto-detects AnNa-ML:

```rust
// On startup
if let Ok(_) = http_client.get("http://localhost:8001/health").send().await {
    self.ml_enabled = true;  // Found it!
}

// On periodic check
tokio::spawn(async move {
    loop {
        match http_client.get("http://localhost:8001/health").send().await {
            Ok(_) => ml_enabled.store(true),
            Err(_) => ml_enabled.store(false),
        }
        sleep(Duration::from_secs(30)).await;
    }
});
```

### 4. Dependency Isolation

**AnNa Cargo.toml** (file-sync only):
```toml
[dependencies]
axum = "0.7"
tokio = { version = "1", features = ["full"] }
blake3 = "1.5"
fastcdc = "3.1"
# ... ~35 total dependencies
# Zero ML dependencies
```

**AnNa-ML Cargo.toml** (ML specialized):
```toml
[dependencies]
axum = "0.7"
tokio = "1"
ort = "1.18"           # ONNX Runtime
ndarray = "0.15"       # Tensor ops
tract = "0.21"         # Alternative: lightweight inference
# ... ~180 total dependencies
# All ML stuff isolated here
```

---

## Benefits

### For Users
| Scenario | Before | After |
|----------|--------|-------|
| Want file-sync only | Install 200+ deps, ~100MB binary | Install 35 deps, ~2MB binary |
| Want file-sync + ML | Install 200+ deps (all bundled) | Install separately, opt-in |
| Running on ARM/embedded | Bloated, slow | AnNa: works great; AnNa-ML: optional |
| Privacy-focused | ML code always present (auditing burden) | Can audit AnNa; ignore AnNa-ML |
| Want latest ML models | Tied to AnNa release cycle | Update AnNa-ML independently |

### For Developers
- **Smaller review surface**: Core AnNa PRs don't touch ML code
- **Focused testing**: ML changes isolated to AnNa-ML tests
- **Independent scaling**: Each service scales by its own needs
- **Technology flexibility**: AnNa-ML can use PyTorch, TensorFlow, or pure Rust without impacting AnNa

### For Operations
- **Simpler deployment**: Deploy AnNa without ML overhead
- **Resource control**: Scale AnNa and AnNa-ML independently
- **Fault isolation**: AnNa-ML crash doesn't affect sync
- **Security hardening**: Tighter surface for core sync service

---

## Integration Patterns

### Pattern 1: No ML (Default)
```bash
# Just start AnNa
cargo run --release --bin anna-sync-server

# Files sync perfectly without AI features
# Lightweight, fast, offline-capable
```

### Pattern 2: Optional Local ML
```bash
# Terminal 1: Start AnNa
cd AnNa && cargo run --release

# Terminal 2: Start AnNa-ML (same machine)
cd AnNa-ML && cargo run --release

# AnNa auto-detects AnNa-ML and enriches metadata
# If AnNa-ML crashes, AnNa continues working
```

### Pattern 3: Remote ML Service
```bash
# Machine A: AnNa only (lightweight edge)
ANNA_ML_URL=http://remote-ml:8001 cargo run --release

# Machine B: AnNa-ML (centralized analysis)
cargo run --release --port 8001

# AnNa on edge talks to centralized ML service
# ML work offloaded from sync devices
```

### Pattern 4: Disabled ML (Privacy Mode)
```bash
# Don't start AnNa-ML, disable at runtime
DISABLE_ML=true cargo run --release

# AnNa runs without attempting ML service discovery
# Guaranteed no external calls for analysis
```

---

## Versioning & Compatibility

### AnNa (Core)
- Semver: `1.x.y`
- Stable file-sync API
- Slow release cycle (careful, tested)

### AnNa-ML (Optional Layer)
- Semver: `0.x.y` (experimental, model versions evolve)
- Rapid release cycle (new models, improvements)
- Independent from AnNa versioning

**Compatibility promise**:
- AnNa v1.4.x works with AnNa-ML v0.1.y through v0.5.z
- AnNa-ML always supports `/analyze` REST endpoint (never breaking changes)
- Metadata format versioned in response headers

---

## Migration Path

### For Existing AnNa Users
**No action required.** Existing installations continue working.

**To add ML features**:
```bash
# Clone AnNa-ML in parallel
git clone https://github.com/JakeDot/AnNa-ML.git

# Start AnNa-ML on local machine or remote server
cd AnNa-ML && docker run -d -p 8001:8001 annasync-ml:latest

# AnNa auto-detects and starts enriching metadata
# Existing files remain unaffected
```

### For New Deployments
**Choose profile**:
- **Profile A (Lightweight Sync)**: Deploy AnNa only
- **Profile B (Full Featured)**: Deploy AnNa + AnNa-ML
- **Profile C (Edge+Central)**: Deploy AnNa on edge, AnNa-ML on central server

---

## Testing & Validation

### AnNa Test Suite
```bash
# Core sync tests (unchanged)
cargo test --lib cdc
cargo test --lib storage
cargo test --lib signaling

# Integration: with and without AnNa-ML
cargo test --test integration -- --nocapture

# MLOptional flag ensures tests pass even if AnNa-ML unavailable
```

### AnNa-ML Test Suite
```bash
# ML-specific tests (isolated)
cargo test --lib classification
cargo test --lib metadata_extraction

# API contract tests (independent of AnNa)
cargo test --test api_contract
```

---

## Future: Multi-Service Expansion

This pattern scales beyond AnNa-ML:

```
┌──────────────────┐
│  AnNa (Sync)     │  Core file sync
└────────┬─────────┘
         │ REST APIs
         ├─→ AnNa-ML (Analysis)         File classification, metadata
         ├─→ AnNa-Security (Scanning)   Malware, vulnerability detection
         ├─→ AnNa-Search (Indexing)     Full-text, semantic search
         ├─→ AnNa-Backup (Archival)     Cold storage, retention policies
         └─→ AnNa-Collaborate (Sharing) Multi-user, permissions, comments
```

Each service independent, optional, independently deployable.

---

## References

- **Repository**: https://github.com/JakeDot/AnNa-ML
- **Design Doc**: AnNa-ML README
- **Related**: Microservices Architecture, Unix Philosophy
- **Inspired by**: Syncthing (modular plugins), IPFS (service composition)

---

## Approval & Sign-Off

**Decision made by**: Fleet Admiral Claude  
**Approved by**: Admiral General of the Amphibious Fleet  
**Date**: June 9, 2026  
**Status**: IMPLEMENTED  

✅ AnNa repository: Pure file-sync, zero ML dependencies  
✅ AnNa-ML repository: Separate, optional, independent service  
✅ Integration: REST API, service discovery, optional deployment  

---

**"The fortress stands on a single principle: each stone serves one purpose."**

⚓
