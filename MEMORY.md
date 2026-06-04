# MEMORY.md - Project Memory and Progress Log

This file tracks major architectural decisions, implementation logs, design details, and lessons learned for `s3xc`.

## 📌 Active Development Status
- **Current Phase**: Integration Testing & Functional Validation.
- **Goal**: Verify credential inheritance, copy/move/delete actions, custom caching status headers, and clean build profiles.

---

## 🏛️ Architectural Decisions & Rationale

### 1. Networking Framework: Axum + Hyper v1.0
- **Decision**: Use `axum` (which sits on `hyper` and `tower`) for the HTTP server framework, and `hyper-util`/`reqwest` for upstream client proxying.
- **Rationale**: `axum` is the industry standard for high-performance, asynchronous, type-safe web servers in Rust. It offers optimal ergonomics without sacrificing performance, integrates seamlessly with `tokio`, and allows using Tower middlewares for authentication, rate-limiting, and trace logging.

### 2. Disk I/O: Tokio Asynchronous FS & Direct Writes
- **Decision**: Use `tokio::fs` for non-blocking disk operations combined with logical chunking.
- **Rationale**: Since network proxying is fully async, disk writes should not block tokio's executor threads. logical chunking (splitting files into fixed-size chunks like 8MB) allows us to download and cache only the requested regions, which is extremely important for media streaming and seeking.

### 3. Metadata Indexing: `redb`
- **Decision**: Use `redb` (a pure Rust key-value store modeled after LMDB) for caching index metadata.
- **Rationale**: `redb` is lightweight, transactional, single-file, and has extremely low overhead compared to heavy databases (like SQLite or RocksDB) or unsafe/unmaintained ones (like sled). It provides ACID guarantees with great read performance, which is ideal for caching.

### 4. Zero-Copy and Low Allocation Strategy
- **Decision**: Avoid copying byte buffers. Use `bytes::Bytes` and stream combinators (e.g., from `tokio-util` and `futures-util`) to pipe data directly from network socket (client) to disk (cache writer) and upstream socket (client) without intermediate allocations.

### 5. Upstream Credential Inheritance
- **Decision**: Authenticate clients locally against a configured set of credentials (`S3C_ACCESS_KEY` / `S3C_SECRET_KEY`), extract the verified credentials, and use them to dynamically sign upstream requests to the backend. This ensures the backend validates permission compliance.

### 6. Custom Cache Header: `X-S3XC-CACHED`
- **Decision**: Expose cache status via `X-S3XC-CACHED: (hot|cold)` custom headers instead of standard Cache-Control headers. This gives clients clear feedback on whether the requested chunks were served from the local cache storage or fetched upstream.

### 7. Filesystem-Compliant Cache Actions
- **Decision**:
  - `DELETE` (rm): Drops the cached chunks and database metadata locally.
  - `CopyObject` (cp/mv):
    - `cp` copies the object upstream while leaving the cache cold.
    - `mv` (implemented by clients as Copy + Delete) copies the cached chunks and metadata from source to destination, then deletes the source cache, preserving cache status.
  - `Overwrite / Hotwrites`: Drop the existing cache and metadata on PUT overwrite first, preventing stale chunks and making the cache hot with the new chunks.

---

## 🚀 Execution Roadmap

- [x] **Phase 1: Project Initialization & Cargo Setup**
  - Set up `Cargo.toml` with dependencies for hyper, axum, tokio, redb, bytes, reqwest, signature verification, serialization, etc.
- [x] **Phase 2: Configuration & Logging**
  - Define configuration structures (upstream URL, cache directory, chunk size, eviction limits).
  - Add standard tracing and logs.
- [x] **Phase 3: S3 Proxy Client**
  - Implement a client that can fetch ranges from upstream S3, sign requests, and return streams.
- [x] **Phase 4: S3 Serving Layer**
  - Set up the axum server and handle routing for basic S3 actions (GetObject, HeadObject).
  - Implement AWS V4 signature verification middleware/handler wrapper.
- [x] **Phase 5: VFS Cache & Chunk Manager**
  - Create the chunk writer and stream demuxer (tee-like reader).
  - Implement the `redb` metadata database to track chunk status.
- [x] **Phase 6: Eviction System**
  - Implement LRU/LFU cache eviction based on size limits.
- [x] **Phase 7: Functional filesystem S3 alignment**
  - Support credential inheritance, custom cache headers, copy/move/delete cache operations, and overwrite hotwrites.
- [x] **Phase 8: Revert name to `s3xc` & Dynamic Signing/Serialization**
  - Kept/reverted the package/binary name as `s3xc` to avoid cache misses in CI.
  - Added support for multiple client (frontend) credentials parsed from `--credentials` or `S3C_CREDENTIALS` (format: `access:secret;access:secret`).
  - Added explicit backend authentication control via `--backend-v4-auth` (defaults to true; supports signing using SigV4 or disabling it for Anonymous/unsigned requests).
  - Added support for varying S3 request serialization schemes (Path-style vs Virtual-host-style URLs) based on the `--backend-path-scheme` configuration.
  - Ensured backend requests are signed exclusively with backend credentials (no pass-through of client signatures), dynamically constructing proper canonical URIs and Host headers based on the chosen path scheme.
- [ ] **Phase 9: Performance Benchmarking & Optimization**
  - Set up load tests using tools like `wrk` or `loadtest`.
  - Profile using `perf` / flamegraphs.
