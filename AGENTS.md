# AGENTS.md - Workspace Instructions for s3xc

Welcome to the `s3xc` project workspace. This file establishes the context, architecture guidelines, and operational procedures for all AI agents working on this codebase.

## Project Vision
`s3xc` is a high-performance S3 caching proxy and server written in Rust, designed to replace `rclone`'s VFS caching and serving features with superior performance, efficiency, and modern design. It acts as an intermediary between S3 clients and upstream S3 backends, providing:
1. **S3 Serving**: Exposing an S3-compatible HTTP API to clients.
2. **S3 Reverse Proxy**: Intercepting client requests and forwarding them to upstream S3 backends.
3. **VFS / Local Cache**: Caching files locally on disk to speed up read access, supporting chunk-based sparse caching (range requests), stream demuxing (streaming to client and cache concurrently), and policy-based eviction.

## Technical Stack & Performance Targets
- **Language**: Rust (Edition 2021)
- **Async Runtime**: `tokio` (multi-threaded, optimized configuration)
- **Network I/O**: `axum` and `hyper` (v1.0+) for the server; `reqwest` or `hyper-util` for the client.
- **Disk I/O**: Asynchronous file operations via `tokio::fs`, using buffered writes and `bytes::Bytes` for zero-copy memory transfers where possible.
- **Metadata Storage**: `redb` (a fast, transactional, pure-Rust embedded key-value store) or SQLite (in WAL mode) to store cache indexes, tracking chunk status and LRU/LFU weights.
- **In-Memory Caching**: `dashmap` and `moka` for low-overhead, highly concurrent caching of active metadata.

## Directory Structure
The codebase uses a clean, modular structure:
- `src/main.rs`: Application entry point, CLI parser, and initialization.
- `src/config.rs`: Configuration parsing (TOML/Env vars).
- `src/server/`: HTTP and S3 API server implementation.
  - `mod.rs`: Server entry point and routing.
  - `handlers.rs`: S3 API handlers (GetObject, PutObject, etc.).
  - `s3_api.rs`: S3 protocol parsing and signature validation.
- `src/proxy/`: Upstream S3 client and reverse proxying logic.
  - `mod.rs`: Upstream client connection pool and request forwarding.
- `src/cache/`: Virtual File System (VFS) and cache management.
  - `mod.rs`: Cache coordinator.
  - `storage.rs`: Local disk storage (chunk files, writes, reads).
  - `metadata.rs`: Metadata database (`redb` index for tracking cached chunks).
  - `policy.rs`: Eviction worker and policies (LRU/LFU/TinyLFU).
- `src/utils.rs`: Shared utility functions (hashing, streams, errors).

## Guidelines for Coding & System Performance
1. **Zero-Copy Byte Handling**: Use `bytes::Bytes` for payload passing. Avoid cloning large buffers.
2. **Stream Demuxing (Teecing)**: When proxying a stream from upstream on a cache miss, pipe the stream directly to the client while simultaneously writing chunks to disk. Do not read the entire stream into memory first.
3. **Chunk-Based Sparse Storage**: Do not download entire files on partial range requests. Divide files into logical chunks (e.g., 4MB-16MB) and cache individual chunks as sparse files or named chunk files on disk.
4. **Lock Contention Minimization**: Avoid global locks (`Mutex`/`RwLock` on large structures). Use message passing (`tokio::sync::mpsc`), concurrent collections (`dashmap`), or actor patterns for coordinating disk writes and eviction tasks.
5. **Robust Error Handling**: Implement structured error types using `thiserror` and detailed context. S3 APIs must return standardized XML error responses to clients.

## Ongoing Context (MEMORY.md)
Keep tracking active task status, architectural decisions, and current implementation phases in [MEMORY.md](file:///home/stdpi/repo/s3c/MEMORY.md). Do not make "mental notes".
