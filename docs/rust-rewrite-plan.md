# Zot Rust Rewrite Plan: Pull-Speed Focus

## Executive Summary

Rewrite the Zot OCI registry in Rust to eliminate pull-path bottlenecks identified in the Go implementation. The primary goal is maximizing image pull throughput by exploiting zero-copy I/O, lock-free blob serving, and native HTTP/2 multiplexing. The Go implementation leaves significant performance on the table through per-repo RWMutex contention, userspace data copies (`io.CopyN` in 10MB chunks), per-blob `Stat()` calls, and no `sendfile` usage.

---

## 1. Bottleneck Analysis of Current Go Implementation

### 1.1 Per-repo RWMutex held during blob open (`imagestore.go:1520-1521`)

`GetBlob` acquires `is.lock.RLock()` before calling `storeDriver.Reader()`. While this is a read lock (allows concurrent reads), it still serializes against any write operation on the same repo. Under mixed read/write workloads (pull while push is in progress), this blocks pull requests.

### 1.2 Extra `Stat()` call per blob (`imagestore.go:1482-1509`)

`originalBlobInfo()` calls `storeDriver.Stat(blobPath)` on every `GetBlob`, `GetBlobPartial`, and `GetBlobContent` request. For local storage this is a syscall; for S3 this is a `HeadObject` round-trip (20-100ms). This happens before the blob is even opened.

### 1.3 Userspace copy loop (`routes.go:2068-2081`)

`WriteDataFromReader` copies data from the blob reader to the HTTP response in a `io.CopyN` loop with 10MB chunks. Data flows: disk -> kernel -> userspace buffer -> kernel -> socket. Go's `net/http` does not use `sendfile(2)`, so every byte of every layer transits userspace.

### 1.4 No HTTP/2 multiplexing

Zot uses `gorilla/mux` on Go's `net/http` which supports HTTP/2 but doesn't optimize for stream multiplexing. Each concurrent layer pull from the same client opens a separate stream but shares the same connection-level flow control.

### 1.5 No blob metadata cache

Every pull request stats the filesystem, even for repeatedly-pulled popular images. There's a dedup cache for digest->path mapping but no in-memory size/existence cache.

---

## 2. Can Layers Be Pulled in Parallel?

**Yes, fully.** This is confirmed at every level:

### 2.1 OCI Distribution Spec

The spec explicitly allows it: blobs may be "retrieved in any order." Each `GET /v2/<name>/blobs/<digest>` is stateless and independent. No ordering constraints exist between layer fetches after the manifest is retrieved.

### 2.2 Client behavior

| Client | Default parallel layers | Config |
|---|---|---|
| containerd | 3 | `max_concurrent_downloads` in config.toml |
| Docker | 3 | `--max-concurrent-downloads` daemon flag |
| skopeo | 6 | `--src-concurrency` flag |
| crane | unlimited | default behavior |

### 2.3 What the server can do

The server's role is passive — it serves concurrent requests as fast as possible. The key insight is that **server-side parallelism is about removing serialization points**, not about the server initiating parallel transfers. Specifically:

1. **Eliminate per-repo locks on the read path** — allow unlimited concurrent blob reads
2. **Minimize syscalls per request** — cache blob metadata to avoid `stat()` on every GET
3. **Use zero-copy I/O** — `sendfile(2)` bypasses userspace entirely
4. **Support HTTP/2 multiplexing** — let clients open many streams on one connection
5. **Support range requests** — allow clients to split large layers into parallel chunk fetches

### 2.4 Range-based parallel chunk pulls

The OCI spec requires registries to support `Range:` headers on blob endpoints. A sufficiently advanced client could:
1. `HEAD /v2/<name>/blobs/<digest>` to get `Content-Length`
2. Split the layer into N range requests: `Range: bytes=0-50MB`, `Range: bytes=50MB-100MB`, etc.
3. Fetch chunks in parallel, reassemble, verify digest

Standard containerd/Docker don't do this today, but tools like `nydus` and `overlaybd` exploit range requests for lazy pulling. The Rust registry should be optimized for this pattern.

---

## 3. Architecture

### 3.1 Technology Stack

| Component | Crate | Why |
|---|---|---|
| Async runtime | `tokio` | Industry standard, required by all async crates below |
| HTTP server | `axum` + `hyper` | Native HTTP/2, tower middleware, streaming responses |
| TLS | `rustls` | Pure Rust, ALPN for HTTP/2, no OpenSSL dependency |
| Storage abstraction | Custom trait | `LocalStorage` and `S3Storage` implementations |
| S3 client | `aws-sdk-s3` | Official AWS SDK for Rust, streaming GetObject |
| JSON/OCI types | `oci-spec-rs` + `serde` | OCI image/distribution spec types |
| CAS index | `DashMap` | Lock-free concurrent hashmap for blob metadata cache |
| Logging | `tracing` | Structured, async-aware logging |
| Config | `serde` + `toml` | Compatible with existing Zot config format |
| Metrics | `metrics` + `metrics-exporter-prometheus` | Prometheus-compatible |

### 3.2 Project Layout

```
zot-rs/
  Cargo.toml
  src/
    main.rs                  # startup, config loading, server binding
    config.rs                # config parsing (Zot-compatible TOML/JSON)
    server.rs                # axum router setup
    routes/
      mod.rs
      v2.rs                  # GET /v2/ version check
      manifests.rs           # GET/HEAD/PUT/DELETE manifests
      blobs.rs               # GET/HEAD/DELETE blobs (the hot path)
      uploads.rs             # POST/PATCH/PUT blob uploads
      referrers.rs           # GET referrers
      catalog.rs             # GET _catalog
      tags.rs                # GET tags/list
    storage/
      mod.rs                 # StorageDriver trait
      local.rs               # Local filesystem (sendfile-optimized)
      s3.rs                  # S3 streaming backend
      cas.rs                 # Content-addressable storage layout + path helpers
      cache.rs               # In-memory blob metadata cache (DashMap)
    auth/
      mod.rs                 # Auth middleware
      basic.rs               # Basic auth
      bearer.rs              # Bearer token / OAuth2
    middleware/
      mod.rs
      metrics.rs             # Request timing + Prometheus metrics
      cors.rs                # CORS headers
      logging.rs             # Request/response tracing
    error.rs                 # OCI distribution error types
    digest.rs                # Digest validation + parsing
```

### 3.3 Core Design Principles

1. **No locks on the read path.** Blob serving must be completely lock-free. Use atomic reference counting for metadata cache entries.
2. **Zero-copy by default.** Local blob serving uses `sendfile(2)` (Linux) or `sendfile(2)` (macOS) via `tokio::fs::File` + custom `Body` impl.
3. **Cache blob metadata aggressively.** A `DashMap<Digest, BlobMeta>` caches `{path, size, exists}` to eliminate `stat()` per request.
4. **Stream everything.** Never buffer an entire blob into memory. Manifests (small) can be buffered; layers (large) must stream.
5. **HTTP/2 first.** Default to HTTP/2 with `h2` stream multiplexing so a single client connection can pull all layers concurrently.

---

## 4. Pull Path Implementation Detail

### 4.1 `GET /v2/<name>/blobs/<digest>` — The Hot Path

This is where pull speed is won or lost. The request flow:

```
Request
  |
  v
[axum handler: blobs::get_blob]
  |
  +--> validate digest format (pure computation, no I/O)
  |
  +--> check blob metadata cache (DashMap lookup, lock-free)
  |     |
  |     +-- HIT: have {path, size} -> skip to open
  |     +-- MISS: stat() the file, populate cache, continue
  |
  +--> check Range header
  |     |
  |     +-- No range: serve full blob
  |     +-- Range present: parse range, serve partial
  |
  +--> open file handle (tokio::fs::File::open)
  |
  +--> build response
        |
        +-- Local storage: SendFileBody (zero-copy)
        +-- S3 storage: StreamBody (async streaming from S3 GetObject)
```

### 4.2 Zero-Copy Blob Serving (Local Storage)

```rust
// Conceptual implementation
async fn get_blob(
    State(store): State<Arc<BlobStore>>,
    Path((name, digest)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let digest = Digest::parse(&digest)?;

    // Lock-free metadata lookup
    let meta = store.blob_meta(&name, &digest).await?;

    // Parse range if present
    if let Some(range) = headers.get(RANGE) {
        let (from, to) = parse_range(range, meta.size)?;
        let file = tokio::fs::File::open(&meta.path).await?;
        // Seek to offset, serve partial with sendfile
        return Response::builder()
            .status(StatusCode::PARTIAL_CONTENT)
            .header(CONTENT_LENGTH, to - from + 1)
            .header(CONTENT_RANGE, format!("bytes {}-{}/{}", from, to, meta.size))
            .body(SendFileBody::new(file, from, to - from + 1))
    }

    let file = tokio::fs::File::open(&meta.path).await?;
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_LENGTH, meta.size)
        .header("Docker-Content-Digest", digest.to_string())
        .body(SendFileBody::new(file, 0, meta.size))
}
```

The `SendFileBody` type implements `hyper::body::Body` using `sendfile(2)` under the hood, avoiding any userspace buffer. On Linux, this uses `splice()` + `sendfile()`; on macOS, `sendfile(2)` with the BSD calling convention.

### 4.3 Blob Metadata Cache

```rust
struct BlobMeta {
    path: PathBuf,
    size: u64,
    cached_at: Instant,
}

struct BlobCache {
    entries: DashMap<(String, Digest), BlobMeta>,  // (repo, digest) -> meta
    ttl: Duration,  // e.g., 60 seconds
}

impl BlobCache {
    fn get(&self, repo: &str, digest: &Digest) -> Option<BlobMeta> {
        self.entries.get(&(repo.to_string(), digest.clone()))
            .filter(|e| e.cached_at.elapsed() < self.ttl)
            .map(|e| e.clone())
    }

    fn insert(&self, repo: &str, digest: &Digest, meta: BlobMeta) {
        self.entries.insert((repo.to_string(), digest.clone()), meta);
    }
}
```

This eliminates the `stat()` syscall on repeated pulls of the same image. Cache invalidation happens on TTL expiry and on blob deletion (delete handler evicts the cache entry).

### 4.4 S3 Backend Optimization

For S3, zero-copy isn't possible (data must transit the network), but we can optimize:

1. **Presigned URL redirect**: Return HTTP 307 redirect to a presigned S3 URL, letting the client download directly from S3. This eliminates the registry as a proxy entirely.
2. **Streaming passthrough**: If redirect isn't acceptable (e.g., auth requirements), stream the S3 GetObject body directly to the HTTP response with no intermediate buffering.
3. **Connection pooling**: Reuse `hyper` HTTP client connections to S3 across requests.

---

## 5. Phased Implementation Plan

### Phase 1: Minimal Pull-Only Registry (Weeks 1-3)

Goal: A Rust binary that can serve image pulls from local filesystem storage, compatible with `docker pull` and `containerd`.

**Endpoints:**
- `GET /v2/` — version check
- `HEAD /v2/<name>/manifests/<ref>` — manifest existence
- `GET /v2/<name>/manifests/<ref>` — fetch manifest
- `HEAD /v2/<name>/blobs/<digest>` — blob existence + size
- `GET /v2/<name>/blobs/<digest>` — blob download (with Range support)
- `GET /v2/<name>/tags/list` — tag listing

**Storage:** Read-only local filesystem, Zot-compatible CAS layout (`<root>/<repo>/blobs/sha256/<digest>`).

**Key deliverable:** Benchmark showing pull throughput vs Go Zot on same hardware/images.

### Phase 2: Zero-Copy + HTTP/2 (Weeks 4-5)

- Implement `SendFileBody` for zero-copy blob serving
- Enable HTTP/2 via `rustls` + ALPN
- Implement blob metadata cache (`DashMap`)
- Benchmark: measure throughput improvement over Phase 1

### Phase 3: Push Support (Weeks 6-8)

- `POST /v2/<name>/blobs/uploads/` — initiate upload
- `PATCH /v2/<name>/blobs/uploads/<session>` — chunked upload
- `PUT /v2/<name>/blobs/uploads/<session>` — complete upload
- `PUT /v2/<name>/manifests/<ref>` — push manifest
- `DELETE` endpoints for blobs and manifests
- Session tracking for resumable uploads

### Phase 4: S3 Backend (Weeks 9-10)

- S3 storage driver with streaming GetObject
- Optional presigned URL redirect mode
- Blob metadata cache with S3 `HeadObject` fallback

### Phase 5: Auth + Extensions (Weeks 11-13)

- Basic auth
- Bearer token auth (compatible with Zot's auth config)
- TLS client cert auth
- Sync extension (pull-through cache from upstream registries)
- Garbage collection
- Prometheus metrics

### Phase 6: Feature Parity + Migration (Weeks 14-16)

- Referrers API
- Catalog API
- Image trust / cosign verification
- Search / metadata extensions
- Config compatibility with existing Zot deployments
- Migration tooling (in-place storage reuse — same CAS layout)

---

## 6. Expected Performance Gains

| Bottleneck | Go Zot | Rust Zot | Expected Improvement |
|---|---|---|---|
| Blob serving (data path) | `io.CopyN` 10MB chunks through userspace | `sendfile(2)` zero-copy | 30-50% throughput increase for large layers |
| Metadata lookup | `Stat()` syscall per request | `DashMap` cache, stat on miss | Eliminate ~1ms/req local, ~50ms/req S3 |
| Concurrency under load | RWMutex per repo, goroutine overhead | Lock-free reads, async tasks ~4KB each | Higher concurrent client capacity |
| HTTP/2 multiplexing | Supported but not optimized | Native `h2` stream priority | Reduced connection overhead for parallel pulls |
| Memory per connection | ~1MB per goroutine + 10MB copy buffer | ~4KB per async task, no buffer | 250x lower memory per connection |
| Range requests | Supported, same copy overhead | Zero-copy partial file serving | Enables client-side parallel chunk pulls |

### Realistic throughput targets (NVMe SSD, 10GbE)

- **Single client, single layer:** 1.0 → 1.3 GB/s (limited by NIC)
- **Single client, 3 parallel layers:** 1.0 → 1.3 GB/s (NIC-saturated faster)
- **100 concurrent clients:** 2-4x throughput improvement (reduced per-connection overhead)
- **S3 backend with presigned redirects:** Eliminates registry as bottleneck entirely

---

## 7. Compatibility and Migration

### 7.1 Storage layout compatibility

The Rust implementation will use the same CAS directory layout as Go Zot:
```
<root>/
  <repo>/
    blobs/
      sha256/
        <digest>
    index.json
```

This allows zero-downtime migration: stop Go Zot, start Rust Zot pointing at the same storage root. No data migration needed.

### 7.2 Config compatibility

Parse the existing Zot JSON config format. New Rust-specific options (cache TTL, sendfile toggle, H2 settings) are additive fields.

### 7.3 API compatibility

100% OCI Distribution Spec v1.1 compliance. The Rust implementation serves the same REST API — clients (containerd, Docker, skopeo, crane) require zero changes.

---

## 8. Risks and Mitigations

| Risk | Mitigation |
|---|---|
| `sendfile` not available (container environments) | Fall back to `tokio::io::copy` with 256KB buffer (still better than Go's 10MB chunks) |
| `DashMap` cache memory growth | TTL eviction + max entry count, LRU eviction on pressure |
| S3 SDK maturity | `aws-sdk-s3` is production-grade (GA since 2023) |
| OCI spec edge cases | Use `oci-spec-rs` for type validation, run OCI conformance test suite |
| macOS `sendfile` differences | Conditional compilation with `#[cfg(target_os)]`, tested in CI |
| Extension feature gap | Phase 5-6 addresses extensions; pull-speed benefit is available from Phase 2 |
