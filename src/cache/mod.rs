use std::cmp;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use bytes::Bytes;
use dashmap::DashMap;
use futures_util::{Stream, StreamExt};
use tokio::sync::watch;
use tracing::{debug, error, info};

pub mod metadata;
pub mod policy;
pub mod storage;

use crate::proxy::ProxyClient;
use metadata::{ChunkMetadata, ChunkStatus, MetadataDb, ObjectMetadata};
use storage::Storage;

#[derive(Debug, Clone, PartialEq, Eq)]
enum DownloadStatus {
    Pending,
    Complete,
    Failed(String),
}

pub struct CacheCoordinator {
    pub db: Arc<MetadataDb>,
    storage: Arc<Storage>,
    pub proxy: ProxyClient,
    chunk_size: usize,
    active_downloads: DashMap<String, watch::Receiver<DownloadStatus>>,
}

impl CacheCoordinator {
    pub fn new(
        db: Arc<MetadataDb>,
        storage: Arc<Storage>,
        proxy: ProxyClient,
        chunk_size: usize,
    ) -> Self {
        Self {
            db,
            storage,
            proxy,
            chunk_size,
            active_downloads: DashMap::new(),
        }
    }

    fn now_secs() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64
    }

    /// Retrieve object metadata, caching it on miss.
    pub async fn get_object_metadata(
        &self,
        bucket: &str,
        key: &str,
        _creds: Option<&crate::utils::ClientCredentials>,
    ) -> anyhow::Result<ObjectMetadata> {
        let db_key = format!("{}/{}", bucket, key);

        // Fetch upstream metadata first (always) to inherit credentials and check if it's there
        let upstream = self.proxy.head_object(bucket, key).await?;
        
        let content_length = upstream.content_length;
        let content_type = upstream.content_type.clone();
        let etag = upstream.etag.clone();
        let last_modified = upstream.last_modified;

        // Check if we already have it in local DB, and if the etag/content_length changed.
        // If the etag/content_length changed, we should drop the local cache chunks!
        if let Some(local_meta) = self.db.get_object(&db_key)? {
            if local_meta.etag != etag || local_meta.content_length != content_length {
                // Remote object has been overwritten/modified. Invalidate/drop local cache chunks.
                self.drop_cache_for_object(bucket, key).await?;
            }
        }

        let meta = ObjectMetadata {
            content_length,
            content_type,
            etag,
            last_modified,
        };

        // Cache it
        self.db.put_object(&db_key, &meta)?;
        Ok(meta)
    }

    /// Reads a range of bytes from the cache, downloading chunks on miss.
    /// Note: `end` is exclusive byte index, compatible with standard HTTP ranges (e.g. `start..end`).
    pub async fn read_range(
        self: Arc<Self>,
        bucket: &str,
        key: &str,
        start: u64,
        end: u64,
        creds: Option<crate::utils::ClientCredentials>,
    ) -> anyhow::Result<std::pin::Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>> {
        let db_key = format!("{}/{}", bucket, key);
        let meta = self.get_object_metadata(bucket, key, creds.as_ref()).await?;
        
        let file_size = meta.content_length;
        let end = cmp::min(end, file_size);
        
        if start >= end {
            return Ok(futures_util::stream::once(async { Ok(Bytes::new()) }).boxed());
        }

        let chunk_size = self.chunk_size as u64;
        let start_chunk = start / chunk_size;
        let end_chunk = (end - 1) / chunk_size;

        // Generate streams for each chunk in range
        let mut streams = Vec::new();
        for idx in start_chunk..=end_chunk {
            let this = Arc::clone(&self);
            let db_key = db_key.clone();
            let bucket = bucket.to_string();
            let key = key.to_string();

            // Calculate range within this chunk
            let chunk_start_byte = idx * chunk_size;
            let chunk_end_byte = cmp::min((idx + 1) * chunk_size, file_size);

            let read_start = cmp::max(start, chunk_start_byte);
            let read_end = cmp::min(end, chunk_end_byte);

            let offset_in_chunk = read_start - chunk_start_byte;
            let length_to_read = (read_end - read_start) as usize;

            let fut = async move {
                // Check if chunk is in DB
                let is_cached = if let Ok(Some(chunk_meta)) = this.db.get_chunk(&db_key, idx) {
                    chunk_meta.status == ChunkStatus::Complete
                } else {
                    false
                };

                if is_cached {
                    // Update access time for LRU
                    if let Ok(Some(mut chunk_meta)) = this.db.get_chunk(&db_key, idx) {
                        chunk_meta.last_accessed_at = Self::now_secs();
                        let _ = this.db.put_chunk(&db_key, idx, &chunk_meta);
                    }
                    // Read from disk
                    let bytes = this.storage.read_chunk_range(&db_key, idx, offset_in_chunk, length_to_read).await?;
                    Ok::<Bytes, std::io::Error>(bytes)
                } else {
                    // Cold read: fetch range from upstream directly without writing to cache
                    let chunk_start_byte = idx * chunk_size;
                    let read_start = chunk_start_byte + offset_in_chunk;
                    let read_end = read_start + length_to_read as u64;

                    let range_header = format!("bytes={}-{}", read_start, read_end - 1);
                    let mut stream = this.proxy.get_object_range_stream(&bucket, &key, &range_header).await
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

                    let mut buffer = Vec::with_capacity(length_to_read);
                    while let Some(chunk_res) = stream.next().await {
                        let bytes = chunk_res.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
                        buffer.extend_from_slice(&bytes);
                    }
                    Ok::<Bytes, std::io::Error>(Bytes::from(buffer))
                }
            };
            streams.push(fut);
        }

        // Return a stream that resolves each chunk read future sequentially
        let stream = futures_util::stream::iter(streams)
            .then(|fut| fut)
            .boxed();

        Ok(stream)
    }

    /// Drops the cache entries and files for a specific object.
    pub async fn drop_cache_for_object(&self, bucket: &str, key: &str) -> anyhow::Result<()> {
        let db_key = format!("{}/{}", bucket, key);
        if let Some(meta) = self.db.get_object(&db_key)? {
            let chunk_size = self.chunk_size as u64;
            let num_chunks = if meta.content_length > 0 {
                (meta.content_length - 1) / chunk_size + 1
            } else {
                0
            };
            for idx in 0..num_chunks {
                let _ = self.storage.delete_chunk(&db_key, idx).await;
                let _ = self.db.delete_chunk(&db_key, idx);
            }
        }
        self.db.delete_object(&db_key)?;
        Ok(())
    }

    /// Copies cached object metadata and chunk files to a new destination key.
    pub async fn copy_cache_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dest_bucket: &str,
        dest_key: &str,
    ) -> anyhow::Result<()> {
        let src_db_key = format!("{}/{}", src_bucket, src_key);
        let dest_db_key = format!("{}/{}", dest_bucket, dest_key);

        if let Some(src_meta) = self.db.get_object(&src_db_key)? {
            // Copy object metadata
            self.db.put_object(&dest_db_key, &src_meta)?;

            // Copy all complete chunks
            let chunk_size = self.chunk_size as u64;
            let num_chunks = if src_meta.content_length > 0 {
                (src_meta.content_length - 1) / chunk_size + 1
            } else {
                0
            };

            for idx in 0..num_chunks {
                if let Some(chunk_meta) = self.db.get_chunk(&src_db_key, idx)? {
                    if chunk_meta.status == ChunkStatus::Complete {
                        // Copy physical file
                        let src_path = self.storage.chunk_path(&src_db_key, idx);
                        let dest_path = self.storage.chunk_path(&dest_db_key, idx);
                        if src_path.exists() {
                            if let Some(parent) = dest_path.parent() {
                                tokio::fs::create_dir_all(parent).await?;
                            }
                            tokio::fs::copy(&src_path, &dest_path).await?;
                        }
                        // Copy metadata
                        self.db.put_chunk(&dest_db_key, idx, &chunk_meta)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Verifies if a chunk is cached, and if not, orchestrates download with single-flighting.
    async fn ensure_chunk_cached(
        &self,
        bucket: &str,
        key: &str,
        db_key: &str,
        chunk_idx: u64,
        start_byte: u64,
        end_byte: u64,
    ) -> std::io::Result<()> {
        let chunk_db_key = format!("{}:{}", db_key, chunk_idx);

        // 1. Fast path: Check DB
        if let Ok(Some(chunk_meta)) = self.db.get_chunk(db_key, chunk_idx) {
            if chunk_meta.status == ChunkStatus::Complete {
                // Update access time for LRU
                let mut updated_meta = chunk_meta;
                updated_meta.last_accessed_at = Self::now_secs();
                let _ = self.db.put_chunk(db_key, chunk_idx, &updated_meta);
                return Ok(());
            }
        }

        // 2. Slow path: Check active downloads (Single-Flight)
        loop {
            let rx = {
                if let Some(rx) = self.active_downloads.get(&chunk_db_key) {
                    Some(rx.value().clone())
                } else {
                    None
                }
            };

            if let Some(mut rx) = rx {
                // Wait for the download to complete
                loop {
                    if rx.changed().await.is_err() {
                        break;
                    }
                    match &*rx.borrow() {
                        DownloadStatus::Complete => return Ok(()),
                        DownloadStatus::Failed(e) => {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                format!("Single flight chunk download failed: {}", e),
                            ));
                        }
                        DownloadStatus::Pending => {}
                    }
                }
            } else {
                // Try to register active download
                let (tx, rx) = watch::channel(DownloadStatus::Pending);
                match self.active_downloads.entry(chunk_db_key.clone()) {
                    dashmap::mapref::entry::Entry::Occupied(_) => {
                        // Lost race, loop again to subscribe
                        continue;
                    }
                    dashmap::mapref::entry::Entry::Vacant(v) => {
                        v.insert(rx);
                    }
                }

                // Perform the download
                info!("Cache miss: downloading chunk {} for key {} (bytes {}-{})", chunk_idx, key, start_byte, end_byte);
                match self.download_and_cache_chunk(bucket, key, db_key, chunk_idx, start_byte, end_byte).await {
                    Ok(_) => {
                        let _ = tx.send(DownloadStatus::Complete);
                        self.active_downloads.remove(&chunk_db_key);
                        return Ok(());
                    }
                    Err(e) => {
                        error!("Failed downloading chunk {} for key {}: {:?}", chunk_idx, key, e);
                        let _ = tx.send(DownloadStatus::Failed(e.to_string()));
                        self.active_downloads.remove(&chunk_db_key);
                        return Err(std::io::Error::new(std::io::ErrorKind::Other, e));
                    }
                }
            }
        }
    }

    /// Fetches a specific range from upstream, aggregates it, and writes it to disk.
    async fn download_and_cache_chunk(
        &self,
        bucket: &str,
        key: &str,
        db_key: &str,
        chunk_idx: u64,
        start_byte: u64,
        end_byte: u64,
    ) -> anyhow::Result<()> {
        let range_header = format!("bytes={}-{}", start_byte, end_byte - 1);
        let mut stream = self.proxy.get_object_range_stream(bucket, key, &range_header).await?;
        
        let chunk_size = (end_byte - start_byte) as usize;
        let mut buffer = Vec::with_capacity(chunk_size);

        while let Some(chunk_res) = stream.next().await {
            let bytes = chunk_res?;
            buffer.extend_from_slice(&bytes);
        }

        if buffer.len() != chunk_size {
            anyhow::bail!(
                "Upstream chunk size mismatch. Expected {} bytes, got {} bytes.",
                chunk_size,
                buffer.len()
            );
        }

        // Write to storage
        self.storage.write_chunk(db_key, chunk_idx, &buffer).await?;

        // Update database
        let chunk_meta = ChunkMetadata {
            size: chunk_size as u32,
            last_accessed_at: Self::now_secs(),
            status: ChunkStatus::Complete,
        };
        self.db.put_chunk(db_key, chunk_idx, &chunk_meta)?;

        Ok(())
    }

    /// Writes an uploaded object stream to upstream S3 while concurrently caching chunks on disk.
    /// Returns the ETag of the uploaded object on success.
    pub async fn write_and_cache_object<S, E>(
        &self,
        bucket: &str,
        key: &str,
        mut body_stream: S,
        content_length: u64,
        content_type: Option<String>,
        _creds: Option<crate::utils::ClientCredentials>,
    ) -> anyhow::Result<String>
    where
        S: Stream<Item = Result<Bytes, E>> + Unpin + Send + 'static,
        E: Into<Box<dyn std::error::Error + Send + Sync>> + 'static,
    {
        let db_key = format!("{}/{}", bucket, key);
        let chunk_size = self.chunk_size;

        // Drop any existing cache for this object first to prevent stale chunks/metadata
        let _ = self.drop_cache_for_object(bucket, key).await;

        // 1. Create a channel to pipe bytes to the upstream S3 upload task
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(16);

        // 2. Wrap the channel Receiver in a Stream using futures_util::stream::unfold
        let upload_stream = futures_util::stream::unfold(rx, |mut rx| async move {
            match rx.recv().await {
                Some(res) => Some((res, rx)),
                None => None,
            }
        });

        // 3. Spawn a task to handle the upstream upload
        let proxy = self.proxy.clone();
        let bucket_clone = bucket.to_string();
        let key_clone = key.to_string();
        let content_type_upload = content_type.clone();
        let upload_handle = tokio::spawn(async move {
            proxy.put_object_stream(&bucket_clone, &key_clone, upload_stream, content_length, content_type_upload).await
        });

        // 4. Process incoming body stream: write chunks to disk & send to upload channel
        let mut current_chunk_idx: u64 = 0;
        let mut current_chunk_buffer = Vec::with_capacity(chunk_size);
        let mut chunks_written = Vec::new();
        let mut stream_processing_result = Ok(());

        while let Some(res) = body_stream.next().await {
            let bytes = match res {
                Ok(b) => b,
                Err(e) => {
                    stream_processing_result = Err(std::io::Error::new(std::io::ErrorKind::Other, e.into()));
                    break;
                }
            };

            // Pipe to upload channel
            if tx.send(Ok(bytes.clone())).await.is_err() {
                stream_processing_result = Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "Upstream upload channel closed prematurely"
                ));
                break;
            }

            current_chunk_buffer.extend_from_slice(&bytes);

            // Write chunks if boundary is crossed
            let mut write_err = None;
            while current_chunk_buffer.len() >= chunk_size {
                let chunk_data = current_chunk_buffer.drain(..chunk_size).collect::<Vec<u8>>();
                if let Err(e) = self.storage.write_chunk(&db_key, current_chunk_idx, &chunk_data).await {
                    write_err = Some(e);
                    break;
                }
                chunks_written.push((current_chunk_idx, chunk_size));
                current_chunk_idx += 1;
            }

            if let Some(e) = write_err {
                stream_processing_result = Err(e);
                break;
            }
        }

        if stream_processing_result.is_ok() && !current_chunk_buffer.is_empty() {
            let len = current_chunk_buffer.len();
            if let Err(e) = self.storage.write_chunk(&db_key, current_chunk_idx, &current_chunk_buffer).await {
                stream_processing_result = Err(e);
            } else {
                chunks_written.push((current_chunk_idx, len));
            }
        }

        // Close the channel to signal EOF to upstream
        drop(tx);

        match stream_processing_result {
            Ok(_) => {
                // Wait for the upstream upload to complete and get the raw ETag
                let etag = upload_handle.await??;

                // On upload success, register written chunks in Metadata DB
                for (idx, size) in chunks_written {
                    let chunk_meta = ChunkMetadata {
                        size: size as u32,
                        last_accessed_at: Self::now_secs(),
                        status: ChunkStatus::Complete,
                    };
                    self.db.put_chunk(&db_key, idx, &chunk_meta)?;
                }

                // Register object metadata
                let obj_meta = ObjectMetadata {
                    content_length,
                    content_type: content_type.clone(),
                    etag: Some(etag.clone()),
                    last_modified: Self::now_secs(),
                };
                self.db.put_object(&db_key, &obj_meta)?;

                info!("Successfully uploaded and cached: {}", db_key);
                Ok(etag)
            }
            Err(e) => {
                // If stream processing or writing fails, make sure we clean up chunks written during this session
                for idx in 0..=current_chunk_idx {
                    let _ = self.storage.delete_chunk(&db_key, idx).await;
                }
                Err(e.into())
            }
        }
    }
}
