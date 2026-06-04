use std::sync::Arc;
use std::time::Duration;
use tracing::{info, error, warn, debug};
use crate::cache::metadata::{MetadataDb, ChunkStatus};
use crate::cache::storage::Storage;

pub struct EvictionManager {
    db: Arc<MetadataDb>,
    storage: Arc<Storage>,
    max_size: u64,
    target_size: u64,
}

impl EvictionManager {
    pub fn new(
        db: Arc<MetadataDb>,
        storage: Arc<Storage>,
        max_size: u64,
    ) -> Self {
        // Target 90% of max size when evicting
        let target_size = (max_size as f64 * 0.9) as u64;
        Self {
            db,
            storage,
            max_size,
            target_size,
        }
    }

    /// Starts the background eviction loop.
    pub fn start(self: Arc<Self>) {
        tokio::spawn(async move {
            info!("Eviction worker started. Max cache size: {} bytes, target size: {} bytes", self.max_size, self.target_size);
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                if let Err(e) = self.check_and_evict().await {
                    error!("Error during cache eviction: {:?}", e);
                }
            }
        });
    }

    /// Calculates current cache size and triggers eviction if it exceeds max_size.
    pub async fn check_and_evict(&self) -> anyhow::Result<u64> {
        let chunks = self.db.list_all_chunks()?;
        
        let mut total_size: u64 = 0;
        let mut candidates = Vec::new();

        for (db_key, chunk) in chunks {
            total_size += chunk.size as u64;
            // Only evict complete chunks to avoid breaking active downloads
            if chunk.status == ChunkStatus::Complete {
                candidates.push((db_key, chunk.size, chunk.last_accessed_at));
            }
        }

        debug!("Current total cache size: {}/{} bytes ({} chunks)", total_size, self.max_size, candidates.len());

        if total_size <= self.max_size {
            return Ok(0);
        }

        let bytes_to_evict = total_size.saturating_sub(self.target_size);
        info!("Cache limit exceeded. Total size: {} bytes. Evicting {} bytes...", total_size, bytes_to_evict);

        // Sort candidates by last_accessed_at ascending (oldest first)
        candidates.sort_by_key(|&(_, _, last_accessed)| last_accessed);

        let mut bytes_evicted: u64 = 0;
        for (db_key, size, _) in candidates {
            if bytes_evicted >= bytes_to_evict {
                break;
            }

            // db_key is formatted as "key:chunk_idx"
            if let Some(pos) = db_key.rfind(':') {
                let (key, chunk_idx_str) = db_key.split_at(pos);
                let chunk_idx_str = &chunk_idx_str[1..]; // skip ':'
                if let Ok(chunk_idx) = chunk_idx_str.parse::<u64>() {
                    // Delete file from disk
                    if let Err(e) = self.storage.delete_chunk(key, chunk_idx).await {
                        warn!("Failed to delete chunk file {} for {}: {:?}", chunk_idx, key, e);
                    }
                    
                    // Delete entry from database
                    if let Err(e) = self.db.delete_chunk(key, chunk_idx) {
                        warn!("Failed to delete chunk metadata {} for {}: {:?}", chunk_idx, key, e);
                    } else {
                        bytes_evicted += size as u64;
                        debug!("Evicted chunk {} for key {} ({} bytes)", chunk_idx, key, size);
                    }
                }
            }
        }

        info!("Eviction run completed. Successfully freed {} bytes.", bytes_evicted);
        Ok(bytes_evicted)
    }
}
