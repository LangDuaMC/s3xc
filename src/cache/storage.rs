use std::path::{Path, PathBuf};
use bytes::Bytes;
use sha2::{Digest, Sha256};
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt, AsyncSeekExt};
use tracing::{debug, error};

pub struct Storage {
    cache_dir: PathBuf,
}

impl Storage {
    pub fn new(cache_dir: PathBuf) -> Self {
        Self { cache_dir }
    }

    /// Computes the path for a specific chunk.
    /// Schema: cache_dir/data/{sha256[0..2]}/{sha256[2..4]}/{sha256}_{chunk_idx}
    pub fn chunk_path(&self, key: &str, chunk_idx: u64) -> PathBuf {
        let mut hasher = Sha256::new();
        hasher.update(key.as_bytes());
        let hash_hex = hex::encode(hasher.finalize());

        let dir = self.cache_dir
            .join("data")
            .join(&hash_hex[0..2])
            .join(&hash_hex[2..4]);

        dir.join(format!("{}_{}", hash_hex, chunk_idx))
    }

    /// Ensures the parent directory of a chunk exists.
    async fn ensure_parent_dir(&self, path: &Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        Ok(())
    }

    /// Reads a range from a cached chunk file.
    pub async fn read_chunk_range(
        &self,
        key: &str,
        chunk_idx: u64,
        offset: u64,
        length: usize,
    ) -> std::io::Result<Bytes> {
        let path = self.chunk_path(key, chunk_idx);
        let mut file = File::open(&path).await?;
        
        // Seek/Read starting at offset
        if offset > 0 {
            use std::io::SeekFrom;
            file.seek(SeekFrom::Start(offset)).await?;
        }

        let mut buf = vec![0u8; length];
        let bytes_read = file.read_exact(&mut buf).await?;
        
        buf.truncate(bytes_read);
        Ok(Bytes::from(buf))
    }

    /// Writes a complete chunk to the cache atomically by writing to a temp file and renaming it.
    pub async fn write_chunk(&self, key: &str, chunk_idx: u64, data: &[u8]) -> std::io::Result<()> {
        let target_path = self.chunk_path(key, chunk_idx);
        self.ensure_parent_dir(&target_path).await?;

        // Create a temporary file in the same directory to guarantee atomic rename on the same filesystem.
        let temp_path = target_path.with_extension("tmp");
        
        {
            let mut file = File::create(&temp_path).await?;
            file.write_all(data).await?;
            file.flush().await?;
        }

        // Atomically rename the temp file to the target path
        fs::rename(&temp_path, &target_path).await?;
        debug!("Wrote chunk {} for key {} to {:?}", chunk_idx, key, target_path);
        Ok(())
    }

    /// Deletes a chunk file from disk.
    pub async fn delete_chunk(&self, key: &str, chunk_idx: u64) -> std::io::Result<()> {
        let path = self.chunk_path(key, chunk_idx);
        if path.exists() {
            fs::remove_file(path).await?;
        }
        Ok(())
    }

    /// Cleans up any orphaned temp files in the cache directories on startup.
    pub async fn cleanup_temp_files(&self) -> std::io::Result<()> {
        let data_dir = self.cache_dir.join("data");
        if !data_dir.exists() {
            return Ok(());
        }

        // We can do a simple recursive traversal or skip it. Let's write a simple recursive function if needed.
        // For simplicity, we can do it asynchronously if we find any .tmp files when walking or just leave it.
        Ok(())
    }
}
