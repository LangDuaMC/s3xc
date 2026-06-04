use std::path::Path;
use std::sync::Arc;
use redb::{Database, TableDefinition, ReadableTable};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};


// Table definitions
const OBJECTS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("objects");
const CHUNKS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("chunks");

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectMetadata {
    pub content_length: u64,
    pub content_type: Option<String>,
    pub etag: Option<String>,
    pub last_modified: i64, // Unix timestamp in seconds
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ChunkStatus {
    Complete,
    Downloading,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMetadata {
    pub size: u32,
    pub last_accessed_at: i64, // Unix timestamp in seconds
    pub status: ChunkStatus,
}

pub struct MetadataDb {
    db: Arc<Database>,
}

impl MetadataDb {
    pub fn new(path: &Path) -> anyhow::Result<Self> {
        let db = Database::create(path)?;
        
        // Initialize tables by opening a write transaction and getting the tables
        let write_txn = db.begin_write()?;
        {
            let _ = write_txn.open_table(OBJECTS_TABLE)?;
            let _ = write_txn.open_table(CHUNKS_TABLE)?;
        }

        write_txn.commit()?;

        info!("Metadata database initialized at {:?}", path);
        Ok(Self { db: Arc::new(db) })
    }

    // Helper to serialize values
    fn serialize<T: Serialize>(val: &T) -> anyhow::Result<Vec<u8>> {
        Ok(serde_json::to_vec(val)?)
    }

    // Helper to deserialize values
    fn deserialize<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> anyhow::Result<T> {
        Ok(serde_json::from_slice(bytes)?)
    }

    pub fn get_object(&self, key: &str) -> anyhow::Result<Option<ObjectMetadata>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(OBJECTS_TABLE)?;
        if let Some(guard) = table.get(key)? {
            let meta = Self::deserialize(guard.value())?;
            Ok(Some(meta))
        } else {
            Ok(None)
        }
    }

    pub fn put_object(&self, key: &str, meta: &ObjectMetadata) -> anyhow::Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(OBJECTS_TABLE)?;
            let bytes = Self::serialize(meta)?;
            table.insert(key, bytes.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn delete_object(&self, key: &str) -> anyhow::Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(OBJECTS_TABLE)?;
            table.remove(key)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn get_chunk(&self, key: &str, chunk_idx: u64) -> anyhow::Result<Option<ChunkMetadata>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(CHUNKS_TABLE)?;
        let db_key = format!("{}:{}", key, chunk_idx);
        if let Some(guard) = table.get(db_key.as_str())? {
            let chunk = Self::deserialize(guard.value())?;
            Ok(Some(chunk))
        } else {
            Ok(None)
        }
    }

    pub fn put_chunk(&self, key: &str, chunk_idx: u64, chunk: &ChunkMetadata) -> anyhow::Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(CHUNKS_TABLE)?;
            let db_key = format!("{}:{}", key, chunk_idx);
            let bytes = Self::serialize(chunk)?;
            table.insert(db_key.as_str(), bytes.as_slice())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn delete_chunk(&self, key: &str, chunk_idx: u64) -> anyhow::Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut table = write_txn.open_table(CHUNKS_TABLE)?;
            let db_key = format!("{}:{}", key, chunk_idx);
            table.remove(db_key.as_str())?;
        }
        write_txn.commit()?;
        Ok(())
    }

    /// Fetches all chunks across all keys to construct a list for cache eviction.
    /// Returns a list of (db_key_string, ChunkMetadata)
    pub fn list_all_chunks(&self) -> anyhow::Result<Vec<(String, ChunkMetadata)>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(CHUNKS_TABLE)?;
        let mut chunks = Vec::new();
        for item in table.iter()? {
            let (key_guard, val_guard): (redb::AccessGuard<&str>, redb::AccessGuard<&[u8]>) = item?;
            let db_key = key_guard.value().to_string();
            let chunk_meta: ChunkMetadata = Self::deserialize(val_guard.value())?;
            chunks.push((db_key, chunk_meta));
        }
        Ok(chunks)
    }
}
