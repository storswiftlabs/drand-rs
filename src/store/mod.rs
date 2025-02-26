use std::path::Path;

use thiserror::Error;
use tonic::async_trait;

use crate::key::store::FileStoreError;
use crate::protobuf::drand::BeaconPacket;
// #[cfg(feature = "postgres")]
// pub mod postgres;

// #[cfg(feature = "rocksdb")]
// pub mod rocksdb;

#[cfg(feature = "memstore")]
pub mod memstore;

// pub mod append_store;

// #[cfg(feature = "rocksdb")]
// pub type ChainStore = rocksdb::RocksStore;

// #[cfg(feature = "postgres")]
// pub type ChainStore = postgres::PgStore;

#[cfg(feature = "memstore")]
pub type ChainStore = memstore::MemStore;

// pub type AppendStore = append_store::AppendStore<ChainStore>;

#[cfg(test)]
mod testing;

#[derive(Debug, Clone)]
pub struct Beacon {
    // PreviousSig is the previous signature generated
    pub previous_sig: Vec<u8>,
    // Round is the round number this beacon is tied to
    pub round: u64,
    // Signature is the BLS deterministic signature as per the crypto.Scheme used
    pub signature: Vec<u8>,
}

impl From<BeaconPacket> for Beacon {
    fn from(pb: BeaconPacket) -> Self {
        Self {
            previous_sig: pb.previous_signature,
            round: pb.round,
            signature: pb.signature,
        }
    }
}

#[allow(clippy::len_without_is_empty)]
#[async_trait]
pub trait Store {
    type Cursor<'a>: BeaconCursor + 'a
    where
        Self: 'a;

    async fn len(&self) -> Result<usize, StorageError>;
    async fn put(&self, b: Beacon) -> Result<(), StorageError>;
    async fn first(&self) -> Result<Beacon, StorageError>;
    async fn last(&self) -> Result<Beacon, StorageError>;
    async fn get(&self, round: u64) -> Result<Beacon, StorageError>;
    async fn close(self) -> Result<(), StorageError>;
    async fn del(&self, round: u64) -> Result<(), StorageError>;
    fn cursor(&self) -> Self::Cursor<'_>;
}

#[async_trait]
pub trait NewStore: Sized {
    fn new(path: &Path, requires_previous: bool) -> Result<Self, StorageError>;
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("IO error: {0}")]
    IoError(String),
    #[error("Not found")]
    NotFound,
    #[error("Key error: {0}")]
    KeyError(String),
    #[error("Already exists")]
    AlreadyExists,
    #[error("Duplicate key: {0}")]
    DuplicateKey(String),
    #[error("Invalid key: {0}")]
    InvalidKey(String),
    #[error("Invalid config: {0}")]
    InvalidConfig(String),
    #[error("Migrate error: {0}")]
    MigrateError(String),
}

impl From<StorageError> for FileStoreError {
    fn from(value: StorageError) -> Self {
        Self::ChainStore(value.to_string())
    }
}

#[async_trait]
pub trait BeaconCursor {
    async fn first(&mut self) -> Result<Option<Beacon>, StorageError>;
    async fn next(&mut self) -> Result<Option<Beacon>, StorageError>;
    async fn seek(&mut self, round: u64) -> Result<Option<Beacon>, StorageError>;
    async fn last(&mut self) -> Result<Option<Beacon>, StorageError>;
}
