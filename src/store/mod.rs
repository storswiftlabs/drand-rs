use tonic::async_trait;

use crate::protobuf::drand::BeaconPacket;

#[cfg(feature = "postgres")]
pub mod postgres;

#[cfg(feature = "rocksdb")]
pub mod rocksdb;

#[cfg(feature = "memstore")]
pub mod memstore;

#[cfg(test)]
mod testing;

#[derive(Debug)]
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

#[derive(Debug)]
pub enum StorageError {
    IoError(String),
    NotFound,
    KeyError(String),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::IoError(e) => write!(f, "IoError:: {}", e),
            StorageError::NotFound => write!(f, "Not found"),
            StorageError::KeyError(e) => write!(f, "KeyError:: {}", e),
        }
    }
}

#[async_trait]
pub trait BeaconCursor {
    async fn first(&mut self) -> Result<Option<Beacon>, StorageError>;
    async fn next(&mut self) -> Result<Option<Beacon>, StorageError>;
    async fn seek(&mut self, round: u64) -> Result<Option<Beacon>, StorageError>;
    async fn last(&mut self) -> Result<Option<Beacon>, StorageError>;
}
