use tonic::async_trait;

use crate::protobuf::drand::BeaconPacket;

#[cfg(feature = "postgres")]
pub mod postgres;

#[cfg(feature = "rocksdb")]
pub mod rocksdb;

#[cfg(feature = "memstore")]
pub mod memstore;

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

#[async_trait]
pub trait Store {
    async fn len(&self) -> Result<usize, StorageError>;
    async fn put(&self, b: Beacon) -> Result<(), StorageError>;
    async fn last(&self) -> Result<Beacon, StorageError>;
    async fn get(&self, round: u64) -> Result<Beacon, StorageError>;
    async fn close(self) -> Result<(), StorageError>;
    async fn del(&self, round: u64) -> Result<(), StorageError>;
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
