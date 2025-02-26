use cursor::MemDbCursor;
use std::collections::BTreeMap;
use std::path::Path;
use tokio::sync::RwLock;
use tonic::async_trait;

use super::{Beacon, NewStore, StorageError, Store};

pub mod cursor;

pub struct MemStore {
    // Map of round -> signature
    data: RwLock<BTreeMap<u64, Vec<u8>>>,
    requires_previous: bool,
}

impl MemStore {
    async fn get_beacon(&self, round: u64) -> Result<Beacon, StorageError> {
        let datastore = self.data.read().await;
        let signature = datastore.get(&round).ok_or(StorageError::NotFound)?;

        let beacon = Beacon {
            round,
            signature: signature.to_vec(),
            previous_sig: vec![],
        };

        Ok(beacon)
    }

    async fn get_at_position(&self, pos: usize) -> Result<Beacon, StorageError> {
        let datastore = self.data.read().await;

        let (round, signature) = datastore.iter().nth(pos).ok_or(StorageError::NotFound)?;

        let mut beacon = Beacon {
            round: *round,
            signature: signature.to_vec(),
            previous_sig: vec![],
        };

        if beacon.round > 0 && self.requires_previous {
            let prev = self.get_beacon(beacon.round - 1).await?;
            beacon.previous_sig = prev.signature;
        }

        Ok(beacon)
    }

    async fn get_pos(&self, round: u64) -> Result<usize, StorageError> {
        let datastore = self.data.read().await;

        let mut pos = 0;

        for (r, _) in datastore.iter() {
            if *r == round {
                break;
            }
            pos += 1;
        }

        Ok(pos)
    }
}

impl NewStore for MemStore {
    fn new(_path: &Path, requires_previous: bool) -> Result<Self, StorageError> {
        Ok(MemStore {
            data: RwLock::new(BTreeMap::new()),
            requires_previous,
        })
    }
}

#[async_trait]
impl Store for MemStore {
    type Cursor<'a> = MemDbCursor<'a>;

    async fn len(&self) -> Result<usize, StorageError> {
        let datastore = self.data.read().await;
        Ok(datastore.len())
    }

    async fn put(&self, b: Beacon) -> Result<(), StorageError> {
        let mut datastore = self.data.write().await;
        datastore.insert(b.round, b.signature);
        Ok(())
    }

    async fn last(&self) -> Result<Beacon, StorageError> {
        let datastore = self.data.read().await;

        let (round, signature) = datastore.last_key_value().ok_or(StorageError::NotFound)?;

        let mut beacon = Beacon {
            round: *round,
            signature: signature.to_vec(),
            previous_sig: vec![],
        };

        if beacon.round > 0 && self.requires_previous {
            let prev = self.get_beacon(beacon.round - 1).await?;
            beacon.previous_sig = prev.signature;
        }

        Ok(beacon)
    }

    async fn first(&self) -> Result<Beacon, StorageError> {
        let datastore = self.data.read().await;

        let (round, signature) = datastore.first_key_value().ok_or(StorageError::NotFound)?;

        let mut beacon = Beacon {
            round: *round,
            signature: signature.to_vec(),
            previous_sig: vec![],
        };

        if beacon.round > 0 && self.requires_previous {
            let prev = self.get_beacon(beacon.round - 1).await?;
            beacon.previous_sig = prev.signature;
        }

        Ok(beacon)
    }

    async fn get(&self, round: u64) -> Result<Beacon, StorageError> {
        let mut beacon = self.get_beacon(round).await?;

        if round > 0 && self.requires_previous {
            let prev = self.get_beacon(round - 1).await?;
            beacon.previous_sig = prev.signature;
        }

        Ok(beacon)
    }

    async fn close(self) -> Result<(), StorageError> {
        Ok(())
    }

    async fn del(&self, round: u64) -> Result<(), StorageError> {
        let mut datastore = self.data.write().await;
        datastore.remove(&round);
        Ok(())
    }

    fn cursor(&self) -> MemDbCursor<'_> {
        MemDbCursor::new(self)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use crate::store::testing::{test_cursor, test_store};

    #[test]
    fn test_memstore() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut store = MemStore::new(Path::new("unused"), true).unwrap();
            test_store(&store).await;

            store.requires_previous = false;

            test_cursor(&store).await;
        });
    }
}
