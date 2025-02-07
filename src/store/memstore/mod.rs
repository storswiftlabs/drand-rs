use std::collections::BTreeMap;
use tokio::sync::RwLock;
use tonic::async_trait;

use super::{Beacon, StorageError, Store};

pub struct MemStore {
    // Map of round -> signature
    data: RwLock<BTreeMap<u64, Vec<u8>>>,
    requires_previous: bool,
}

impl MemStore {
    pub fn new(requires_previous: bool) -> Self {
        MemStore {
            data: RwLock::new(BTreeMap::new()),
            requires_previous,
        }
    }

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
}

#[async_trait]
impl Store for MemStore {
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
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_rocksdb() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let store = MemStore::new(true);

            store
                .put(Beacon {
                    previous_sig: vec![1, 2, 3],
                    round: 0,
                    signature: vec![4, 5, 6],
                })
                .await
                .expect("Failed to put beacon");

            store
                .put(Beacon {
                    previous_sig: vec![4, 5, 6],
                    round: 1,
                    signature: vec![7, 8, 9],
                })
                .await
                .expect("Failed to put beacon");

            let b = store.get(1).await.expect("Failed to get beacon");

            assert_eq!(
                b.previous_sig,
                vec![4, 5, 6],
                "Previous signature does not match"
            );
            assert_eq!(b.round, 1, "Round does not match");
            assert_eq!(b.signature, vec![7, 8, 9], "Signature does not match");

            let len = store.len().await.expect("Failed to get length");

            assert_eq!(len, 2, "Length should be 2");

            let b = store.last().await.expect("Failed to get last");
            assert_eq!(
                b.previous_sig,
                vec![4, 5, 6],
                "Previous signature does not match"
            );
            assert_eq!(b.round, 1, "Round does not match");
            assert_eq!(b.signature, vec![7, 8, 9], "Signature does not match");

            store.del(1).await.expect("Failed to delete");

            let len = store.len().await.expect("Failed to get length");

            assert_eq!(len, 1, "Length should be 1");
        });
    }
}
