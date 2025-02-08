use tonic::async_trait;

use super::MemStore;

use crate::store::{Beacon, BeaconCursor, StorageError, Store};

pub struct MemDbCursor<'a> {
    pos: usize,
    store: &'a MemStore,
}

impl<'a> MemDbCursor<'a> {
    pub fn new(store: &'a MemStore) -> Self {
        MemDbCursor { pos: 0, store }
    }
}

#[async_trait]
impl BeaconCursor for MemDbCursor<'_> {
    async fn first(&mut self) -> Result<Option<Beacon>, StorageError> {
        match self.store.first().await {
            Ok(beacon) => {
                self.pos = 0;
                Ok(Some(beacon))
            }
            Err(StorageError::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn next(&mut self) -> Result<Option<Beacon>, StorageError> {
        match self.store.get_at_position(self.pos + 1).await {
            Ok(beacon) => {
                self.pos += 1;
                Ok(Some(beacon))
            }
            Err(StorageError::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn seek(&mut self, round: u64) -> Result<Option<Beacon>, StorageError> {
        match self.store.get_beacon(round).await {
            Ok(beacon) => {
                self.pos = self.store.get_pos(beacon.round).await?;
                Ok(Some(beacon))
            }
            Err(StorageError::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn last(&mut self) -> Result<Option<Beacon>, StorageError> {
        match self.store.last().await {
            Ok(beacon) => {
                self.pos = self.store.len().await? - 1;
                Ok(Some(beacon))
            }
            Err(StorageError::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }
}
