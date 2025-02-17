use std::path::Path;

use cursor::RocksCursor;
use rocksdb::Options;
use tonic::async_trait;

use super::NewStore;
use super::StorageConfig;
use super::{Beacon, StorageError, Store};

pub mod cursor;

pub fn open_rocksdb<P: AsRef<Path>>(path: P) -> Result<rocksdb::DB, StorageError> {
    let mut option = Options::default();
    option.create_if_missing(true);
    option.set_compression_type(rocksdb::DBCompressionType::Lz4);
    let db = rocksdb::DB::open(&option, path).map_err(|e| e.into())?;
    Ok(db)
}

pub struct RocksStore {
    // db is the database connection
    db: rocksdb::DB,
    requires_previous: bool,
}

impl RocksStore {
    fn key(&self, round: u64) -> [u8; 8] {
        round.to_be_bytes()
    }

    fn decode_key(&self, key: &[u8]) -> u64 {
        u64::from_be_bytes(key[..8].try_into().unwrap())
    }

    async fn get_beacon(&self, round: u64) -> Result<Beacon, StorageError> {
        let signature = self.db.get(self.key(round)).map_err(|e| e.into())?;

        let signature = match signature {
            Some(s) => s,
            None => return Err(StorageError::NotFound),
        };

        let beacon = Beacon {
            round,
            signature,
            previous_sig: vec![],
        };

        Ok(beacon)
    }

    async fn get_at_position(&self, pos: usize) -> Result<Beacon, StorageError> {
        let (round, signature) = self
            .db
            .iterator(rocksdb::IteratorMode::Start)
            .nth(pos)
            .ok_or(StorageError::NotFound)?
            .map_err(|e| e.into())?;

        let mut beacon = Beacon {
            round: self.decode_key(&round),
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
        let mut pos = 0;

        for data in self.db.iterator(rocksdb::IteratorMode::Start) {
            let (k, _) = data.map_err(|e| e.into())?;
            let r = u64::from_be_bytes(k[..8].try_into().unwrap());
            if r == round {
                break;
            }
            pos += 1;
        }

        Ok(pos)
    }
}

#[async_trait]
impl NewStore for RocksStore {
    async fn new(config: StorageConfig, requires_previous: bool) -> Result<Self, StorageError> {
        let path = config
            .path
            .ok_or(StorageError::InvalidConfig("empty path".to_string()))?;

        let path = path.join("rocksdb");

        if !path.exists() {
            std::fs::create_dir_all(&path).map_err(|e| StorageError::IoError(e.to_string()))?;
        }
        let db = open_rocksdb(path)?;

        Ok(RocksStore {
            db,
            requires_previous,
        })
    }
}

#[async_trait]
impl Store for RocksStore {
    type Cursor<'a> = RocksCursor<'a>;
    async fn len(&self) -> Result<usize, StorageError> {
        let res = self.db.iterator(rocksdb::IteratorMode::End).count();
        Ok(res)
    }

    async fn put(&self, b: Beacon) -> Result<(), StorageError> {
        self.db
            .put(self.key(b.round), b.signature)
            .map_err(|e| e.into())?;
        Ok(())
    }

    async fn last(&self) -> Result<Beacon, StorageError> {
        let mut iter = self.db.iterator(rocksdb::IteratorMode::End);

        let (key, value) = match iter.next() {
            Some(res) => match res {
                Ok(res) => res,
                Err(_) => return Err(StorageError::NotFound),
            },
            None => return Err(StorageError::NotFound),
        };

        let round = self.decode_key(&key);

        let mut beacon = Beacon {
            round,
            signature: value.to_vec(),
            previous_sig: vec![],
        };

        if beacon.round > 0 && self.requires_previous {
            let prev = self.get_beacon(beacon.round - 1).await?;
            beacon.previous_sig = prev.signature;
        }

        Ok(beacon)
    }

    async fn first(&self) -> Result<Beacon, StorageError> {
        let mut iter = self.db.iterator(rocksdb::IteratorMode::Start);

        let (key, value) = match iter.next() {
            Some(res) => match res {
                Ok(res) => res,
                Err(_) => return Err(StorageError::NotFound),
            },
            None => return Err(StorageError::NotFound),
        };

        let round = u64::from_be_bytes(key[..8].try_into().unwrap());

        let mut beacon = Beacon {
            round,
            signature: value.to_vec(),
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
        self.db.cancel_all_background_work(true);
        Ok(())
    }

    async fn del(&self, round: u64) -> Result<(), StorageError> {
        self.db.delete(self.key(round)).map_err(|e| e.into())?;
        Ok(())
    }

    fn cursor(&self) -> RocksCursor<'_> {
        RocksCursor::new(self)
    }
}

fn rocksdbto_storage_error(e: rocksdb::Error) -> StorageError {
    match e.kind() {
        rocksdb::ErrorKind::NotFound => StorageError::NotFound,
        _ => StorageError::IoError(e.to_string()),
    }
}

#[allow(clippy::from_over_into)]
impl Into<StorageError> for rocksdb::Error {
    fn into(self) -> StorageError {
        rocksdbto_storage_error(self)
    }
}

#[cfg(test)]
mod tests {

    use tempfile::tempdir;

    use crate::store::testing::{test_cursor, test_store};

    use super::*;

    #[test]
    fn test_rocksdb() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let tmp_dir = tempdir().unwrap();
            let path = tmp_dir.path();

            let mut store = RocksStore::new(
                StorageConfig {
                    path: Some(path.to_str().unwrap().to_string()),
                    beacon_id: "beacon_id".to_string(),
                    ..Default::default()
                },
                true,
            )
            .await
            .expect("Failed to create store");

            test_store(&store).await;

            store.requires_previous = false;

            test_cursor(&store).await;

            tmp_dir.close().unwrap();
        });
    }
}
