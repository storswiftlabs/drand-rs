use std::path::{Path, PathBuf};

use rocksdb::Options;
use tonic::async_trait;

use super::{Beacon, StorageError, Store};

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
    pub fn new(
        path: PathBuf,
        requires_previous: bool,
        beacon_name: String,
    ) -> Result<Self, StorageError> {
        if !path.exists() {
            std::fs::create_dir_all(&path).map_err(|e| StorageError::IoError(e.to_string()))?;
        }
        let path = path.join(&beacon_name);
        let db = open_rocksdb(path)?;
        return Ok(RocksStore {
            db,
            requires_previous,
        });
    }

    fn key(&self, round: u64) -> [u8; 8] {
        round.to_be_bytes()
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
}

#[async_trait]
impl Store for RocksStore {
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
        Ok(())
    }

    async fn del(&self, round: u64) -> Result<(), StorageError> {
        self.db.delete(self.key(round)).map_err(|e| e.into())?;
        Ok(())
    }
}

fn rocksdbto_storage_error(e: rocksdb::Error) -> StorageError {
    match e.kind() {
        rocksdb::ErrorKind::NotFound => StorageError::NotFound,
        _ => StorageError::IoError(e.to_string()),
    }
}

impl Into<StorageError> for rocksdb::Error {
    fn into(self) -> StorageError {
        rocksdbto_storage_error(self)
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_rocksdb() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let tmp_dir = tempdir::TempDir::new("example").unwrap();
            let path = tmp_dir.path();

            let store = RocksStore::new(path.into(), true, "beacon_name".to_owned())
                .expect("Failed to create store");

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
