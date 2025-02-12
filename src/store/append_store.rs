use tokio::sync::Mutex;

use tonic::async_trait;

use super::{Beacon, StorageError, Store};

// appendStore is a store that only appends new block with a round +1 from the
// last block inserted and with the corresponding previous signature
pub struct AppendStore<T: Store> {
    store: T,
    last: Mutex<Beacon>,
}

impl<T: Store> AppendStore<T> {
    pub async fn new(store: T) -> Result<Self, StorageError> {
        let last = store.last().await?;
        Ok(AppendStore {
            store,
            last: Mutex::new(last),
        })
    }
}

#[async_trait]
impl<T: Store + Sync + Send> Store for AppendStore<T> {
    type Cursor<'a>
        = T::Cursor<'a>
    where
        T: 'a;
    async fn len(&self) -> Result<usize, StorageError> {
        self.store.len().await
    }

    async fn put(&self, b: Beacon) -> Result<(), StorageError> {
        let mut last = self.last.lock().await;
        if b.round == last.round {
            if last.signature == b.signature {
                if last.previous_sig == b.previous_sig {
                    return Err(StorageError::AlreadyExists);
                }
                return Err(StorageError::DuplicateKey(
                    "duplicate round with different previous signature".to_string(),
                ));
            }
            return Err(StorageError::DuplicateKey(
                "duplicate round with different signature".to_string(),
            ));
        }

        if b.round != last.round + 1 {
            return Err(StorageError::InvalidKey(format!(
                "invalid round: last {}, new: {}",
                last.round, b.round,
            )));
        }

        self.store.put(b.clone()).await?;
        *last = b;
        Ok(())
    }

    async fn last(&self) -> Result<Beacon, StorageError> {
        self.store.last().await
    }

    async fn first(&self) -> Result<Beacon, StorageError> {
        self.store.first().await
    }

    async fn get(&self, round: u64) -> Result<Beacon, StorageError> {
        self.store.get(round).await
    }

    async fn close(self) -> Result<(), StorageError> {
        self.store.close().await
    }

    async fn del(&self, round: u64) -> Result<(), StorageError> {
        self.store.del(round).await
    }

    fn cursor(&self) -> Self::Cursor<'_> {
        self.store.cursor()
    }
}
