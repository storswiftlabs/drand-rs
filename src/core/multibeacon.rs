use super::beacon::BeaconCmd;
use super::beacon::BeaconID;
use super::beacon::BeaconProcess;

use crate::key::store::FileStore;
use crate::key::store::FileStoreError;
use crate::key::Scheme;

use arc_swap::ArcSwap;
use arc_swap::ArcSwapAny;
use arc_swap::Guard;
use energon::drand::schemes::*;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio_util::task::TaskTracker;

pub type Snapshot = Guard<Arc<Vec<BeaconHandler>>>;

/// Handler for sending commands to the beacon node
#[derive(Clone)]
pub struct BeaconHandler {
    pub beacon_id: BeaconID,
    pub tx: Sender<BeaconCmd>,
    pub tracker: TaskTracker,
}

impl BeaconHandler {
    pub fn new(id: &str, fs: FileStore) -> Result<Self, FileStoreError> {
        // get scheme from filestore
        let pair = fs.load_key_pair_toml()?;
        let scheme = pair
            .get_scheme_id()
            .ok_or(FileStoreError::InvalidPairSchemes)?;

        // Attempt to initialize Beacon<S>
        let handler = match scheme {
            DefaultScheme::ID => BeaconProcess::<DefaultScheme>::run(fs, pair, id)?,
            UnchainedScheme::ID => BeaconProcess::<UnchainedScheme>::run(fs, pair, id)?,
            SigsOnG1Scheme::ID => BeaconProcess::<SigsOnG1Scheme>::run(fs, pair, id)?,
            BN254UnchainedOnG1Scheme::ID => {
                BeaconProcess::<BN254UnchainedOnG1Scheme>::run(fs, pair, id)?
            }
            _ => return Err(FileStoreError::FailedInitID)?,
        };

        Ok(handler)
    }

    pub fn id(&self) -> &BeaconID {
        &self.beacon_id
    }
    pub fn sender(&self) -> &Sender<BeaconCmd> {
        &self.tx
    }
}

/// An atomic storage to keep [`BeaconHandler`] collection
pub struct MultiBeacon(ArcSwapAny<Arc<Vec<BeaconHandler>>>);

impl MultiBeacon {
    /// This call is success only if *all* detected storages has minimal valid structure.
    /// Succesfull value contains a turple with valid absolute path to multibeacon folder.
    pub fn new(folder: &str, id: Option<&str>) -> Result<(PathBuf, Self), FileStoreError> {
        let (multibeacon_path, fstores) = FileStore::read_multibeacon_folder(folder)?;
        let mut handlers = vec![];
        match id {
            // Non-empty value means request to load single beacon id
            Some(id) => {
                let fs = fstores
                    .into_iter()
                    .find(|fs| fs.get_beacon_id() == Some(id))
                    .ok_or(FileStoreError::BeaconNotFound)?;
                let handler = BeaconHandler::new(id, fs)?;
                handlers.push(handler);
            }
            // Empty value means request to load all present beacon ids
            None => {
                for fs in fstores.into_iter() {
                    let id = fs
                        .get_beacon_id()
                        .ok_or(FileStoreError::BeaconNotFound)?
                        .to_owned();
                    let handler = BeaconHandler::new(&id, fs)?;
                    handlers.push(handler);
                }
            }
        }
        let multibeacon = Self(ArcSwap::from(Arc::new(handlers)));

        Ok((multibeacon_path, multibeacon))
    }

    pub fn snapshot(&self) -> Snapshot {
        self.0.load()
    }

    /// Replaces the value inside this instance
    pub fn replace_store(&self, val: Arc<Vec<BeaconHandler>>) {
        self.0.store(val)
    }

    /// Sends a command to the beacon identified by `id`.
    /// Returns an error if the id is not presented in store or if sending the command fails.
    pub async fn cmd(&self, cmd: BeaconCmd, id: &str) -> Result<(), BeaconHandlerError> {
        let store = self.0.load();
        let handler = store
            .iter()
            .find(|h| h.beacon_id.is_eq(id))
            .ok_or(BeaconHandlerError::UnknownID)?;

        handler
            .tx
            .send(cmd)
            .await
            .map_err(|_| BeaconHandlerError::SendError)?;

        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum BeaconHandlerError {
    #[error("Beacon id is not found")]
    UnknownID,
    /// For diagnostic, should not be possible.
    #[error("SendError: Beacon cmd receiver has been dropped")]
    SendError,
    #[error("Beacon id is already loaded")]
    AlreadyLoaded,
}
