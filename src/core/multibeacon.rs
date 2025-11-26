use super::beacon::{BeaconCmd, BeaconID, BeaconProcess};
use crate::{
    cli::Config,
    info,
    key::{
        store::{FileStore, FileStoreError},
        Scheme,
    },
    log::Logger,
    net::{pool::PoolSender, protocol::PartialMsg},
};
use arc_swap::{ArcSwap, ArcSwapAny, Guard};
use energon::drand::schemes::{BN254UnchainedOnG1Scheme, DefaultScheme, SigsOnG1Scheme};
use std::{path::PathBuf, sync::Arc};
use tokio::sync::mpsc;

/// Atomic storage for beacon handlers.
type Snapshot = Guard<Arc<Vec<BeaconHandler>>>;

#[derive(thiserror::Error, Debug)]
pub enum BeaconHandlerError {
    #[error("beacon_id is not found")]
    UnknownID,
    #[error("beacon_id is already loaded")]
    AlreadyLoaded,
    #[error("packet metadata is missing")]
    MetadataRequired,
    #[error("receiver for beacon process has been closed unexpectedly")]
    ClosedBpRx,
    #[error("failed to receive stop_id response - callback is dropped")]
    DroppedCallback,
    #[error(transparent)]
    ShutdownError(#[from] super::beacon::ShutdownError),
}

/// Handler for sending commands to the beacon node
#[derive(Clone)]
pub struct BeaconHandler {
    pub beacon_id: BeaconID,
    /// Sender for beacon commands
    pub process_tx: mpsc::Sender<BeaconCmd>,
    /// Sender for partial signature packets (hot path)
    pub partial_tx: mpsc::Sender<PartialMsg>,
}

impl BeaconHandler {
    pub fn new(
        fs: FileStore,
        pool: PoolSender,
        private_listen: String,
    ) -> Result<Self, FileStoreError> {
        let pair = &fs.load_key_pair_toml()?;
        let scheme = pair
            .get_scheme_id()
            .ok_or(FileStoreError::InvalidPairSchemes)?;

        let handler = match scheme {
            DefaultScheme::ID => {
                BeaconProcess::<DefaultScheme>::run(fs, pair, pool, private_listen)?
            }
            SigsOnG1Scheme::ID => {
                BeaconProcess::<SigsOnG1Scheme>::run(fs, pair, pool, private_listen)?
            }
            BN254UnchainedOnG1Scheme::ID => {
                BeaconProcess::<BN254UnchainedOnG1Scheme>::run(fs, pair, pool, private_listen)?
            }
            _ => return Err(FileStoreError::FailedInitID),
        };

        Ok(handler)
    }

    pub fn id(&self) -> &BeaconID {
        &self.beacon_id
    }
}

pub struct MultiBeacon {
    /// Atomic storage for beacon handlers.
    beacons: ArcSwapAny<Arc<Vec<BeaconHandler>>>,
    /// Sender for partial beacons pool.
    tx_pool: PoolSender,
}

impl MultiBeacon {
    /// This call is success only if *all* detected storages has minimal valid structure.
    /// Succesfull value contains a turple with valid absolute path to multibeacon folder.
    pub fn new(
        config: Config,
        tx_pool: PoolSender,
        log: &Logger,
    ) -> Result<(PathBuf, Self), FileStoreError> {
        let (multibeacon_path, fstores) = FileStore::read_multibeacon_folder(&config.folder)?;
        info!(
            log,
            "Detected stores: folder {}, amount {}",
            multibeacon_path.display(),
            fstores.len()
        );
        let beacons: Vec<BeaconHandler> = match &config.id {
            // Load single id
            Some(id) => {
                let fs = fstores
                    .into_iter()
                    .find(|fs| fs.get_beacon_id() == Some(id))
                    .ok_or(FileStoreError::BeaconNotFound)?;
                vec![BeaconHandler::new(
                    fs,
                    tx_pool.clone(),
                    config.private_listen,
                )?]
            }
            // Load all ids
            None => fstores
                .into_iter()
                .map(|fs| BeaconHandler::new(fs, tx_pool.clone(), config.private_listen.clone()))
                .collect::<Result<_, _>>()?,
        };
        let multibeacon = Self {
            beacons: ArcSwap::from(Arc::new(beacons)),
            tx_pool,
        };

        Ok((multibeacon_path, multibeacon))
    }

    pub fn snapshot(&self) -> Snapshot {
        self.beacons.load()
    }

    /// Replaces the value inside this instance
    pub fn replace_store(&self, val: Arc<Vec<BeaconHandler>>) {
        self.beacons.store(val);
    }

    /// Sends a command to the beacon identified by `id`.
    /// Returns an error if the id is not presented in store or if sending the command fails.
    pub async fn cmd(&self, cmd: BeaconCmd, id: &str) -> Result<(), BeaconHandlerError> {
        let store = self.beacons.load();
        let handler = store
            .iter()
            .find(|h| h.beacon_id.is_eq(id))
            .ok_or(BeaconHandlerError::UnknownID)?;

        handler
            .process_tx
            .send(cmd)
            .await
            .map_err(|_| BeaconHandlerError::ClosedBpRx)?;

        Ok(())
    }

    pub async fn send_partial(&self, partial: PartialMsg) -> Result<(), BeaconHandlerError> {
        let id = partial.0.packet.metadata.as_ref().map_or_else(
            || Err(BeaconHandlerError::MetadataRequired),
            |meta| Ok(meta.beacon_id.as_str()),
        )?;

        let store = self.beacons.load();
        let handler = store
            .iter()
            .find(|h| h.beacon_id.is_eq(id))
            .ok_or(BeaconHandlerError::UnknownID)?;

        handler
            .partial_tx
            .send(partial)
            .await
            .map_err(|_| BeaconHandlerError::ClosedBpRx)?;

        Ok(())
    }

    pub(super) fn get_pool(&self) -> PoolSender {
        self.tx_pool.clone()
    }
}
