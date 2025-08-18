use super::beacon::BeaconCmd;
use super::beacon::BeaconID;
use super::beacon::BeaconProcess;

use crate::chain::PartialPacket;
use crate::cli::Config;
use crate::key::store::FileStore;
use crate::key::store::FileStoreError;
use crate::key::Scheme;
use crate::net::pool::Pool;
use crate::net::pool::PoolSender;

use arc_swap::ArcSwap;
use arc_swap::ArcSwapAny;
use arc_swap::Guard;
use energon::drand::schemes::DefaultScheme;
use energon::drand::schemes::SigsOnG1Scheme;
use energon::drand::schemes::UnchainedScheme;

use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;

type Snapshot = Guard<Arc<Vec<BeaconHandler>>>;

/// Handler for sending commands to the beacon node
#[derive(Clone)]
pub struct BeaconHandler {
    pub beacon_id: BeaconID,
    /// Sender for beacon commands
    pub process_tx: Sender<BeaconCmd>,
    /// Sender for partial signature packets (hot path)
    pub partial_tx: mpsc::Sender<PartialPacket>,
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
            UnchainedScheme::ID => {
                BeaconProcess::<UnchainedScheme>::run(fs, pair, pool, private_listen)?
            }
            SigsOnG1Scheme::ID => {
                BeaconProcess::<SigsOnG1Scheme>::run(fs, pair, pool, private_listen)?
            }
            _ => return Err(FileStoreError::FailedInitID)?,
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
    pub fn new(config: Config) -> Result<(PathBuf, Self), FileStoreError> {
        let private_listen = config.private_listen.clone();

        // Connection pool for partial beacon packets is shared across beacon ids.
        let pool_span = tracing::info_span!("", partials_pool = &private_listen);
        let pool = Pool::start(pool_span);

        let (multibeacon_path, fstores) = FileStore::read_multibeacon_folder(&config.folder)?;
        let beacons: Vec<BeaconHandler> = match &config.id {
            // Load single id
            Some(id) => {
                let fs = fstores
                    .into_iter()
                    .find(|fs| fs.get_beacon_id() == Some(id))
                    .ok_or(FileStoreError::BeaconNotFound)?;
                vec![BeaconHandler::new(fs, pool.clone(), config.private_listen)?]
            }
            // Load all ids
            None => fstores
                .into_iter()
                .map(|fs| BeaconHandler::new(fs, pool.clone(), config.private_listen.clone()))
                .collect::<Result<_, _>>()?,
        };
        let multibeacon = Self {
            beacons: ArcSwap::from(Arc::new(beacons)),
            tx_pool: pool,
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
            .map_err(|_| BeaconHandlerError::SendError)?;

        Ok(())
    }

    pub async fn send_partial(&self, partial: PartialPacket) -> Result<(), BeaconHandlerError> {
        let id = partial.0.metadata.as_ref().map_or_else(
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
            .map_err(|_| BeaconHandlerError::SendError)?;

        Ok(())
    }

    pub(super) fn get_pool(&self) -> PoolSender {
        self.tx_pool.clone()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum BeaconHandlerError {
    #[error("Beacon id is not found")]
    UnknownID,
    #[error("SendError: Beacon cmd receiver has been dropped")]
    SendError,
    #[error("Beacon id is already loaded")]
    AlreadyLoaded,
    #[error("Packet metadata is missing")]
    MetadataRequired,
}
