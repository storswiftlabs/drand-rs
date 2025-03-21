use super::chain::ChainHandler;
use super::multibeacon::BeaconHandler;

use crate::key::keys::Identity;
use crate::key::keys::Pair;
use crate::key::store::FileStore;
use crate::key::store::FileStoreError;
use crate::key::toml::PairToml;
use crate::key::toml::Toml;
use crate::key::PointSerDeError;
use crate::key::Scheme;

use crate::dkg::process::Actions;
use crate::dkg::process::DkgProcess;
use crate::store::memstore::MemStore;
use crate::store::ChainStore;
use crate::store::NewStore;

use crate::protobuf::drand::ChainInfoPacket;
use crate::protobuf::drand::IdentityResponse;

use energon::drand::traits::BeaconDigest;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_util::task::TaskTracker;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::info_span;
use tracing::Span;

pub const DEFAULT_BEACON_ID: &str = "default";

/// Callback for [`BeaconCmd`]
pub type Callback<T, E> = oneshot::Sender<Result<T, E>>;

/// There is a direct relationship between an empty string and the reserved id "default".
pub fn is_default_beacon_id(beacon_id: &str) -> bool {
    beacon_id == DEFAULT_BEACON_ID || beacon_id.is_empty()
}

#[derive(PartialEq, Eq)]
pub struct BeaconID {
    inner: Arc<str>,
}

impl BeaconID {
    pub fn new(id: impl Into<Arc<str>>) -> Self {
        Self { inner: id.into() }
    }

    pub fn is_default(&self) -> bool {
        &*self.inner == DEFAULT_BEACON_ID
    }

    pub fn as_str(&self) -> &str {
        self.inner.as_ref()
    }

    pub fn is_eq(&self, id: &str) -> bool {
        self.inner.as_ref() == id
    }
}

/// Commands for communication between server-side and [`BeaconProcess`]
pub enum BeaconCmd {
    Shutdown(Callback<(), ()>),
    IdentityRequest(Callback<IdentityResponse, PointSerDeError>),
    Sync(Callback<Arc<ChainStore>, &'static str>),
    ChainInfo(Callback<ChainInfoPacket, &'static str>),
    DkgActions(Actions),
}

/// BeaconProcess is the main logic of the BeaconID. It reads the keys / group file, it
/// can start the DKG, read/write shares to files and can initiate/respond to tBLS
/// signature requests.
pub struct BeaconProcess<S: Scheme> {
    beacon_id: BeaconID,
    tracker: TaskTracker,
    fs: FileStore,
    keypair: Pair<S>,
    span: Span,
    chain_handler: ChainHandler,
    chain_store: Arc<MemStore>,
}

impl<S: Scheme> BeaconProcess<S> {
    fn new(fs: FileStore, pair: PairToml, id: &str) -> Result<Self, FileStoreError> {
        let keypair: Pair<S> = Toml::toml_decode(&pair).ok_or(FileStoreError::TomlError)?;
        let span = info_span!(
            "",
            id = format!("{}.{id}", keypair.public_identity().address())
        );

        let chain_handler = ChainHandler::try_init(&fs, id)?;
        let chain_store = Arc::new(ChainStore::new(&fs.db_path(), S::Beacon::is_chained())?);
        let process = Self {
            beacon_id: BeaconID::new(id),
            fs,
            chain_store,
            chain_handler,
            keypair,
            span,
            tracker: TaskTracker::new(),
        };

        Ok(process)
    }

    pub fn run(fs: FileStore, pair: PairToml, id: &str) -> Result<BeaconHandler, FileStoreError> {
        let is_new_node = fs.is_fresh_run()?;
        let bp = Box::new(Self::new(fs, pair, id)?);
        let dkg = DkgProcess::init(&bp, is_new_node)?;

        let tracker = bp.tracker().to_owned();
        let (tx, mut rx) = mpsc::channel::<BeaconCmd>(1);
        if is_new_node {
            info!(parent: &bp.span, "beacon id [{id}]: will run as fresh install -> expect to run DKG.");
        }

        tracker.spawn(async move {
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    BeaconCmd::Shutdown(callback) => {
                        if let Err(err) = bp.shutdown(callback) {
                            error!("failed to shutdown gracefully, {err}")
                        };
                        break;
                    }
                    BeaconCmd::IdentityRequest(callback) => {
                        if let Err(err) = bp.identity_request(callback) {
                            error!("failed to send identity responce, {err}")
                        }
                    }
                    BeaconCmd::Sync(callback) => {
                        if callback.send(Ok(Arc::clone(&bp.chain_store))).is_err() {
                            error!("failed to proceed sync request")
                        }
                    }
                    BeaconCmd::ChainInfo(callback) => {
                        if callback.send(Ok(bp.chain_handler.chain_info())).is_err() {
                            error!("failed to send chain_info, receiver is dropped")
                        }
                    }
                    BeaconCmd::DkgActions(action) => {
                        if dkg.send(action).await.is_err() {
                            error!("{}", crate::dkg::process::ERR_SEND)
                        }
                    }
                }
            }
        });

        Ok(BeaconHandler {
            beacon_id: BeaconID::new(id),
            tracker,
            tx,
        })
    }

    fn shutdown(&self, sender: Callback<(), ()>) -> anyhow::Result<()> {
        debug!("received shutdown for {}", self.beacon_id);

        // TODO: remove 'result' once implemented:
        //  - beacon_id removed from connection pool is_ok
        //  - database closed correctly is_ok
        //  - chain handler stopped correctly is_ok
        //  - any other tasks which might be spawned by tracker.
        let result = Ok(());
        sender.send(result).map_err(|_| SendError)?;

        Ok(())
    }

    fn identity_request(
        &self,
        sender: Callback<IdentityResponse, PointSerDeError>,
    ) -> anyhow::Result<()> {
        sender
            .send(self.keypair.public_identity().try_into())
            .map_err(|_| SendError)?;

        Ok(())
    }

    pub fn tracker(&self) -> &TaskTracker {
        &self.tracker
    }

    pub fn id(&self) -> &BeaconID {
        &self.beacon_id
    }

    /// Returns public identity for BeaconID
    pub fn identity(&self) -> &Identity<S> {
        self.keypair.public_identity()
    }

    /// Returns absolute path to BeaconID folder
    pub fn path(&self) -> &Path {
        self.fs.beacon_path.as_path()
    }
}

impl Clone for BeaconID {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl std::fmt::Display for BeaconID {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

#[derive(thiserror::Error, Debug)]
/// Diagnostic error type.
#[error("callback receiver has already been dropped")]
struct SendError;
