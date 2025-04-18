use super::chain::ChainHandler;
use super::multibeacon::BeaconHandler;

use crate::dkg::actions_active::ActionsActive;
use crate::dkg::actions_passive::ActionsPassive;
use crate::dkg::execution::ExecuteDkg;
use crate::dkg::store::DkgStore;
use crate::dkg::utils::GateKeeper;
use crate::dkg::ActionsError;

use crate::key::keys::Identity;
use crate::key::keys::Pair;
use crate::key::store::FileStore;
use crate::key::store::FileStoreError;
use crate::key::toml::PairToml;
use crate::key::toml::Toml;
use crate::key::PointSerDeError;
use crate::key::Scheme;

use crate::store::memstore::MemStore;
use crate::store::ChainStore;
use crate::store::NewStore;

use crate::protobuf::dkg::DkgPacket;
use crate::protobuf::dkg::DkgStatusResponse;
use crate::protobuf::drand::ChainInfoPacket;
use crate::protobuf::drand::IdentityResponse;

use crate::net::utils::Callback;
use crate::transport::dkg::Command;
use crate::transport::dkg::GossipPacket;
use crate::transport::dkg::Participant;

use energon::drand::traits::BeaconDigest;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::task::TaskTracker;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::info_span;
use tracing::Span;

pub const DEFAULT_BEACON_ID: &str = "default";

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

/// Commands for communication with [`BeaconProcess`]
pub enum BeaconCmd {
    Shutdown(Callback<(), ShutdownError>),
    IdentityRequest(Callback<IdentityResponse, PointSerDeError>),
    Sync(Callback<Arc<ChainStore>, SyncError>),
    ChainInfo(Callback<ChainInfoPacket, ChainInfoError>),
    DkgActions(Actions),
    CompletedDkg,
}

/// Subcommand for DKG actions [`BeaconCmd::DkgActions`]
pub enum Actions {
    Gossip(GossipPacket, Callback<(), ActionsError>),
    Command(Command, Callback<(), ActionsError>),
    Broadcast(DkgPacket, Callback<(), ActionsError>),
    Status(Callback<DkgStatusResponse, ActionsError>),
}

/// BeaconProcess is the main logic of the BeaconID instance. It reads the keys / group file, it
/// can start the DKG, read/write shares to files and can initiate/respond to tBLS
/// signature requests.
pub struct BeaconProcess<S: Scheme> {
    beacon_id: BeaconID,
    tracker: TaskTracker,
    fs: FileStore,
    keypair: Pair<S>,
    chain_handler: ChainHandler,
    chain_store: Arc<MemStore>,
    dkg_store: DkgStore,
    cmd_sender: mpsc::Sender<BeaconCmd>,
    log: Span,
}

impl<S: Scheme> BeaconProcess<S> {
    fn new(
        fs: FileStore,
        pair: PairToml,
        id: &str,
        tx: mpsc::Sender<BeaconCmd>,
    ) -> Result<Self, FileStoreError> {
        let keypair: Pair<S> = Toml::toml_decode(&pair).ok_or(FileStoreError::TomlError)?;

        let chain_handler = ChainHandler::try_init(&fs, id)?;
        let chain_store = Arc::new(ChainStore::new(&fs.db_path(), S::Beacon::is_chained())?);
        let is_fresh = fs.is_fresh_run()?;
        let dkg_store = DkgStore::init::<S>(fs.beacon_path.as_path(), is_fresh, id)?;
        let log = info_span!(
            "",
            id = format!("{}.{id}", keypair.public_identity().address())
        );
        if is_fresh {
            info!(parent: &log, "beacon id [{id}]: will run as fresh install -> expect to run DKG.");
        }

        let process = Self {
            beacon_id: BeaconID::new(id),
            fs,
            chain_store,
            chain_handler,
            keypair,
            tracker: TaskTracker::new(),
            dkg_store,
            cmd_sender: tx,
            log,
        };

        Ok(process)
    }

    pub fn run(fs: FileStore, pair: PairToml, id: &str) -> Result<BeaconHandler, FileStoreError> {
        let (tx, mut rx) = mpsc::channel::<BeaconCmd>(1);
        let bp = Self::new(fs, pair, id, tx.clone())?;
        let tracker = bp.tracker().clone();

        tracker.spawn(async move {
            let mut gk = GateKeeper::new(bp.log());

            while let Some(cmd) = rx.recv().await {
                match cmd {
                    BeaconCmd::IdentityRequest(cb) => cb.reply(bp.identity().try_into()),
                    BeaconCmd::Sync(cb) => cb.reply(bp.sync()),
                    BeaconCmd::ChainInfo(cb) => cb.reply(bp.chain_info()),
                    BeaconCmd::DkgActions(action) => bp.dkg_actions(action, &mut gk).await,
                    BeaconCmd::CompletedDkg => bp.completed_dkg(&mut gk).await,
                    BeaconCmd::Shutdown(cb) => {
                        cb.reply(bp.shutdown());
                        break;
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

    async fn dkg_actions(&self, request: Actions, gk: &mut GateKeeper<S>) {
        match request {
            Actions::Status(cb) => cb.reply(self.dkg_status()),
            Actions::Command(cmd, cb) => cb.reply(self.command(cmd).await),
            Actions::Broadcast(packet, cb) => cb.reply(gk.broadcast(packet).await),
            Actions::Gossip(packet, cb) => cb.reply(self.gossip(gk, packet).await),
        }
    }

    async fn gossip(
        &self,
        gk: &mut GateKeeper<S>,
        packet: GossipPacket,
    ) -> Result<(), ActionsError> {
        // ignore duplicated or incorrect packets
        if !gk.is_new_packet(&packet) {
            return Ok(());
        }

        match self.packet(packet).await {
            Ok(Some(start_execution_time)) => self.setup_dkg(start_execution_time, gk).await,
            Ok(None) => Ok(()),
            Err(err) => Err(err),
        }
    }

    async fn completed_dkg(&self, gk: &mut GateKeeper<S>) {
        gk.set_empty();

        /* Handle completed dkg.
           ..
        */
    }

    fn shutdown(&self) -> Result<(), ShutdownError> {
        debug!(parent: self.log(), "received shutdown");

        // TODO:
        //  - beacon_id removed from connection pool is_ok
        //  - database closed correctly is_ok
        //  - chain handler stopped correctly is_ok
        //  - any other tasks which might be spawned by tracker.

        Ok(())
    }

    fn sync(&self) -> Result<Arc<MemStore>, SyncError> {
        // TODO: this module is not unfinished
        Ok(Arc::clone(&self.chain_store))
    }

    fn chain_info(&self) -> Result<ChainInfoPacket, ChainInfoError> {
        // TODO: this module is unfinished
        Ok(self.chain_handler.chain_info())
    }

    /// Returns [`Participant`] which is dkg representation of [`Identity`].
    pub fn as_participant(&self) -> Result<Participant, ActionsError> {
        self.identity()
            .try_into()
            .map_err(|_| ActionsError::IntoParticipant)
    }

    /// A signal from protocol task that DKG finished succesfully
    pub async fn dkg_is_completed(&self) {
        if self.cmd_sender.send(BeaconCmd::CompletedDkg).await.is_err() {
            error!(parent: self.log(), "BeaconProcess receiver closed unexpectedly")
        }
    }

    pub fn tracker(&self) -> &TaskTracker {
        &self.tracker
    }

    pub fn id(&self) -> &str {
        self.beacon_id.as_str()
    }

    pub fn log(&self) -> &Span {
        &self.log
    }

    pub fn dkg_store(&self) -> &DkgStore {
        &self.dkg_store
    }

    pub fn private_key(&self) -> &S::Scalar {
        self.keypair.private_key()
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
#[error("failed to shutdown gracefully")]
pub struct ShutdownError;

/// Temporary tracker for unfinished logic
pub fn todo_request<D: std::fmt::Display + ?Sized>(kind: &D) -> Result<(), ActionsError> {
    tracing::warn!("received TODO request: {kind}");

    Err(ActionsError::TODO)
}

/// Error type for unfinished sync module
#[derive(thiserror::Error, Debug)]
#[error("sync error")]
pub struct SyncError;

/// Error type for unfinished chain info module
#[derive(thiserror::Error, Debug)]
#[error("chain info error")]
pub struct ChainInfoError;
