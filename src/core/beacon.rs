use super::multibeacon::BeaconHandler;
use crate::chain::init_chain;
use crate::chain::ChainCmd;
use crate::chain::ChainError;
use crate::chain::ChainedBeacon;
use crate::chain::StoreError;
use crate::chain::StoreStreamResponse;
use crate::chain::SyncError;
use crate::chain::UnChainedBeacon;

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

use crate::net::control::SyncProgressResponse;
use crate::net::pool::PoolSender;
use crate::net::protocol::PartialMsg;
use crate::protobuf::drand::StartSyncRequest;
use crate::protobuf::drand::StatusResponse;

use crate::protobuf::dkg::DkgPacket;
use crate::protobuf::dkg::DkgStatusResponse;
use crate::protobuf::drand::ChainInfoPacket;
use crate::protobuf::drand::IdentityResponse;

use crate::net::utils::Callback;
use crate::transport::dkg::Command;
use crate::transport::dkg::GossipPacket;
use crate::transport::dkg::Participant;

use energon::drand::traits::BeaconDigest;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::task::TaskTracker;
use tracing::{error, info_span, Span};

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

    pub fn as_str(&self) -> &str {
        self.inner.as_ref()
    }

    pub fn is_eq(&self, id: &str) -> bool {
        self.inner.as_ref() == id
    }
}

pub enum BeaconCmd {
    Shutdown(Callback<(), ShutdownError>),
    IdentityRequest(Callback<IdentityResponse, PointSerDeError>),
    Sync(
        u64,
        Callback<mpsc::Receiver<StoreStreamResponse>, StoreError>,
    ),
    Follow(
        StartSyncRequest,
        Callback<mpsc::Receiver<SyncProgressResponse>, SyncError>,
    ),
    ChainInfo(Callback<ChainInfoPacket, ChainError>),
    Status(Callback<StatusResponse, StoreError>),
    DkgActions(Actions),
    FinishedDkg,
}

/// Subcommand for DKG actions [`BeaconCmd::DkgActions`]
pub enum Actions {
    Gossip(GossipPacket, Callback<(), ActionsError>),
    Command(Command, Callback<(), ActionsError>),
    Broadcast(DkgPacket, Callback<(), ActionsError>),
    Status(Callback<DkgStatusResponse, ActionsError>),
}

/// `BeaconProcess` is responsible for the main logic of the `BeaconID` instance. It reads the keys / group file, it
/// can start the DKG, read/write shares to files and can initiate/respond to tBLS signature requests.
pub struct BeaconProcess<S: Scheme> {
    // Shared access is required to execute DKG protocol.
    inner: Arc<InnerProcess<S>>,
}

pub struct InnerProcess<S: Scheme> {
    beacon_id: BeaconID,
    tracker: TaskTracker,
    fs: FileStore,
    keypair: Pair<S>,
    dkg_store: DkgStore,
    process_cmd_tx: mpsc::Sender<BeaconCmd>,
    pub chain_cmd_tx: mpsc::Sender<ChainCmd>,
    l: Span,
}

impl<S: Scheme> BeaconProcess<S> {
    fn new(
        fs: FileStore,
        pair: &PairToml,
        process_cmd_tx: mpsc::Sender<BeaconCmd>,
        pool: PoolSender,
        private_listen: String,
    ) -> Result<(Self, mpsc::Sender<PartialMsg>), FileStoreError> {
        let keypair: Pair<S> = Toml::toml_decode(pair).ok_or(FileStoreError::TomlError)?;
        let our_addr = keypair.public_identity().address.clone();
        let id = fs.get_beacon_id().ok_or(FileStoreError::FailedToReadID)?;
        let is_fresh = fs.is_fresh_run()?;
        let dkg_store = DkgStore::init::<S>(fs.beacon_path.as_path(), is_fresh, id)?;
        let log = info_span!("", id = format!("{private_listen}.{id}"));
        let t = TaskTracker::new();

        let (partial_tx, chain_cmd_tx) = if S::Beacon::is_chained() {
            init_chain::<S, ChainedBeacon>(
                is_fresh,
                fs.clone(),
                private_listen,
                pool,
                id.to_string(),
                our_addr,
                &t,
            )
        } else {
            init_chain::<S, UnChainedBeacon>(
                is_fresh,
                fs.clone(),
                private_listen,
                pool,
                id.to_string(),
                our_addr,
                &t,
            )
        };

        let process = Self {
            inner: Arc::new(InnerProcess {
                beacon_id: BeaconID::new(id),
                fs,
                keypair,
                tracker: t,
                dkg_store,
                process_cmd_tx,
                chain_cmd_tx,
                l: log,
            }),
        };

        Ok((process, partial_tx))
    }

    pub fn run(
        fs: FileStore,
        pair: &PairToml,
        pool: PoolSender,
        private_listen: String,
    ) -> Result<BeaconHandler, FileStoreError> {
        // Create cmd channel for beacon process
        let (bp_tx, mut bp_rx) = mpsc::channel::<BeaconCmd>(1);
        // Initialize beacon process.
        let (bp, partial_tx) = Self::new(fs, pair, bp_tx.clone(), pool, private_listen)?;
        let beacon_id = bp.beacon_id.clone();
        let tracker = bp.tracker().clone();

        tracker.spawn(async move {
            let mut gk = GateKeeper::new(bp.log());
            while let Some(cmd) = bp_rx.recv().await {
                match cmd {
                    BeaconCmd::Status(cb) =>bp.status(cb).await,
                    BeaconCmd::IdentityRequest(cb) => cb.reply(bp.identity().try_into()),
                    BeaconCmd::Sync(from_round, cb) => {
                        if let Err(err)=bp
                            .chain_cmd_tx
                            .send(ChainCmd::ReSync { from_round, cb })
                            .await
                        {
                            // Catch the callback and track fatal state details.
                            if let ChainCmd::ReSync {from_round, cb } = err.0 {
                                error!(parent: &bp.l,"fatal: chainstore: sync request from round {from_round} has not been processed");
                                cb.reply(Err(StoreError::Internal));
                                break
                            }
                        }
                    }
                    BeaconCmd::ChainInfo(cb) => bp.chain_info(cb).await,
                    BeaconCmd::DkgActions(action) => bp.dkg_actions(action, &mut gk).await,
                    BeaconCmd::FinishedDkg => gk.set_empty(),
                    BeaconCmd::Shutdown(cb) => {
                        cb.reply(bp.shutdown().await);
                        break;
                    }
                    BeaconCmd::Follow(req, cb) => {
                        if let Err(err)= bp
                            .chain_cmd_tx
                            .send(ChainCmd::Follow{req, cb})
                            .await
                        {
                            // Catch the callback and track fatal state details.
                            if let ChainCmd::Follow { req, cb } = err.0 {
                                error!(parent: &bp.l,"fatal: chain: follow request up_to {} has not been processed", req.up_to);
                                cb.reply(Err(SyncError::Internal));
                                break
                            }
                        }
                    }
                }
            }
        });

        Ok(BeaconHandler {
            beacon_id,
            process_tx: bp_tx,
            partial_tx,
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

    async fn status(&self, cb: Callback<StatusResponse, StoreError>) {
        if self
            .chain_cmd_tx
            .send(ChainCmd::LatestStored(cb))
            .await
            .is_err()
        {
            error!(parent: &self.l, "fatal: chain module in failed state");
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
            Ok(Some(start_execution_time)) => {
                self.setup_and_run_dkg(start_execution_time, gk).await
            }
            Ok(None) => Ok(()),
            Err(err) => Err(err),
        }
    }

    async fn shutdown(&self) -> Result<(), ShutdownError> {
        let (tx, rx) = Callback::new();

        self.chain_cmd_tx
            .send(ChainCmd::Shutdown(tx))
            .await
            .unwrap();
        rx.await.unwrap().unwrap();
        Ok(())
    }

    async fn chain_info(&self, cb: Callback<ChainInfoPacket, ChainError>) {
        self.chain_cmd_tx
            .send(ChainCmd::ChainInfo(cb))
            .await
            .unwrap();
    }

    /// Returns [`Participant`] which is dkg representation of [`Identity`].
    pub fn as_participant(&self) -> Result<Participant, ActionsError> {
        self.identity()
            .try_into()
            .map_err(|_| ActionsError::IntoParticipant)
    }

    pub async fn dkg_finished_notification(&self) {
        if self
            .process_cmd_tx
            .send(BeaconCmd::FinishedDkg)
            .await
            .is_err()
        {
            error!(parent: self.log(), "BeaconProcess receiver closed unexpectedly");
        }
    }

    pub fn tracker(&self) -> &TaskTracker {
        &self.tracker
    }

    pub fn id(&self) -> &str {
        self.beacon_id.as_str()
    }

    pub fn log(&self) -> &Span {
        &self.l
    }

    pub fn fs(&self) -> &FileStore {
        &self.fs
    }

    pub fn dkg_store(&self) -> &DkgStore {
        &self.dkg_store
    }

    pub fn private_key(&self) -> &S::Scalar {
        self.keypair.private_key()
    }

    /// Returns public identity for `BeaconID`.
    pub fn identity(&self) -> &Identity<S> {
        self.keypair.public_identity()
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

    Err(ActionsError::Todo)
}

impl<S: Scheme> std::ops::Deref for BeaconProcess<S> {
    type Target = InnerProcess<S>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<S: Scheme> Clone for BeaconProcess<S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}
