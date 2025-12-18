// Copyright 2023-2025 StorSwift Inc.
// SPDX-License-Identifier: Apache-2.0

use super::multibeacon::BeaconHandler;
use crate::{
    chain::{
        init_chain, ChainCmd, ChainedBeacon, StoreError, StoreStreamResponse, SyncError,
        UnChainedBeacon,
    },
    dkg::{
        actions_active::ActionsActive, actions_passive::ActionsPassive, execution::ExecuteDkg,
        store::DkgStore, utils::GateKeeper, ActionsError,
    },
    key::{
        keys::{Identity, Pair},
        store::{FileStore, FileStoreError},
        toml::{PairToml, Toml},
        Scheme,
    },
    log::Logger,
    net::{control::SyncProgressResponse, pool::PoolSender, protocol::PartialMsg, utils::Callback},
    protobuf::{
        dkg::{DkgPacket, DkgStatusResponse},
        drand::{ChainInfoPacket, IdentityResponse, StartSyncRequest, StatusResponse},
    },
    transport::dkg::{Command, GossipPacket, Participant},
    {error, info},
};
use energon::drand::traits::BeaconDigest;
use std::sync::Arc;
use tokio::sync::mpsc::{self, error::SendError};
use tokio_util::task::TaskTracker;

#[derive(thiserror::Error, Debug)]
pub enum BeaconProcessError {
    /// Critical error, the process will log details and terminate.
    #[error("beacon process internal failure")]
    Internal,
    /// Some operations may fail if node is not in right state e.g. DKG setup required.
    #[error("not available")]
    NotAvailable,
}

/// This value should not be changed for backward-compatibility reasons (Drand-go).
pub const DEFAULT_BEACON_ID: &str = "default";

#[derive(Default, Clone, Copy, PartialEq, Eq, Ord, PartialOrd)]
pub struct BeaconID(&'static str);

impl BeaconID {
    /// Creates unique identifier for beacon process on *startup*.
    /// This is an intentional leak in order to provide an efficient type for runtime hot paths.
    /// Memory gets cleaned up by OS when the APP process exits.
    fn leak_once(id: String) -> Self {
        Self(id.leak())
    }

    /// There is a direct relationship between an empty string and the reserved id "default".
    pub fn is_default(other_id: &str) -> bool {
        other_id == DEFAULT_BEACON_ID || other_id.is_empty()
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    #[inline(always)]
    pub fn as_str(&self) -> &'static str {
        self.0
    }

    #[inline(always)]
    pub fn is_eq(&self, other_id: &str) -> bool {
        self.0 == other_id
    }

    #[cfg(test)]
    pub fn from_static(id: &'static str) -> Self {
        if id.is_empty() {
            panic!("BeaconID is empty")
        } else {
            Self(id)
        }
    }
}

impl std::fmt::Display for BeaconID {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

pub enum BeaconCmd {
    Shutdown(Callback<(), ShutdownError>),
    IdentityRequest(Callback<IdentityResponse, BeaconProcessError>),
    Sync(
        u64,
        Callback<mpsc::Receiver<StoreStreamResponse>, StoreError>,
    ),
    Follow(
        StartSyncRequest,
        Callback<mpsc::Receiver<SyncProgressResponse>, SyncError>,
    ),
    ChainInfo(Callback<ChainInfoPacket, BeaconProcessError>),
    Status(Callback<StatusResponse, BeaconProcessError>),
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

/// `BeaconProcess` is responsible for the main logic of the `BeaconID` instance.
/// It reads the keys / group file, it can start the DKG,
/// read/write shares to files and can initiate/respond to tBLS signature requests.
pub struct BeaconProcess<S: Scheme> {
    // Shared access required to offload DKG protocol
    // execution from beacon manager task.
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
    log: Logger,
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
        let id = fs
            .get_beacon_id()
            .ok_or(FileStoreError::FailedToReadID)?
            .to_string();
        let id = BeaconID::leak_once(id);

        // Fresh run means no DKG setup yet.
        let is_fresh = fs.is_fresh_run()?;
        let dkg_store = DkgStore::init::<S>(fs.beacon_path.as_path(), is_fresh, id.as_str())?;
        let log = crate::log::set_id(&private_listen, id.as_str());
        let t = TaskTracker::new();

        let (partial_tx, chain_cmd_tx) = if S::Beacon::is_chained() {
            init_chain::<S, ChainedBeacon>(
                is_fresh,
                fs.clone(),
                private_listen,
                pool,
                id,
                our_addr,
                log.clone(),
                &t,
            )
        } else {
            init_chain::<S, UnChainedBeacon>(
                is_fresh,
                fs.clone(),
                private_listen,
                pool,
                id,
                our_addr,
                log.clone(),
                &t,
            )
        };

        let process = Self {
            inner: Arc::new(InnerProcess {
                beacon_id: id,
                fs,
                keypair,
                tracker: t,
                dkg_store,
                process_cmd_tx,
                chain_cmd_tx,
                log,
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
        // BeaconHandler -> BeaconProcess.
        let (bp_tx, mut bp_rx) = mpsc::channel::<BeaconCmd>(1);
        let (bp, partial_tx) = Self::new(fs, pair, bp_tx.clone(), pool, private_listen)?;
        let bp_id = bp.beacon_id;
        let tracker = bp.tracker().clone();

        tracker.spawn(async move {
            let mut gk = GateKeeper::new(bp.log.clone());
            while let Some(cmd) = bp_rx.recv().await {
                match cmd {
                    BeaconCmd::Status(cb) => {
                        if let Err(SendError(ChainCmd::LatestStored(cb))) =
                            bp.chain_cmd_tx.send(ChainCmd::LatestStored(cb)).await
                        {
                            error!(&bp.log, "latest_stored request failed");
                            cb.reply(Err(BeaconProcessError::Internal));
                            break;
                        }
                    }
                    BeaconCmd::IdentityRequest(cb) => match bp.identity().try_into() {
                        Ok(identity) => cb.reply(Ok(identity)),
                        Err(err) => {
                            error!(&bp.log, "identity request failed: {err}");
                            cb.reply(Err(BeaconProcessError::Internal));
                            break;
                        }
                    },
                    BeaconCmd::Sync(from_round, cb) => {
                        if let Err(SendError(ChainCmd::ReSync { from_round, cb })) = bp
                            .chain_cmd_tx
                            .send(ChainCmd::ReSync { from_round, cb })
                            .await
                        {
                            error!(&bp.log, "sync request from round {from_round} failed");
                            cb.reply(Err(StoreError::Internal));
                            break;
                        }
                    }
                    BeaconCmd::ChainInfo(cb) => {
                        if let Err(SendError(ChainCmd::ChainInfo(cb))) =
                            bp.chain_cmd_tx.send(ChainCmd::ChainInfo(cb)).await
                        {
                            error!(&bp.log, "chain_info request failed");
                            cb.reply(Err(BeaconProcessError::Internal));
                            break;
                        }
                    }
                    BeaconCmd::DkgActions(action) => bp.dkg_actions(action, &mut gk).await,
                    BeaconCmd::FinishedDkg => {
                        gk.set_empty();
                        info!(bp.log(), "DKG gate is closed, protocol terminated normally");
                    }
                    BeaconCmd::Shutdown(cb) => {
                        cb.reply(bp.shutdown().await);
                        break;
                    }
                    BeaconCmd::Follow(req, cb) => {
                        if let Err(SendError(ChainCmd::Follow { req, cb })) =
                            bp.chain_cmd_tx.send(ChainCmd::Follow { req, cb }).await
                        {
                            error!(
                                &bp.log,
                                "follow request up_to {} has not been processed", req.up_to
                            );
                            cb.reply(Err(SyncError::Internal));
                            break;
                        }
                    }
                }
            }
            info!(bp.log(), "shutting down");
        });

        Ok(BeaconHandler {
            beacon_id: bp_id,
            process_tx: bp_tx,
            partial_tx,
        })
    }

    async fn dkg_actions(&self, request: Actions, gk: &mut GateKeeper<S>) {
        match request {
            Actions::Status(cb) => cb.reply(self.dkg_status().inspect_err(|err| {
                error!(self.log(), "dkg_status: {err}");
            })),
            Actions::Command(cmd, cb) => cb.reply(self.command(cmd).await.inspect_err(|err| {
                error!(self.log(), "dkg_command: {err}");
            })),
            Actions::Broadcast(packet, cb) => {
                cb.reply(gk.broadcast(packet).await.inspect_err(|err| {
                    error!(self.log(), "dkg_broadcast: {err}");
                }));
            }
            Actions::Gossip(packet, cb) => {
                cb.reply(self.gossip(gk, packet).await.inspect_err(|err| {
                    error!(self.log(), "dkg_gossip: {err}");
                }));
            }
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

    /// Returns [`Participant`] which is dkg representation of [`Identity`].
    pub fn as_participant(&self) -> Result<Participant, ActionsError> {
        self.identity()
            .try_into()
            .map_err(|_| ActionsError::IntoParticipant)
    }

    pub async fn dkg_finished_notification(&self) -> Result<(), SendError<BeaconCmd>> {
        self.process_cmd_tx.send(BeaconCmd::FinishedDkg).await
    }

    pub fn tracker(&self) -> &TaskTracker {
        &self.tracker
    }

    pub fn id(&self) -> &str {
        self.beacon_id.as_str()
    }

    #[inline(always)]
    pub fn log(&self) -> &Logger {
        &self.log
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

#[derive(thiserror::Error, Debug)]
#[error("failed to shutdown gracefully")]
pub struct ShutdownError;

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
