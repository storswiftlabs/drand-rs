//! This module contains logic for syncing and resyncing beacons.
//!
//! - Sync is called manually (CLI) by nodes without DKG setup to
//!   download historical beacons up to current height from chain node.
//! - Resync is triggered automatically by chain nodes once latest stored
//!   beacon is more than one round late for expected chain height.
use super::{
    info::ChainInfo,
    store::{BeaconRepr, ChainStore, StoreError},
};
use crate::{
    core::beacon::BeaconID,
    key::Scheme,
    log::Logger,
    net::{
        control::SyncProgressResponse,
        protocol::ProtocolClient,
        public::PublicClient,
        utils::{Address, Seconds},
    },
    protobuf::drand::{BeaconPacket, ChainInfoPacket, StartSyncRequest, SyncProgress},
    {debug, error, info, warn},
};
use energon::traits::Affine;
use rand::seq::SliceRandom;
use std::time::Duration;
use tokio::{
    sync::mpsc,
    task::{self, JoinHandle},
    time::Instant,
};
use tonic::Status;

/// Renew resync if no beacons received for factor*period duration.
const RESYNC_EXPIRY_FACTOR: u8 = 2;

/// Used to reduce log verbosity when doing bulk processes.
pub const LOGS_TO_SKIP: u64 = 50;

#[derive(thiserror::Error, Debug)]
pub enum SyncError {
    #[error("received invalid info packet")]
    InvalidInfoPacket,
    #[error("info packet is not compatible with existing configuration")]
    InfoPacketMismatch,
    #[error("syncing is already in progress")]
    AlreadySyncing,
    #[error("all peers should be in valid format")]
    PeersInvalidFormat,
    #[error("failed to get chain info from all peers")]
    FailedInfoFromAllPeers,
    #[error("chain hash mismatch: {0}")]
    ChainHashMismatch(String),
    #[error("internal: chain module in failed state")]
    Internal,
    #[error("chain store: {0}")]
    ChainStore(#[from] StoreError),
    #[error("invalid follow request: from {from} up_to {target}")]
    InvalidTarget { from: u64, target: u64 },
    #[error("sync channel closed unexpectedly")]
    SyncClosedTx,
    #[error("tried all peers, latest received round {last}")]
    TriedAllPers { last: u64 },
    #[error("`follow_request` allowed only for nodes without DKG setup")]
    ForbiddenToFollow,
}

/// Wrapper around `JoinHandle` for resync task, including task state.
pub struct HandleReSync {
    /// Handle for resync task.
    handle: JoinHandle<Result<(), SyncError>>,
    /// Time of latest received beacon from resync task.
    latest_received: Instant,
    /// Expiry factor for the handle.
    factor: Duration,
}

impl Drop for HandleReSync {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl HandleReSync {
    /// Registers a new resync task.
    pub fn new(period: Seconds, handle: JoinHandle<Result<(), SyncError>>) -> Self {
        Self {
            latest_received: Instant::now(),
            handle,
            factor: Duration::from_secs(
                (period.get_value() * u32::from(RESYNC_EXPIRY_FACTOR)).into(),
            ),
        }
    }

    /// Returns `true` if resync is running and making progress.
    pub fn is_running(&self) -> bool {
        if self.handle.is_finished() {
            false
        } else {
            self.latest_received.elapsed() < self.factor
        }
    }

    /// Updates handle expiry time once new beacon received.
    pub fn update_last_received_time(&mut self) {
        self.latest_received = Instant::now();
    }
}

/// Initial config for `follow` request. Used to start [`DefaultSyncer`].
pub struct DefaultSyncerConfig<B: BeaconRepr> {
    store: ChainStore<B>,
    pub packet: ChainInfoPacket,
    pub beacon_id: BeaconID,
    peers: Vec<Address>,
}

/// Default syncer used for nodes without DKG setup.
pub struct DefaultSyncer<S: Scheme, B: BeaconRepr> {
    store: ChainStore<B>,
    info: ChainInfo<S>,
    peers: Vec<Address>,
}

impl<S: Scheme, B: BeaconRepr> DefaultSyncer<S, B> {
    pub fn from_config(c: DefaultSyncerConfig<B>, log: &Logger) -> Result<Self, SyncError> {
        let DefaultSyncerConfig {
            store,
            packet,
            beacon_id,
            peers,
        } = c;

        let info = match ChainInfo::from_packet(&packet, beacon_id) {
            Ok(info) => info,
            Err(err) => {
                error!(log, "{err}");
                return Err(SyncError::InvalidInfoPacket);
            }
        };

        let syncer = Self { store, info, peers };

        Ok(syncer)
    }

    #[rustfmt::skip] // readability
    pub fn process_follow_request(
        self,
        target: u64,
        tx: mpsc::Sender<SyncProgressResponse>,
        log: Logger,
    ) -> JoinHandle<Result<(), SyncError>> {
        task::spawn(async move {
            let mut last_stored = self.store.last().await?;
            if last_stored.round() >= target {
                warn!(&log, "sync request rejected: target {target}, latest_stored {}", last_stored.round());
                return Ok(());
            }
            info!(&log, "processing sync request: target {target}, latest_stored {}", last_stored.round());

            let started_from = last_stored.round();
            if target - started_from > LOGS_TO_SKIP {
                debug!(&log, "logging will use rate limiting {LOGS_TO_SKIP}");
            }

            // Peers are randomly sorted on configuration step (see [start_follow_chain]).
            'peers: for peer in &self.peers {
                let from = last_stored.round() + 1;
                if target < from {
                    let err = SyncError::InvalidTarget { from, target };
                    error!(&log, "latest stored round {}, {err}", last_stored.round());
                    return Err(err);
                }

                let mut stream = match ProtocolClient::new(peer).await {
                    Ok(mut client) => {
                        match client.sync_chain(from, self.info.beacon_id.to_string()).await {
                            Ok(stream) => stream,
                            Err(err) => {
                                error!(&log, "skipping {peer}, failed to get stream: {err}");
                                continue;
                            }
                        }
                    }
                    Err(err) => {
                        error!(&log, "skipping {peer}, unable to create client: {err}",);
                        continue;
                    }
                };

                while let Ok(Some(p)) = stream.message().await {
                    let Some(ref meta) = p.metadata else {
                        error!(&log, "stream: skipping {peer}, no metadata for round {}", p.round);
                        continue 'peers;
                    };

                    if self.info.beacon_id.as_str() != meta.beacon_id {
                        error!(&log, "stream: skipping {peer}, invalid beacon_id {} for round {}", meta.beacon_id, p.round);
                        continue 'peers;
                    }
                    if p.round != last_stored.round() + 1 {
                        error!(&log, "stream: skipping {peer}, round expected {}, received {}", last_stored.round() + 1, p.round);
                        continue 'peers;
                    }
                    if target - p.round < LOGS_TO_SKIP || p.round % LOGS_TO_SKIP == 0 {
                        debug!(&log, "new_beacon_fetched: peer {peer}, from_round {from}, got_round {}", p.round);
                    }

                    // Verify beacon before moving data from packet.
                    let Ok(new_sig) = Affine::deserialize(&p.signature) else {
                        error!(&log, "stream: skipping peer {peer}: failed to deserialize signature for round {}", p.round);
                        continue 'peers;
                    };

                    if super::is_valid_signature::<S>(&self.info.public_key, last_stored.signature(), p.round, &new_sig) {
                        // Signature and round has been checked - beacon is valid.
                        let valid_beacon = B::from_packet(p);
                        if let Err(err) = self.store.put(valid_beacon.clone()).await {
                            error!(&log, "failed to store beacon for round {}: {err}", valid_beacon.round());
                            return Err(SyncError::ChainStore(err));
                        }
                        last_stored = valid_beacon;

                        // Report sync progress to control client side.
                        if tx.send(Ok(SyncProgress { current: last_stored.round(), target, metadata: None })).await.is_err() {
                            debug!(&log, "sync request cancelled: synced {}, latest_stored {}", last_stored.round() - started_from, last_stored.round());
                            return Ok(());
                        }
                        if last_stored.round() == target {
                            debug!(&log, "finished syncing up_to {target} round");
                            return Ok(());
                        }
                    } else {
                        error!(&log, "skipping peer {peer}: invalid beacon signature, round {}", p.round);
                        continue 'peers;
                    }
                }
            }

            if last_stored.round() != target {
                let err = SyncError::TriedAllPers {
                    last: last_stored.round(),
                };

                let _ = tx.send(Err(Status::cancelled(err.to_string()))).await;
                error!(&log, "sync request finished: {err}");
                return Err(err);
            }

            Ok(())
        })
    }
}

pub async fn start_follow_chain<B: BeaconRepr>(
    req: &StartSyncRequest,
    id: BeaconID,
    store: &ChainStore<B>,
    log: &Logger,
) -> Result<DefaultSyncerConfig<B>, SyncError> {
    info!(log, "starting follow_chain: up_to {}", req.up_to);

    let mut peers = Vec::with_capacity(req.nodes.len());
    for node in &req.nodes {
        match Address::precheck(node.as_str()) {
            Ok(peer) => peers.push(peer),
            Err(err) => {
                error!(log, "invalid peer address: {err}");
                continue;
            }
        }
    }
    if peers.is_empty() {
        return Err(SyncError::PeersInvalidFormat);
    }

    // Peers will be connected in random order.
    peers.shuffle(&mut rand::rng());

    // Packet beacon ID from metadata should match the chain config ID.
    let packet = chain_info_from_peers(&peers, id, log).await?;
    debug!(log, "received chain info from peers:\n{packet}");

    // Packet hash should match the chain hash of beacon process recorded in packet metadata.
    let hash = super::info::hash_packet(&packet, id);

    if hash
        != *req
            .metadata
            .as_ref()
            .expect("metadata is already checked")
            .chain_hash
    {
        let err_details = format!(
            "rcv({}) != bp({})",
            hex::encode(hash),
            hex::encode(&packet.group_hash)
        );
        return Err(SyncError::ChainHashMismatch(err_details));
    }
    store.check_genesis(&packet.group_hash, log).await?;
    info!(
        log,
        "follow_chain: fetched chain info, hash {}",
        hex::encode(hash)
    );

    let config = DefaultSyncerConfig {
        store: store.clone(),
        packet,
        beacon_id: id,
        peers,
    };

    Ok(config)
}

/// Resync is triggered if latest stored beacon is more than one round late for expected chain height.
pub fn resync(
    start_from: u64,
    up_to: u64,
    peers: Vec<Address>,
    id: BeaconID,
    tx_synced: mpsc::Sender<BeaconPacket>,
    log: Logger,
) -> JoinHandle<Result<(), SyncError>> {
    task::spawn(async move {
        let mut last_sent = start_from - 1;

        'peers: for peer in peers {
            if up_to <= last_sent {
                return Err(SyncError::InvalidTarget {
                    from: last_sent + 1,
                    target: up_to,
                });
            }
            let mut stream = match ProtocolClient::new(&peer).await {
                Ok(mut conn) => match conn.sync_chain(last_sent + 1, id.to_string()).await {
                    Ok(stream) => stream,
                    Err(err) => {
                        error!(&log, "failed to get stream: peer {peer} {err}");
                        continue;
                    }
                },
                Err(err) => {
                    error!(&log, "failed to create client: peer {peer} {err}");
                    continue;
                }
            };

            debug!(
                &log,
                "start resyns: peer {peer}, from_round {}, up_to {up_to}",
                last_sent + 1
            );
            while let Ok(Some(p)) = stream.message().await {
                let Some(ref p_meta) = p.metadata else {
                    error!(&log, "skipping {peer}, no metadata for round {}", p.round);
                    continue 'peers;
                };
                if id.as_str() != p_meta.beacon_id {
                    error!(
                        &log,
                        "skipping {peer}, invalid beacon_id {} for round {}",
                        p_meta.beacon_id,
                        p.round
                    );
                    continue 'peers;
                }
                if p.round != last_sent + 1 {
                    error!(
                        &log,
                        "skipping {peer}: round expected {}, received {}",
                        last_sent + 1,
                        p.round
                    );
                    continue 'peers;
                }
                if tx_synced.send(p).await.is_err() {
                    return Err(SyncError::SyncClosedTx);
                }
                last_sent += 1;

                // Stop if target is reached
                if last_sent == up_to {
                    debug!(
                        &log,
                        "resync finished successfully: peer {peer}, target {up_to}"
                    );
                    return Ok(());
                }
            }
        }
        let err = SyncError::TriedAllPers { last: last_sent };
        error!(&log, "resync: {err}");

        Err(err)
    })
}

/// Retrieves public chain information from list of peers with prechecked beacon id.
/// Used only by nodes without DKG setup.
async fn chain_info_from_peers(
    peers: &[Address],
    id: BeaconID,
    log: &Logger,
) -> Result<ChainInfoPacket, SyncError> {
    for peer in peers {
        match PublicClient::new(peer).await {
            Ok(mut client) => {
                debug!(log, "connected to {peer}, sending chain info request..",);
                match client.chain_info(id.to_string()).await {
                    Ok(packet) => {
                        if let Some(ref m) = packet.metadata {
                            if m.beacon_id == id.as_str() {
                                return Ok(packet);
                            }
                            warn!(
                                log,
                                "chain_info: skipping {peer} with invalid beacon_id {}",
                                m.beacon_id
                            );
                        } else {
                            warn!(log, "chain_info: skipping {peer} without metadata");
                        }
                    }
                    Err(err) => {
                        warn!(log, "chain_info: skipping {peer}: {err}");
                    }
                }
            }
            Err(err) => {
                warn!(log, "chain_info: unable to create client: {err}");
            }
        };
    }

    Err(SyncError::FailedInfoFromAllPeers)
}
