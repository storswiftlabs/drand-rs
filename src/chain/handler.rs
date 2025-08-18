use super::cache::get_partial_index;
use super::cache::CACHE_LIMIT_ROUNDS;
use super::epoch::EpochConfig;
use super::epoch::EpochNode;
use super::info::ChainInfo;
use super::registry::Registry;
use super::store::BeaconRepr;
use super::store::ChainStore;
use super::store::StoreError;
use super::store::StoreStreamResponse;
use super::sync::start_follow_chain;
use super::sync::DefaultSyncer;
use super::sync::SyncError;
use super::sync::LOGS_TO_SKIP;
use super::ticker;
use super::time;

use crate::key::group::Group;
use crate::key::keys::DistPublic;
use crate::key::store::FileStore;
use crate::key::store::FileStoreError;
use crate::key::Scheme;

use crate::net::pool::PoolSender;
use crate::net::utils::Address;
use crate::net::utils::Callback;
use crate::net::utils::Seconds;

use crate::protobuf::drand::BeaconPacket;
use crate::protobuf::drand::ChainInfoPacket;
use crate::protobuf::drand::PartialBeaconPacket;
use crate::protobuf::drand::StartSyncRequest;
use crate::protobuf::drand::StatusResponse;
use crate::protobuf::drand::SyncProgress;

use energon::drand::traits::BeaconDigest;
use energon::kyber::tbls::recover_unchecked;
use energon::kyber::tbls::TBlsError;
use energon::points::SigPoint;
use energon::traits::Affine;

use rand::seq::SliceRandom;
use std::fmt::Debug;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio::time::Instant;
use tokio_util::task::TaskTracker;
use tonic::Status;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;
use tracing::Span;

const SHORT_SIG_BYTES: usize = 3;

/// Partial packet is a turple of the packet and callback.
pub type PartialPacket = (PartialBeaconPacket, Callback<(), ChainError>);

#[derive(thiserror::Error, Debug)]
pub enum ChainError {
    #[error("sender for partial signatures has been closed unexpectedly")]
    PartialClosedTx,
    #[error("sender for chain cmd has been closed unexpectedly")]
    CmdClosedTx,
    #[error("receiver for chain cmd has been closed unexpectedly")]
    CmdClosedRx,
    #[error("sender for round ticker has been closed unexpectedly")]
    TickerClosedTx,
    #[error("pool receiver has been closed unexpectedly")]
    PoolClosedRx,
    #[error("invalid lengh of partial: expected {expected} received {received}")]
    InvalidShareLenght { expected: usize, received: usize },
    #[error("attempt to process beacon from node of index {0}, but it was not in the group file")]
    UnknownIndex(u32),
    #[error("received partial with invalid signature")]
    InvalidPartialSignature,
    #[error("internal: failed to proceed chain_info request")]
    FailedToGetInfo,
    #[error("invalid round: {invalid}, instead of {current}")]
    InvalidRound { invalid: u64, current: u64 },
    #[error("failed to serialize recovered signature")]
    SerializeRecovered,
    #[error("recovered signature is invalid")]
    InvalidRecovered,
    #[error("chain store: {0}")]
    ChainStoreError(#[from] StoreError),
    #[error("t_bls: {0}")]
    TBlsError(#[from] TBlsError),
    #[error("fs: {0}")]
    FileStoreError(#[from] FileStoreError),
    #[error("no dkg group setup yet")]
    DkgSetupRequired,
}

/// Handler to initiate and react to the tBLS protocol.
struct ChainHandler<S: Scheme, B: BeaconRepr> {
    /// Public information of chain.
    chain_info: ChainInfo<S>,
    /// Minimum period allowed between and subsequent partial generation.
    catchup_period: Duration,
    /// Actor handle for beacon persistent database.
    store: ChainStore<B>,
    /// Actor handle for partial packets connection pool.
    pool: PoolSender,
    /// Used for loading distributed materials after each DKG.
    fs: FileStore,
    /// Epoch config is representation of DKG output.
    ec: EpochConfig<S>,
    /// Binding address of the private API used as base for every new span.
    /// *Single* span metadata follows Drand Golang notation:
    /// `{private_listen}.{beacon_id}.{dkg_index}`.
    private_listen: String,
    our_addres: Address,
    l: Span,
}

pub enum ChainCmd {
    Shutdown(Callback<(), ChainError>),
    /// Chain is notified once DKG output is received, triggering preparation
    /// for transition into the next epoch (see: [`wait_last_round`]).
    NewEpoch {
        first_round: u64,
    },
    /// Partial reload of the chain module during transition to update [`EpochConfig`] and logger.
    Reload,
    /// Request for chain public information.
    ChainInfo(Callback<ChainInfoPacket, ChainError>),
    /// Resync request from chain node.
    ReSync {
        from_round: u64,
        cb: Callback<mpsc::Receiver<StoreStreamResponse>, StoreError>,
    },
    /// Follow (sync) request from node without DKG setup.
    Follow {
        req: StartSyncRequest,
        cb: Callback<mpsc::Receiver<Result<SyncProgress, Status>>, SyncError>,
    },
    /// Status request for latest stored round.
    LatestStored(Callback<StatusResponse, StoreError>),
}

/// Holder to simplify channels management, see [`init_chain`] for detailed channels description.
struct Channels {
    rx_partial: mpsc::Receiver<PartialPacket>,
    tx_cmd: mpsc::Sender<ChainCmd>,
    rx_cmd: mpsc::Receiver<ChainCmd>,
    tx_resync: mpsc::Sender<BeaconPacket>,
    rx_resync: mpsc::Receiver<BeaconPacket>,
    tx_catchup: mpsc::Sender<()>,
    rx_catchup: mpsc::Receiver<()>,
}

/// Permanent chain configuration, used during transitions (see [`run_chain`] and [`run_chain_default`]).  
pub struct ChainConfig<B: BeaconRepr> {
    chan: Channels,
    pool: PoolSender,
    fs: FileStore,
    store: ChainStore<B>,
    private_listen: String,
    beacon_id: String,
    our_addres: Address,
}

impl<S: Scheme, B: BeaconRepr> ChainHandler<S, B> {
    /// This function updates the epoch configuration and span for the chain handler
    /// after each DKG for mutex-free implementation.
    ///
    /// Returns top-level components of chain module with their channels:
    /// - Immutable chain configuration: [`ChainHandler`].
    /// - Mutable operational state: [`Registry`].
    pub async fn from_config(
        c: ChainConfig<B>,
    ) -> Result<(Self, Registry<S, B>, Channels), FileStoreError> {
        let ChainConfig {
            chan: channels,
            pool,
            fs,
            store,
            private_listen,
            beacon_id,
            our_addres,
        } = c;

        // Load group and share from filestore.
        let group = fs.load_group::<S>()?;

        // This is only possible if the groupfile has been changed manually.
        if group.beacon_id != beacon_id {
            error!(
                "load_group: ID [{}] != chain handler ID [{beacon_id}]",
                group.beacon_id
            );
            return Err(FileStoreError::InvalidData);
        }

        // Map group and share into epoch config.
        let Group {
            threshold: _,
            period,
            catchup_period,
            genesis_time,
            transition_time,
            genesis_seed,
            beacon_id,
            nodes,
            dist_key: DistPublic { mut commits },
        } = group;

        let share = fs.load_share::<S>()?;
        let public_key = commits.swap_remove(0);
        drop(commits);
        let ec = EpochConfig::new(nodes, share);

        // Create spans for handler and partial cache.
        let span_meta = format!("{private_listen}.{}.{}", beacon_id, ec.our_index());
        let l_handler = tracing::info_span!("", chain = span_meta);
        let l_partial = tracing::info_span!("", cache = span_meta);

        // Check corner case for transition.
        check_transition(period, transition_time, &l_handler).await;

        // Genesis beacon should always match the group.genesis_seed.
        store
            .check_genesis(&genesis_seed, &l_handler)
            .await
            .map_err(|_| FileStoreError::InvalidData)?;

        let chain_info = ChainInfo {
            public_key,
            beacon_id,
            period,
            genesis_time,
            genesis_seed,
        };
        let catchup_period = Duration::from_secs(catchup_period.get_value().into());

        let chain_handler = Self {
            chain_info,
            catchup_period,
            store,
            pool,
            fs,
            ec,
            private_listen,
            our_addres,
            l: l_handler,
        };

        let latest_stored = chain_handler.store.last().await?;

        let registry = Registry::new(
            &chain_handler.chain_info,
            latest_stored,
            channels.tx_catchup.clone(),
            channels.tx_resync.clone(),
            chain_handler.ec.thr(),
            l_partial,
        );

        Ok((chain_handler, registry, channels))
    }

    /// Adds remote nodes to connection pool for given beacon ID.
    ///
    /// If some nodes are already in pool (registered for other ID),
    /// their connections will be reused.
    async fn register_in_pool(&self) -> Result<(), ChainError> {
        let peers = self
            .ec
            .nodes()
            .iter()
            .map(|n| n.peer().to_owned())
            .collect();

        self.pool
            .add_id(self.chain_info.beacon_id.clone(), peers)
            .await
            .map_err(|_| ChainError::PoolClosedRx)
    }

    /// Signs own partial, returns packet to broadcast.
    async fn sign_partial(
        &self,
        reg: &mut Registry<S, B>,
    ) -> Result<PartialBeaconPacket, ChainError> {
        reg.align_cache(&self.ec, &self.l);
        let c_round = reg.current_round();
        let ls_round = reg.latest_stored().round();

        let (previous_signature, round) = if c_round == ls_round {
            debug!(parent: &self.l, "re-broadcasting already stored beacon, round {c_round}");
            let prev_sig = match reg.latest_stored().prev_signature() {
                Some(s) => s.to_vec(),
                None => {
                    if S::Beacon::is_chained() {
                        error!(parent: &self.l, "prev_sig is empty for chained beacon, round {c_round}, scheme {}", S::ID);
                        panic!()
                    } else {
                        // Valid case for unchained schemes.
                        vec![]
                    }
                }
            };
            (prev_sig, c_round)
        } else {
            (reg.latest_stored().signature().to_vec(), ls_round + 1)
        };

        let msg = S::Beacon::digest(&previous_signature, round);
        let sigshare = match self.ec.sign_partial(&msg) {
            Ok(s) => s,
            Err(err) => {
                error!(parent: &self.l, "sign partial: round: {round}, error: {err}");
                return Err(ChainError::TBlsError(err));
            }
        };

        let partial_sig = match sigshare.serialize() {
            Ok(s) => s,
            Err(err) => {
                error!(parent: &self.l, "serialize sigshare: round {round}, error: {err}");
                return Err(ChainError::TBlsError(err));
            }
        };

        // Add own share into cache if the share is for round we want to aggregate.
        if round == ls_round + 1
            && round <= c_round
            && !reg.cache().is_share_present(self.ec.our_index())
        {
            if let Some(thr_sigs) = reg.cache_mut().add_prechecked(sigshare, &self.our_addres) {
                let Ok(recovered) = recover_unchecked(thr_sigs) else {
                    error!(parent: &self.l, "recover_unchecked: scalar is non-invertable");
                    panic!()
                };
                if let Err(err) = self.save_recovered(round, &recovered, reg).await {
                    error!(parent: &self.l, "failed to save recovered beacon for round {round}: {err}");
                    panic!()
                };
            }
        }

        let short_prev_sig = previous_signature
            .get(..SHORT_SIG_BYTES)
            .unwrap_or_default();
        let short_msg = msg.get(..SHORT_SIG_BYTES).expect("always 32 bytes");
        debug!(parent: &self.l,"{{\"broadcast_partial\": {round}, \"prev_sig\": \"{}\", \"msg_sign\": \"{}\"}}", hex::encode(short_prev_sig), hex::encode(short_msg));
        let packet = PartialBeaconPacket {
            round,
            previous_signature,
            partial_sig,
            metadata: crate::protobuf::drand::Metadata::golang_node_version(
                self.chain_info.beacon_id.clone(),
                None,
            )
            .into(),
        };

        Ok(packet)
    }

    /// Sends partial to the connection pool to broadcast for nodes subscribed for given beacon ID.
    async fn broadcast(&self, packet: PartialBeaconPacket) -> Result<(), ChainError> {
        let round = packet.round;
        if self.pool.broadcast_partial(packet).await.is_err() {
            error!(parent: &self.l, "failed to broadcast partial, round {round}, error: {}", ChainError::PoolClosedRx);
            return Err(ChainError::PoolClosedRx);
        }

        Ok(())
    }

    async fn process_partial(
        &self,
        reg: &mut Registry<S, B>,
        partial: PartialBeaconPacket,
    ) -> Result<(), ChainError> {
        let p_round = partial.round;
        let c_round = reg.current_round();
        let ls_round = reg.latest_stored().round();

        if p_round <= ls_round {
            debug!(parent: &self.l, "ignoring partial for round: {p_round}, current {c_round}, latest_stored {ls_round}");
            return Ok(());
        }
        // Allowed one round off in the future because of small clock drifts possible.
        if p_round > c_round + 1 {
            error!(parent: &self.l, "ignoring future partial for round {p_round}, current {c_round}");
            return Err(ChainError::InvalidRound {
                invalid: p_round,
                current: c_round,
            });
        }

        // Cache updates once per stored beacon, between updates this call is cheap.
        reg.align_cache(&self.ec, &self.l);

        // Add packet to cache if p_round hits the allowed range and signature is not duplicated.
        if p_round > ls_round + 1 && p_round <= ls_round + 1 + CACHE_LIMIT_ROUNDS {
            let Some(is_added) = reg.cache_mut().add_packet(partial) else {
                // Logical error.
                error!(parent: &self.l, "cache_height {}: attempt to add non-allowed round {p_round}, current {c_round}, please report this.", reg.cache().height());
                return Err(ChainError::InvalidRound {
                    invalid: p_round,
                    current: c_round,
                });
            };
            if is_added {
                debug!(parent: &self.l, "cache: added partial for round {p_round}, latest stored {ls_round}, current {c_round}");
            } else {
                debug!(parent: &self.l, "cache: ignoring already added partial for round {p_round}");
            }
            return Ok(());

        // Process packet as sigshare.
        } else if p_round == ls_round + 1 {
            let Some(idx) = get_partial_index::<S>(&partial.partial_sig) else {
                return Err(ChainError::InvalidShareLenght {
                    expected: <S::Sig as energon::traits::Group>::POINT_SIZE + 2,
                    received: partial.partial_sig.len(),
                });
            };

            // Ignore already present sigshare.
            if reg.cache().is_share_present(idx) {
                debug!(parent: &self.l, "ignoring already cached sigshare for round {p_round}");
                return Ok(());
            }
            let (valid_sigshare, node_addr) = self.ec.verify_partial(&partial)?;

            // Recover and save beacon.
            // Note: Sigshares are prechecked and sorted by their index.
            if let Some(thr_sigs) = reg.cache_mut().add_prechecked(valid_sigshare, node_addr) {
                let Ok(recovered) = recover_unchecked(thr_sigs) else {
                    error!(parent: &self.l, "fatal: recover_unchecked: scalar is non-invertable");
                    panic!()
                };
                if let Err(err) = self.save_recovered(p_round, &recovered, reg).await {
                    error!(parent: &self.l, "fatal: failed to save recovered beacon for round {p_round}: {err}");
                    panic!()
                };
            }
        }

        Ok(())
    }

    async fn save_recovered(
        &self,
        r_round: u64,
        r_sig: &SigPoint<S>,
        reg: &mut Registry<S, B>,
    ) -> Result<(), ChainError> {
        let ls_round = reg.latest_stored().round();

        if ls_round + 1 == r_round {
            let valid_beacon = if super::is_valid_signature::<S>(
                &self.chain_info.public_key,
                reg.latest_stored().signature(),
                r_round,
                r_sig,
            ) {
                let Ok(r_sig) = Affine::serialize(r_sig) else {
                    error!(parent: &self.l, "round {r_round}: error: {}", ChainError::SerializeRecovered);
                    return Err(ChainError::SerializeRecovered);
                };
                B::new(reg.latest_stored(), r_sig.into())
            } else {
                error!(parent: &self.l, "round {r_round}: error: {}", ChainError::InvalidRecovered);
                return Err(ChainError::InvalidRecovered);
            };
            let discrepancy = time::round_discrepancy_ms(
                self.chain_info.period,
                self.chain_info.genesis_time,
                r_round,
            );

            // Store beacon, measure storage time.
            let start = Instant::now();
            self.store.put(valid_beacon.clone()).await?;
            let storage_time = start.elapsed().as_millis();
            info!(parent: &self.l,"{{\"NEW_BEACON_STORED\": \"{{ round: {r_round}, sig: {}, prevSig: {:?} }}\", \"time_discrepancy_ms\": {discrepancy}, \"storage_time_ms\": {storage_time}", valid_beacon.short_sig(), valid_beacon.short_prev_sig().unwrap_or_default());
            reg.update_latest_stored(valid_beacon);
            reg.align_cache(&self.ec, &self.l);

            // Check if catchup required.
            let ls_round = reg.latest_stored().round();
            let c_round = reg.current_round();
            let catchup_launch = c_round > ls_round;
            debug!(parent: &self.l, "{{\"beacon_loop\": \"catchupmode\", \"last_is\" {ls_round}, \"current\": {c_round}, \"catchup_launch\": {catchup_launch}}}");
            if catchup_launch {
                reg.start_catchup(self.catchup_period);
            }
        } else {
            // Beacon for this round has already been received via resync.
            // Stop resync as no longer relevant:
            // - We are at same height with other nodes (at least with thr nodes).
            // - If chain is late - we are on catchup mode (still no reason to resync unless network in ~strange state).
            // - Resync may be triggered again on next round tick.
            warn!(parent: &self.l, "ignoring recovered beacon for round {r_round}, current {}, latest_stored {ls_round}", reg.current_round());
            reg.stop_resync();
        };

        Ok(())
    }

    async fn save_resynced(
        &self,
        p: BeaconPacket,
        reg: &mut Registry<S, B>,
    ) -> Result<(), ChainError> {
        let l = &self.l;
        // Check if we still need beacon for this round (it might have already recovered on cathup mode).
        if p.round == reg.latest_stored().round() + 1 {
            let Ok(p_signature) = Affine::deserialize(&p.signature) else {
                error!(parent: l, "save_resynced: failed to deserialize signature for round {}, aborting resync task..", p.round);
                reg.stop_resync();
                return Ok(());
            };

            if super::is_valid_signature::<S>(
                &self.chain_info.public_key,
                reg.latest_stored().signature(),
                p.round,
                &p_signature,
            ) {
                let valid_beacon = B::new(reg.latest_stored(), p.signature);
                // Skip logs if round is too far from current round.
                if reg.current_round() - p.round < LOGS_TO_SKIP || p.round % 300 == 0 {
                    let discrepancy = time::round_discrepancy_ms(
                        self.chain_info.period,
                        self.chain_info.genesis_time,
                        p.round,
                    );
                    // Store beacon; measure storage time.
                    let start = Instant::now();
                    self.store.put(valid_beacon.clone()).await?;
                    let storage_time = start.elapsed().as_millis();
                    info!(parent: l,"NEW_BEACON_STORED: round {}, time_discrepancy_ms: {discrepancy}, storage_time_ms: {storage_time}", p.round);
                } else {
                    self.store.put(valid_beacon.clone()).await?;
                }
                reg.update_latest_stored(valid_beacon);
                reg.extend_resync_expiry_time();
            } else {
                error!(parent: l, "save_resynced: invalid signature for round {}, aborting resync task..", p.round);
                reg.stop_resync();
            }
        } else {
            debug!(parent: l, "save_resynced: ignoring beacon for round {}, latest_stored {}, aborting sync task..", p.round, reg.latest_stored().round());
            reg.stop_resync();
        }

        Ok(())
    }

    /// Trigger for catchup and resync, starting them if needed and not already running.
    pub fn check_resync_catchup(&self, reg: &mut Registry<S, B>) {
        let c_round = reg.current_round();
        let ls_round = reg.latest_stored().round();

        if c_round - ls_round > 1 {
            reg.start_catchup(self.catchup_period);
            if !reg.is_resync_active() {
                let tx_resync = reg.get_tx_resync();
                let mut peers: Vec<Address> = self
                    .ec
                    .nodes()
                    .iter()
                    .map(EpochNode::peer)
                    .cloned()
                    .collect();
                peers.shuffle(&mut rand::rng());

                let id = self.chain_info.beacon_id.clone();
                let start_from = ls_round + 1;
                let up_to = c_round;
                let l = tracing::info_span!(
                    "",
                    resync = format!(
                        "{}.{id}.{} from {start_from} to {up_to}",
                        self.private_listen,
                        self.ec.our_index()
                    )
                );

                let handle = super::sync::resync(start_from, up_to, peers, id, tx_resync, l);
                reg.new_resync_handle(self.chain_info.period, handle);
            }
        }
    }
}

/// Default chain used for fresh nodes without DKG setup.
async fn run_chain_default<S: Scheme, B: BeaconRepr>(
    mut cc: ChainConfig<B>,
) -> Result<Option<ChainConfig<B>>, ChainError> {
    let l = tracing::info_span!(
        "",
        chain = format!("{}.{}", cc.private_listen, cc.beacon_id)
    );
    info!(parent: &l, "will run as fresh install -> expect to run DKG.");
    let mut chain_info = ChainInfo::<S>::default();

    // Handle for sync task.
    let mut sync_handle: Option<JoinHandle<Result<(), SyncError>>> = None;

    loop {
        tokio::select! {
            new_partial = cc.chan.rx_partial.recv()=> {
                match new_partial{
                    Some((_partial, cb)) => cb.reply(Err(ChainError::DkgSetupRequired)),
                    None => return Err(ChainError::PartialClosedTx)
                }
            }

            cmd = cc.chan.rx_cmd.recv()=> {
                match cmd{
                    Some(ChainCmd::NewEpoch{first_round:_}) => {
                        // Chain config will be reused for non-default chain (see: [init_chain]).
                        return Ok(Some(cc))
                    }
                    Some(ChainCmd::Shutdown(cb))=> {
                        cb.reply(Ok(()));
                        return Ok(None)
                    },
                    Some(ChainCmd::Follow{req, cb})=>{
                        cb.reply(
                            follow_chain::<S, B>(&cc, &req, &mut chain_info, &mut sync_handle).await
                        );
                    },
                    Some(ChainCmd::LatestStored(cb))=>{
                        cb.reply(
                            match cc.store.last().await{
                                Ok(last) => Ok(StatusResponse{latest_stored_round: last.round()}),
                                Err(err) => Err(err),
                            }
                        );
                    }
                    Some(ChainCmd::Reload)=> unreachable!("reload is never called on default chain"),
                    // Following the node without DKG setup is forbidden.
                    Some(ChainCmd::ReSync {from_round: _, cb})=> cb.reply(Err(StoreError::Internal)),
                    // Same for ChainInfo.
                    Some(ChainCmd::ChainInfo(cb))=>cb.reply(Err(ChainError::DkgSetupRequired)),
                    None => return Err(ChainError::CmdClosedTx),
                }
            }
        }
    }
}

async fn follow_chain<S: Scheme, B: BeaconRepr>(
    cc: &ChainConfig<B>,
    req: &StartSyncRequest,
    chain_info: &mut ChainInfo<S>,
    handle: &mut Option<JoinHandle<Result<(), SyncError>>>,
) -> Result<mpsc::Receiver<Result<SyncProgress, Status>>, SyncError> {
    let should_proceed = match handle {
        Some(ref h) => h.is_finished(),
        None => true,
    };

    if should_proceed {
        let l = tracing::info_span!(
            "",
            follow_chain = format!("{}.{}", cc.private_listen, cc.beacon_id)
        );
        let new_config = start_follow_chain(req, &cc.beacon_id, &cc.store, l).await?;
        let new_ci = new_config.chain_info_from_packet()?;

        if chain_info.genesis_seed.is_empty() {
            *chain_info = new_ci;
        } else if *chain_info != new_ci {
            return Err(SyncError::InfoPacketMismatch);
        }

        // Follow request always has upper boundary.
        let current_round = time::current_round(
            time::time_now().as_secs(),
            chain_info.period.get_value(),
            chain_info.genesis_time,
        );
        let target = if req.up_to > 0 && req.up_to < current_round {
            req.up_to
        } else {
            current_round
        };

        let syncer = DefaultSyncer::<S, B>::from_config(new_config)?;
        // Channel to display (and keep-alive) sync progress on client side.
        let (tx, rx) = mpsc::channel(128);

        *handle = Some(syncer.process_follow_request(target, tx));

        Ok(rx)
    } else {
        Err(SyncError::AlreadySyncing)
    }
}

async fn run_chain<S: Scheme, B: BeaconRepr>(
    inner: ChainConfig<B>,
) -> Result<Option<ChainConfig<B>>, ChainError> {
    // Initialize handler and registry.
    let (h, mut reg, mut channels) = ChainHandler::<S, B>::from_config(inner).await?;

    // Add epoch nodes into connection pool to broadcast our partial beacon packets.
    h.register_in_pool().await?;

    // Start round ticker.
    let mut rx_round = ticker::start_ticker(h.chain_info.genesis_time, h.chain_info.period);
    info!(parent: &h.l, "run_chain: latest stored {}, current {}",  reg.latest_stored().round(), reg.current_round());

    loop {
        tokio::select! {
            // New round from round ticker.
            round = rx_round.recv()=> {
                let Some(round) = round else {
                     return Err(ChainError::TickerClosedTx)
                };
                reg.new_round(round);

                info!(parent: &h.l, "{{\"beacon_loop\": \"new_round\", \"round\": {}, \"lastbeacon\": {}}}", reg.current_round(), reg.latest_stored().round());
                let partial = h.sign_partial(&mut reg).await?;
                h.broadcast(partial).await?;

                // Trigger cachup and resync, starting them if needed and not already running..
                h.check_resync_catchup(&mut reg);
            }

            // Partial beacon packet received from other nodes.
            partial = channels.rx_partial.recv()=> {
                let Some((partial, cb)) = partial else{
                    return Err(ChainError::PartialClosedTx)
                };
                cb.reply(h.process_partial(&mut reg, partial).await);
            }

            // Signal arrives if catchup mode is active.
            signal = channels.rx_catchup.recv()=>{
                if signal.is_some(){
                    reg.catchup_signal_received();
                    let packet = h.sign_partial(&mut reg).await?;
                    h.broadcast(packet).await?;
                }
            }

            // Beacon packet from resync task.
            resynced = channels.rx_resync.recv()=>{
                if let Some(p)=resynced{
                    h.save_resynced(p, &mut reg).await?;
                }
            }

            cmd = channels.rx_cmd.recv()=> {
                match cmd {
                    Some(ChainCmd::NewEpoch{first_round})=>{
                        // We need to store last round before transition.
                        let want_round = first_round - 1;
                        warn!(parent: &h.l, "new epoch will start at round {first_round}");
                        wait_last_round(want_round, h.store.clone(), channels.tx_cmd.clone(), h.l.clone());
                    },
                    Some(ChainCmd::Reload)=> {
                        info!(parent: &h.l, "stop_chain: reconfiguration: moving to new epoch!");
                        break
                    },
                    Some(ChainCmd::ReSync{from_round,cb})=>h.store.sync(from_round,cb).await,
                    Some(ChainCmd::Follow{ req:_, cb})=>cb.reply(Err(SyncError::ForbiddenToFollow)),
                    Some(ChainCmd::Shutdown(cb))=>{
                        h.pool.remove_id(h.chain_info.beacon_id).await.map_err(|_|ChainError::PoolClosedRx)?;
                        cb.reply(Ok(()));
                        return Ok(None);
                    },
                    Some(ChainCmd::ChainInfo(cb))=>{
                        let Some(packet)=h.chain_info.as_packet() else{
                            error!(parent: &h.l, "failed to map chain_info to packet");
                            cb.reply(Err(ChainError::FailedToGetInfo));
                            return Err(ChainError::FailedToGetInfo)
                        };
                        cb.reply(Ok(packet));
                    }
                    None => return Err(ChainError::CmdClosedTx),
                    Some(ChainCmd::LatestStored(cb))=>{
                        cb.reply(
                            match h.store.last().await{
                                Ok(last) => Ok(StatusResponse{latest_stored_round: last.round()}),
                                Err(err) => Err(err),
                            }
                        );
                    }
                }
            }
        }
    }

    // Prepare for transition.
    // Remove old nodes from connection pool.
    h.pool
        .remove_id(h.chain_info.beacon_id.clone())
        .await
        .map_err(|_| ChainError::PoolClosedRx)?;

    let config_for_next_epoch = ChainConfig {
        chan: channels,
        pool: h.pool,
        store: h.store,
        private_listen: h.private_listen,
        beacon_id: h.chain_info.beacon_id,
        fs: h.fs,
        our_addres: h.our_addres,
    };

    Ok(Some(config_for_next_epoch))
}

/// Top-level function of chain module.
///
/// Node can be started as fresh [`run_chain_default`] or with DKG setup [`run_chain`].
/// Outputs with `Ok(None)` indicate graceful shutdown.
pub fn init_chain<S: Scheme, B: BeaconRepr>(
    is_fresh_run: bool,
    fs: FileStore,
    private_listen: String,
    pool: PoolSender,
    id: String,
    our_addres: Address,
    t: &TaskTracker,
) -> (mpsc::Sender<PartialPacket>, mpsc::Sender<ChainCmd>) {
    // #[hot]
    // Shortcut channel to send partial beacons from server side to chain handler directly.
    let (tx_partial, rx_partial) = mpsc::channel(1);

    // Channel to send commands from main beacon process to chain handler.
    let (tx_cmd, rx_cmd) = mpsc::channel::<ChainCmd>(2);

    // Notification channel for signals delayed by catchup period.
    let (tx_catchup, rx_catchup) = mpsc::channel::<()>(1);

    // Channel for resyncing beacons.
    let (tx_resync, rx_resync) = mpsc::channel::<BeaconPacket>(64);

    let chan = Channels {
        rx_partial,
        tx_cmd: tx_cmd.clone(),
        rx_cmd,
        tx_catchup,
        rx_catchup,
        tx_resync,
        rx_resync,
    };

    t.spawn(async move {
        let store = match ChainStore::start(fs.chain_store_path(), id.clone()).await {
            Ok(store) => store,
            Err(err) => {
                error!(
                    "init_chain: failed to start actor for chain store, beacon id [{id}]: {err}"
                );
                return;
            }
        };

        let inner = ChainConfig {
            chan,
            pool,
            fs,
            store,
            private_listen,
            beacon_id: id,
            our_addres,
        };

        // Loaded fresh node.
        if let Err(err) = if is_fresh_run {
            match run_chain_default::<S, B>(inner).await {
                // First DKG is completed succesfully.
                Ok(Some(inner)) => {
                    let mut config = inner;
                    loop {
                        match run_chain::<S, B>(config).await {
                            Ok(Some(inner)) => config = inner,
                            Ok(None) => break Ok(()),
                            Err(err) => break Err(err),
                        }
                    }
                }
                Ok(None) => return,
                Err(err) => Err(err),
            }
        } else {
            // Loaded node with DKG setup.
            let mut config = inner;
            loop {
                match run_chain::<S, B>(config).await {
                    Ok(Some(inner)) => config = inner,
                    Ok(None) => break Ok(()),
                    Err(err) => break Err(err),
                }
            }
        } {
            error!("chain layer returned with error: {err}");
        }
    });

    (tx_partial, tx_cmd)
}

/// Duration for delay to recheck latest stored round.
/// Calculated as 1/4 of minimal catchup period.
const TRANSITION_DELAY: Duration = Duration::from_millis(250);

/// Transition is successful only if last round of finishing epoch is stored.
/// Note: Distributed materials for new epoch are saved into filesystem at the moment of receiving DKG output.
fn wait_last_round<B: BeaconRepr>(
    want_round: u64,
    store: ChainStore<B>,
    tx: mpsc::Sender<ChainCmd>,
    l: Span,
) {
    tokio::task::spawn({
        async move {
            let mut attempt = 1;
            loop {
                match store.last().await {
                    Ok(last_stored) => {
                        if last_stored.round() >= want_round {
                            warn!(parent: &l, "transition: epoch last round {want_round} has been stored. Start updating chain module...");
                            if tx.send(ChainCmd::Reload).await.is_err() {
                                error!(parent: &l, "transition: {}", ChainError::CmdClosedRx);
                            }
                            return;
                        }
                        warn!(parent: &l, "transition: waiting for {want_round} round, latest_stored {}, attempts {attempt}", last_stored.round());
                        sleep(TRANSITION_DELAY).await;
                        attempt += 1;
                    }
                    Err(err) => {
                        error!(parent: &l, "transition: {err}");
                        return;
                    }
                }
            }
        }
    });
}

/// Mitigates non-graceful transition corner case, where DKG output is already received
/// but node reloaded before transition time.
async fn check_transition(period: Seconds, transition_time: u64, l: &Span) {
    let epoch_last_round = transition_time - u64::from(period.get_value());
    let time_now = time::time_now().as_secs();
    if time_now < epoch_last_round {
        // Adding 1 second to skip last round tick of finished epoch.
        let delta = epoch_last_round - time_now + 1;
        warn!(parent: l, "non-graceful transition? time_now: {time_now}, transition_time: {transition_time}, sleeping {delta}s");
        sleep(Duration::from_secs(delta)).await;
    }
}
