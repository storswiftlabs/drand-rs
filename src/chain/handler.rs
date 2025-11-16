use super::{
    cache,
    epoch::{EpochConfig, EpochNode},
    info::ChainInfo,
    registry::Registry,
    store::{BeaconRepr, ChainStore, StoreError, StoreStreamResponse},
    sync::{self, DefaultSyncer, SyncError, LOGS_TO_SKIP},
    ticker, time,
};
use crate::{
    core::beacon::{BeaconID, BeaconProcessError},
    key::{
        group::Group,
        keys::DistPublic,
        store::{FileStore, FileStoreError},
        Scheme,
    },
    log::Logger,
    net::{
        metrics::{self, GroupMetrics},
        pool::PoolSender,
        protocol::{PartialMsg, PartialPacket},
        utils::{Address, Callback, Seconds},
    },
    protobuf::drand::{
        BeaconPacket, ChainInfoPacket, PartialBeaconPacket, StartSyncRequest, StatusResponse,
        SyncProgress,
    },
    {debug, error, info, warn},
};
use energon::{drand::traits::BeaconDigest, kyber::tbls, points::SigPoint, traits::Affine};
use rand::seq::SliceRandom;
use std::{fmt::Debug, time::Duration};
use tokio::{
    sync::mpsc,
    task::JoinHandle,
    time::{sleep, Instant},
};
use tokio_util::task::TaskTracker;
use tonic::Status;

const SHORT_SIG_BYTES: usize = 3;

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
    #[error("failed to proceed chain_info request")]
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
    TBlsError(#[from] tbls::TBlsError),
    #[error("fs: {0}")]
    FileStoreError(#[from] FileStoreError),
    #[error("recover_unchecked: scalar is non-invertable")]
    NonInvertableScalar,
    #[error("internal error")]
    Internal,
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
    /// Private binding address.
    private_listen: String,
    /// Public URI Authority.
    our_addres: Address,
    log: Logger,
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
    ChainInfo(Callback<ChainInfoPacket, BeaconProcessError>),
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
    LatestStored(Callback<StatusResponse, BeaconProcessError>),
}

/// Holder to simplify channels management, see [`init_chain`] for detailed channels description.
struct Channels {
    rx_partial: mpsc::Receiver<PartialMsg>,
    tx_cmd: mpsc::Sender<ChainCmd>,
    rx_cmd: mpsc::Receiver<ChainCmd>,
    tx_resync: mpsc::Sender<BeaconPacket>,
    rx_resync: mpsc::Receiver<BeaconPacket>,
    tx_catchup: mpsc::Sender<()>,
    rx_catchup: mpsc::Receiver<()>,
}

/// Configuration used during transitions (see [`run_chain`] and [`run_chain_default`]).  
/// This config is same for all epoches.
pub struct ChainConfig<B: BeaconRepr> {
    chan: Channels,
    pool: PoolSender,
    fs: FileStore,
    store: ChainStore<B>,
    private_listen: String,
    id: BeaconID,
    our_addres: Address,
}

impl<S: Scheme, B: BeaconRepr> ChainHandler<S, B> {
    /// This function updates the epoch configuration and logger for chain handler
    /// after each DKG.
    ///
    /// Returns top-level components of chain module with their channels:
    /// - Chain configuration: [`ChainHandler`].
    /// - Operational state: [`Registry`]. (mut)
    pub async fn from_config(
        c: ChainConfig<B>,
    ) -> Result<(Self, Registry<S, B>, Channels), FileStoreError> {
        let ChainConfig {
            chan: channels,
            pool,
            fs,
            store,
            private_listen,
            id,
            our_addres,
        } = c;

        // Load group and share from filestore.
        let group = fs.load_group::<S>()?;

        // Map group and share into epoch config.
        let Group {
            threshold,
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

        let group_size = nodes.len();
        let metrics = GroupMetrics::new(group_size, threshold);
        let ec = EpochConfig::new(nodes, share, metrics);
        let log = crate::log::set_chain(&private_listen, beacon_id.as_str(), ec.our_index());

        // Check corner case for transition.
        check_transition(period, transition_time, &log).await;

        // Genesis beacon should always match the group.genesis_seed.
        store
            .check_genesis(&genesis_seed, &log)
            .await
            .map_err(|_| FileStoreError::InvalidData)?;

        let chain_info = ChainInfo {
            public_key,
            beacon_id: id,
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
            log,
        };

        let latest_stored = chain_handler.store.last().await?;

        let registry = Registry::new(
            &chain_handler.chain_info,
            latest_stored,
            channels.tx_catchup.clone(),
            channels.tx_resync.clone(),
            threshold as usize,
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
            .add_id(self.chain_info.beacon_id, peers)
            .await
            .map_err(|_| ChainError::PoolClosedRx)
    }

    /// Signs own partial, returns packet to broadcast.
    async fn sign_partial(
        &self,
        reg: &mut Registry<S, B>,
    ) -> Result<PartialBeaconPacket, ChainError> {
        reg.align_cache(&self.ec, &self.log)?;
        let c_round = reg.current_round();
        let ls_round = reg.latest_stored().round();

        let (previous_signature, round) = if c_round == ls_round {
            debug!(
                &self.log,
                "re-broadcasting already stored beacon, round {c_round}"
            );
            let prev_sig = match reg.latest_stored().prev_signature() {
                Some(s) => s.to_vec(),
                None => {
                    if S::Beacon::is_chained() {
                        error!(
                            &self.log,
                            "prev_sig is empty for chained beacon, round {c_round}, scheme {}, please report this",
                            S::ID
                        );
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
                error!(&self.log, "sign partial: round {round} {err}");
                return Err(ChainError::TBlsError(err));
            }
        };

        let partial_sig = match sigshare.serialize() {
            Ok(s) => s,
            Err(err) => {
                error!(&self.log, "serialize sigshare: round {round} {err}");
                return Err(ChainError::TBlsError(err));
            }
        };

        // Add own share into cache if the share is for round we want to aggregate.
        if round == ls_round + 1
            && round <= c_round
            && !reg.cache().is_share_present(self.ec.our_index())
        {
            if let Some(thr_sigs) =
                reg.cache_mut()
                    .add_prechecked(sigshare, &self.our_addres, &self.log)
            {
                let Ok(recovered) = tbls::recover_unchecked(thr_sigs) else {
                    return Err(ChainError::NonInvertableScalar);
                };
                self.save_recovered(round, &recovered, reg).await?;
            }
        }

        let short_prev_sig = previous_signature
            .get(..SHORT_SIG_BYTES)
            .unwrap_or_default();
        let short_msg = msg.get(..SHORT_SIG_BYTES).expect("always 32 bytes");
        debug!(
            &self.log,
            "broadcast_partial: {round}, prev_sig {}, msg_sign {}",
            hex::encode(short_prev_sig),
            hex::encode(short_msg)
        );
        let packet = PartialBeaconPacket {
            round,
            previous_signature,
            partial_sig,
            metadata: crate::protobuf::drand::Metadata::golang_node_version(
                self.chain_info.beacon_id.to_string(),
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
            error!(
                &self.log,
                "failed to broadcast partial: round {round} {}",
                ChainError::PoolClosedRx
            );
            return Err(ChainError::PoolClosedRx);
        }

        Ok(())
    }

    // Returns `true` if full signature has been recovered.
    // This will be used to track app layer latency for the event.
    async fn process_partial(
        &self,
        reg: &mut Registry<S, B>,
        partial: PartialPacket,
    ) -> Result<bool, ChainError> {
        let p_round = partial.packet.round;
        let c_round = reg.current_round();
        let ls_round = reg.latest_stored().round();
        debug!(
            &self.log,
            "processing partial: round {p_round}, from {}", partial.from
        );

        if p_round <= ls_round {
            debug!(
                &self.log,
                "ignoring partial: round {p_round}, current {c_round}, latest_stored {ls_round}"
            );
            return Ok(false);
        }
        // Allowed one round off in the future because of small clock drifts possible.
        if p_round > c_round + 1 {
            debug!(
                &self.log,
                "ignoring future partial: round {p_round}, current {c_round}",
            );
            return Err(ChainError::InvalidRound {
                invalid: p_round,
                current: c_round,
            });
        }

        // Cache updates once per stored beacon, between updates this call is cheap.
        reg.align_cache(&self.ec, &self.log)?;

        // Add packet to cache if p_round hits the allowed range and signature is not duplicated.
        if p_round > ls_round + 1 && p_round <= ls_round + 1 + cache::CACHE_LIMIT_ROUNDS {
            let Some(is_added) = reg.cache_mut().add_packet(partial.packet) else {
                error!(&self.log, "cache_height {}: attempt to add non-allowed round {p_round}, current {c_round}, please report this.", reg.cache().height());
                return Err(ChainError::InvalidRound {
                    invalid: p_round,
                    current: c_round,
                });
            };
            if is_added {
                debug!(&self.log, "cache: added partial for round {p_round}, latest stored {ls_round}, current {c_round}");
            } else {
                debug!(
                    &self.log,
                    "cache: ignoring already added partial for round {p_round} from {}",
                    partial.from
                );
            }
            return Ok(false);

        // Process packet as sigshare.
        } else if p_round == ls_round + 1 {
            let Some(idx) = cache::get_partial_index::<S>(&partial.packet.partial_sig) else {
                return Err(ChainError::InvalidShareLenght {
                    expected: <S::Sig as energon::traits::Group>::POINT_SIZE + 2,
                    received: partial.packet.partial_sig.len(),
                });
            };

            // Ignore already present sigshare.
            if reg.cache().is_share_present(idx) {
                debug!(
                    &self.log,
                    "ignoring already cached sigshare for round {p_round}",
                );
                return Ok(false);
            }
            let (valid_sigshare, node_addr) = self.ec.verify_partial(&partial.packet)?;

            // Recover and save beacon.
            // Note: Sigshares are prechecked and sorted by their index.
            if let Some(thr_sigs) =
                reg.cache_mut()
                    .add_prechecked(valid_sigshare, node_addr, &self.log)
            {
                let Ok(recovered) = tbls::recover_unchecked(thr_sigs) else {
                    return Err(ChainError::NonInvertableScalar);
                };
                if let Err(err) = self.save_recovered(p_round, &recovered, reg).await {
                    error!(
                        &self.log,
                        "failed to save recovered beacon for round {p_round}, {err}",
                    );
                    return Err(ChainError::Internal);
                }
                return Ok(true);
            }
        }

        Ok(false)
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
                    error!(
                        &self.log,
                        "round {r_round} {}",
                        ChainError::SerializeRecovered
                    );
                    return Err(ChainError::SerializeRecovered);
                };
                B::new(reg.latest_stored(), r_sig.into())
            } else {
                error!(
                    &self.log,
                    "round {r_round} {}",
                    ChainError::InvalidRecovered
                );
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
            info!(&self.log, "NEW_BEACON_STORED: round {r_round}, sig {}, prev_sig {:?}, time_discrepancy_ms {discrepancy}, storage_time_ms {storage_time}", valid_beacon.short_sig(), valid_beacon.short_prev_sig().unwrap_or_default());

            metrics::report_metrics_on_put(
                self.chain_info.beacon_id,
                discrepancy,
                r_round,
                self.ec.metrics_for_group(),
            );

            // Update registry.
            reg.update_latest_stored(valid_beacon);
            reg.align_cache(&self.ec, &self.log)?;

            // Check if catchup required.
            let ls_round = reg.latest_stored().round();
            let c_round = reg.current_round();
            let catchup_launch = c_round > ls_round;
            debug!(&self.log, "beacon_loop: catchupmode, last_is {ls_round}, current: {c_round}, catchup_launch {catchup_launch}");
            if catchup_launch {
                reg.start_catchup(self.catchup_period);
            }
        } else {
            // Beacon for this round has already been received via resync.
            // Stop resync as no longer relevant:
            // - We are at same height with other nodes (at least with thr nodes).
            // - If chain is late - we are on catchup mode (still no reason to resync unless network in ~strange state).
            // - Resync may be triggered again on next round tick.
            warn!(
                &self.log,
                "ignoring recovered beacon: round {r_round}, current {}, latest_stored {ls_round}",
                reg.current_round()
            );
            reg.stop_resync();
        };

        Ok(())
    }

    async fn save_resynced(
        &self,
        p: BeaconPacket,
        reg: &mut Registry<S, B>,
    ) -> Result<(), ChainError> {
        // Check if we still need beacon for this round (it might have already recovered on cathup mode).
        if p.round == reg.latest_stored().round() + 1 {
            let Ok(p_signature) = Affine::deserialize(&p.signature) else {
                error!(&self.log, "save_resynced: failed to deserialize signature for round {}, aborting resync task..", p.round);
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
                let discrepancy = time::round_discrepancy_ms(
                    self.chain_info.period,
                    self.chain_info.genesis_time,
                    p.round,
                );

                // Store beacon; measure storage time.
                let start = Instant::now();
                self.store.put(valid_beacon.clone()).await?;
                let storage_time = start.elapsed().as_millis();

                // Skip logs if round is too far from current round.
                if reg.current_round() - p.round < LOGS_TO_SKIP || p.round % 300 == 0 {
                    info!(&self.log, "NEW_BEACON_STORED: round {}, time_discrepancy_ms {discrepancy}, storage_time_ms {storage_time}", p.round);
                }

                metrics::report_metrics_on_put(
                    self.chain_info.beacon_id,
                    discrepancy,
                    p.round,
                    self.ec.metrics_for_group(),
                );

                // Update registry.
                reg.update_latest_stored(valid_beacon);
                reg.extend_resync_expiry_time();
            } else {
                error!(
                    &self.log,
                    "save_resynced: invalid signature for round {}, aborting resync task..",
                    p.round
                );
                reg.stop_resync();
            }
        } else {
            debug!(&self.log, "save_resynced: ignoring beacon for round {}, latest_stored {}, aborting sync task..", p.round, reg.latest_stored().round());
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

                let id = self.chain_info.beacon_id;
                let start_from = ls_round + 1;
                let up_to = c_round;
                let handle =
                    super::sync::resync(start_from, up_to, peers, id, tx_resync, self.log.clone());
                reg.new_resync_handle(self.chain_info.period, handle);
            }
        }
    }
}

/// Default chain used for fresh nodes without DKG setup.
async fn run_chain_default<S: Scheme, B: BeaconRepr>(
    mut cc: ChainConfig<B>,
    log: Logger,
) -> Result<Option<ChainConfig<B>>, ChainError> {
    info!(&log, "will run as fresh install -> expect to run DKG.");
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
                            follow_chain::<S, B>(&cc, &req, &mut chain_info, &mut sync_handle, log.clone()).await
                        );
                    },
                    Some(ChainCmd::LatestStored(cb))=>{
                        match cc.store.last().await{
                            Ok(last) => cb.reply(Ok(StatusResponse{latest_stored_round: last.round()})),
                            Err(err) => {
                                error!(&log, "latest_stored request failed: {err}");
                                cb.reply(Err(BeaconProcessError::NotAvailable));
                            },
                        }
                    }
                    Some(ChainCmd::Reload)=> unreachable!("reload is never called on default chain"),
                    // Syncing from node without DKG setup is forbidden.
                    Some(ChainCmd::ReSync {from_round: _, cb})=> cb.reply(Err(StoreError::Internal)),
                    Some(ChainCmd::ChainInfo(cb))=> cb.reply(Err(BeaconProcessError::NotAvailable)),
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
    log: Logger,
) -> Result<mpsc::Receiver<Result<SyncProgress, Status>>, SyncError> {
    let should_proceed = match handle {
        Some(ref h) => h.is_finished(),
        None => true,
    };

    if should_proceed {
        let new_config = sync::start_follow_chain(req, cc.id, &cc.store, &log).await?;
        let new_ci = ChainInfo::<S>::from_packet(&new_config.packet, new_config.beacon_id)
            .map_err(|err| {
                error!(&log, "{err}");
                SyncError::InvalidInfoPacket
            })?;

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

        let syncer = DefaultSyncer::<S, B>::from_config(new_config, &log)?;
        // Channel to display (and keep-alive) sync progress on client side.
        let (tx, rx) = mpsc::channel(128);

        *handle = Some(syncer.process_follow_request(target, tx, log));

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
    info!(
        &h.log,
        "chain is initialized: latest stored {}, current {}",
        reg.latest_stored().round(),
        reg.current_round()
    );

    loop {
        tokio::select! {
            // New round from round ticker.
            round = rx_round.recv()=> {
                let Some(round) = round else {
                     return Err(ChainError::TickerClosedTx)
                };
                reg.new_round(round);

                debug!(&h.log, "beacon_loop: new_round: {}, last_beacon: {}", reg.current_round(), reg.latest_stored().round());
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
                        warn!(&h.log, "new epoch will start at round {first_round}");
                        wait_last_round(want_round, h.store.clone(), channels.tx_cmd.clone(), h.log.clone());
                    },
                    Some(ChainCmd::Reload)=> {
                        info!(&h.log, "stop_chain: reconfiguration: moving to new epoch!");
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
                            cb.reply(Err(BeaconProcessError::Internal));
                            return Err(ChainError::FailedToGetInfo)
                        };
                        cb.reply(Ok(packet));
                    }
                    None => return Err(ChainError::CmdClosedTx),
                    Some(ChainCmd::LatestStored(cb))=>{
                        match h.store.last().await{
                            Ok(last) => cb.reply(Ok(StatusResponse{latest_stored_round: last.round()})),
                            Err(err) => {
                                cb.reply(Err(BeaconProcessError::Internal));
                                error!(&h.log, "latest_stored request failed: {err}");
                                return Err(ChainError::Internal);
                            },
                        }
                    }
                }
            }
        }
    }

    // Prepare for transition.
    // Remove old nodes from connection pool.
    h.pool
        .remove_id(h.chain_info.beacon_id)
        .await
        .map_err(|_| ChainError::PoolClosedRx)?;

    let config_for_next_epoch = ChainConfig {
        chan: channels,
        pool: h.pool,
        store: h.store,
        private_listen: h.private_listen,
        id: h.chain_info.beacon_id,
        fs: h.fs,
        our_addres: h.our_addres,
    };

    Ok(Some(config_for_next_epoch))
}

/// Top-level function of chain module.
///
/// Node can be started as fresh [`run_chain_default`] or with DKG setup [`run_chain`].
/// Outputs with `Ok(None)` indicate graceful shutdown.
#[allow(
    clippy::too_many_arguments,
    reason = "1: data is already is scope. 2: global config eats the stack"
)]
pub fn init_chain<S: Scheme, B: BeaconRepr>(
    is_fresh_run: bool,
    fs: FileStore,
    private_listen: String,
    pool: PoolSender,
    id: BeaconID,
    our_addres: Address,
    log: Logger,
    t: &TaskTracker,
) -> (mpsc::Sender<PartialMsg>, mpsc::Sender<ChainCmd>) {
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
        let store = match ChainStore::start(fs.chain_store_path(), id, log.clone()).await {
            Ok(store) => store,
            Err(err) => {
                error!(
                    &log,
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
            id,
            our_addres,
        };

        // Loaded fresh node.
        if let Err(err) = if is_fresh_run {
            match run_chain_default::<S, B>(inner, log.clone()).await {
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
            error!(&log, "chain layer: {err}");
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
    log: Logger,
) {
    tokio::task::spawn({
        async move {
            let mut attempt = 1;
            loop {
                match store.last().await {
                    Ok(last_stored) => {
                        if last_stored.round() >= want_round {
                            warn!(&log, "transition: epoch last round {want_round} has been stored. Start updating chain module...");
                            if tx.send(ChainCmd::Reload).await.is_err() {
                                error!(&log, "transition: {}", ChainError::CmdClosedRx);
                            }
                            return;
                        }
                        warn!(&log, "transition: waiting for {want_round} round, latest_stored {}, attempts {attempt}", last_stored.round());
                        sleep(TRANSITION_DELAY).await;
                        attempt += 1;
                        if attempt > 30 {
                            warn!(
                                &log,
                                "please check if transition time is same for all nodes"
                            );
                        }
                    }
                    Err(err) => {
                        error!(&log, "transition: {err}");
                        return;
                    }
                }
            }
        }
    });
}

/// Mitigates transition corner case, where DKG output is already received
/// but node manually reloaded before transition time.
async fn check_transition(period: Seconds, transition_time: u64, log: &Logger) {
    let epoch_last_round = transition_time - u64::from(period.get_value());
    let time_now = time::time_now().as_secs();
    if time_now < epoch_last_round {
        // Adding 1 second to skip last round tick of finished epoch.
        let delta = epoch_last_round - time_now + 1;
        warn!(log, "transition is not graceful, time_now {time_now}, transition_time {transition_time}, sleeping {delta}s");
        sleep(Duration::from_secs(delta)).await;
    }
}
