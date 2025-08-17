use super::cache::PartialCache;
use super::epoch::EpochConfig;
use super::info::ChainInfo;
use super::store::BeaconRepr;
use super::sync::HandleReSync;
use super::sync::SyncError;
use super::time;
use crate::key::Scheme;
use crate::net::utils::Seconds;
use crate::protobuf::drand::BeaconPacket;

use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::Span;

/// Registry holds actual data that may change per / within a round.
pub struct Registry<S: Scheme, B: BeaconRepr> {
    /// Latest verified and successfully stored beacon.
    latest_stored: B,
    /// Current expected chain height.
    /// Updated from `RoundTicker` or calculated (see: [`Registry::new`]).
    current_round: u64,
    /// Cache for partial packets and sigshares.
    p_cache: PartialCache<S>,
    /// Always `true` if catchup has been triggered but its signal has not arrived yet.
    catchup_enabled: bool,
    /// Sender to be cloned for launching catchup task.
    /// The task finishes once signal is sent after catchup delay.
    tx_catchup: mpsc::Sender<()>,
    /// Sender to be cloned for launching resync task.
    tx_resync: mpsc::Sender<BeaconPacket>,
    /// Handle for resync task.
    h_resync: Option<HandleReSync>,
}

impl<S: Scheme, B: BeaconRepr> Registry<S, B> {
    pub fn new(
        info: &ChainInfo<S>,
        latest_stored: B,
        tx_catchup: mpsc::Sender<()>,
        tx_resync: mpsc::Sender<BeaconPacket>,
        thr: usize,
    ) -> Self {
        let current_round = time::current_round(
            time::time_now().as_secs(),
            info.period.get_value(),
            info.genesis_time,
        );
        let p_cache = PartialCache::new(latest_stored.round(), thr);

        Self {
            latest_stored,
            current_round,
            p_cache,
            catchup_enabled: false,
            tx_catchup,
            tx_resync,
            h_resync: None,
        }
    }

    /// Updates expiry time for resync task to prevent handle from being aborted.
    pub fn extend_resync_expiry_time(&mut self) {
        if let Some(h) = self.h_resync.as_mut() {
            h.update_last_received_time();
        }
    }

    pub fn get_tx_resync(&self) -> mpsc::Sender<BeaconPacket> {
        self.tx_resync.clone()
    }

    /// Returns `true` if resync task is running and making progress.
    pub fn is_resync_active(&self) -> bool {
        if let Some(ref h) = self.h_resync {
            h.is_running()
        } else {
            false
        }
    }

    pub fn new_resync_handle(
        &mut self,
        period: Seconds,
        handle: JoinHandle<Result<(), SyncError>>,
    ) {
        self.h_resync = Some(HandleReSync::new(period, handle));
    }

    /// Spawns a task to send a single catch-up signal to the main chain logic.
    /// The registry prevents spawning multiple tasks if the previous signal has not been received.
    pub fn start_catchup(&mut self, catchup_period: Duration) {
        if !self.catchup_enabled {
            self.catchup_enabled = true;

            tokio::task::spawn({
                let tx = self.tx_catchup.clone();
                let sleep = tokio::time::sleep(catchup_period);

                async move {
                    sleep.await;
                    let _ = tx.send(()).await;
                }
            });
        }
    }

    /// Catchup can be enabled again after the signal is received.
    pub fn catchup_signal_received(&mut self) {
        self.catchup_enabled = false;
    }

    pub fn new_round(&mut self, new_round: u64) {
        self.current_round = new_round;
    }

    /// Updates the cache if the latest stored round differs
    /// from the cache internals (see: [`PartialCache::align`]).
    ///
    /// WARNING: To prevent burning of the partial cache,
    /// this method should NEVER be called within the logic for resync.
    pub fn align_cache(&mut self, ec: &EpochConfig<S>, l: &Span) {
        self.p_cache.align(ec, self.latest_stored.round(), l);
    }

    pub fn cache(&self) -> &PartialCache<S> {
        &self.p_cache
    }

    pub fn cache_mut(&mut self) -> &mut PartialCache<S> {
        &mut self.p_cache
    }

    /// Registry should always be updated AFTER the beacon is stored in persistent DB.
    pub fn update_latest_stored(&mut self, latest_stored: B) {
        self.latest_stored = latest_stored;
    }

    pub fn current_round(&self) -> u64 {
        self.current_round
    }

    pub fn latest_stored(&self) -> &B {
        &self.latest_stored
    }

    pub fn stop_resync(&mut self) {
        self.h_resync = None;
    }
}
