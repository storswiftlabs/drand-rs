// Copyright 2023-2025 StorSwift Inc.
// SPDX-License-Identifier: Apache-2.0

//! # Context
//! Beacon aggregation is the most CPU-intensive operation for randomness generation.
//!
//! # Functionality
//! This module provides functionality to measure application layer processing
//! time for packets which **successfully** trigger beacon aggregation.
//! Measurement is invoked on the server side for partial packets containing
//! last missing sigshare for partial cache which already has (threshold - 1)
//! valid sigshares for corresponding round. Callback is returned after
//! new aggregated beacon is saved into chain store.
//!
//! # Flow
//! 1. Partial packet arrived.
//! 2. Packet is being processed by application layer, awaiting callback.
//! 3. If callback returns with `is_aggregated == true` then
//!    measure the amount of time elapsed since the packet arrived.
//!    For details, see [`super::protocol::ProtocolHandler::partial_beacon`]
//!
//! # Report
//! Data accumulated into a statistical report (see [`write_report`])
//! and displayed in logs for each [`LOG_INTERVAL`].
//! This is useful for monitoring and for comparing performance
//! across different backend implementations.
//!
//! # Downside of this approach
//! Max outliers (> P50 * N: networks under monitoring) may appear due to async
//! cooperative task scheduling, meaning logs should be checked *manually*
//! to confirm the reason. Outliers are common when rounds from different
//! networks collide at the same starting time.

use crate::{debug, error, info, log::Logger};
use hdrhistogram::Histogram;
use std::{collections::HashSet, fmt::Write};
use tokio::{
    sync::mpsc::{self, error::TrySendError},
    time::{interval, Duration, Instant},
};

/// Interval to display report.
const LOG_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// Capacity is reserved for optimistic sending and should always be >= N + 1,
/// where N is the number of networks to monitor.
const CHANNEL_CAPACITY: usize = 5;

/// Non-blocking reporter (see: [`Self::send`]) used on server side to send
/// measured value into monitoring task.
pub struct AppResponseTimeReporter(Option<Reporter>);

impl AppResponseTimeReporter {
    /// Spawns reporting task if input is valid.
    pub fn initialize(beacon_ids: Vec<String>, log: Logger) -> Self {
        // No configuration passed.
        if beacon_ids.is_empty() {
            return Self(None);
        }

        if beacon_ids.len() >= CHANNEL_CAPACITY {
            error!(
                &log,
                "setup ignored: max networks to track {}, increase channel capacity",
                CHANNEL_CAPACITY - 1
            );
            return Self(None);
        }

        let unique_count = beacon_ids.iter().collect::<HashSet<_>>().len();
        if unique_count != beacon_ids.len() {
            error!(
                &log,
                "setup ignored: beacon id duplication is not allowed: {:?}", beacon_ids
            );
            return Self(None);
        }

        Self(Some(Reporter::new_checked(beacon_ids, log)))
    }

    pub async fn send(&self, tracked: TrackedID, delay_ms: u64, round: u64) {
        // Note(unwrap): [TrackedID] can only be created if reporter is Some, see [Self::check_id].
        let reporter = self.0.as_ref().unwrap();

        let record = Record {
            id: tracked.encoded_id,
            value_ms: delay_ms,
        };
        let id_str = reporter.networks.as_str(record.id);
        debug!(
            &reporter.log,
            "new record for {id_str}: round {round}, with_delay_ms {}", record.value_ms
        );

        if let Err(TrySendError::Full(record)) = reporter.sender.try_send(record) {
            if reporter.sender.send(record).await.is_err() {
                error!(&reporter.log, "failed to send record for {id_str}");
            };
        }
    }

    /// Returns metadata if beacon ID is tracked.
    pub fn check_id(&self, id: &str) -> Option<TrackedID> {
        let reporter = self.0.as_ref()?;
        let encoded_id = reporter.networks.as_usize(id)?;
        Some(TrackedID {
            start: Instant::now(),
            encoded_id,
        })
    }
}

/// Metadata for tracked beacon id.
pub struct TrackedID {
    /// Measurement once partial is received on RPC.
    start: Instant,
    /// Numeric beacon ID representation.
    encoded_id: usize,
}

impl TrackedID {
    /// Returns the amount of time elapsed since this ID was tracked.
    pub fn elapsed_ms(&self) -> u64 {
        self.start
            .elapsed()
            .as_millis()
            // Unwraps the error into noteciable outlier.
            // Max outlier is always included in final report.
            .try_into()
            .unwrap_or(u64::MAX)
    }
}

struct Reporter {
    networks: Monitored,
    sender: mpsc::Sender<Record>,
    log: Logger,
}

impl Reporter {
    fn new_checked(ids: Vec<String>, log: Logger) -> Self {
        let encoder = Monitored::new_checked(ids);
        let decoder = encoder.clone();

        let (tx, rx) = mpsc::channel::<Record>(CHANNEL_CAPACITY);
        tokio::spawn({
            let log = log.clone();
            async move {
                run(rx, decoder, log.clone())
                    .await
                    .inspect_err(|err| error!(&log, "latency check is failed: {err}"))
            }
        });

        Self {
            networks: encoder,
            sender: tx,
            log,
        }
    }
}

struct Record {
    /// Beacon ID from metadata of [`PartialBeaconPacket`] converted into numeric form.
    id: usize,
    /// Application layer processing time.
    value_ms: u64,
}

#[derive(Clone)]
struct Monitored {
    beacon_ids: Vec<String>,
}

impl Monitored {
    fn new_checked(beacon_ids: Vec<String>) -> Self {
        Self { beacon_ids }
    }

    /// Returns compact representation for beacon ID if such ID is enabled for monitoring.
    fn as_usize(&self, id_from_packet: &str) -> Option<usize> {
        self.beacon_ids.iter().position(|id| id == id_from_packet)
    }

    /// Converts a numeric representation back to the original beacon ID string slice.
    fn as_str(&self, encoded: usize) -> &str {
        self.beacon_ids[encoded].as_str()
    }
}

async fn run(mut rx: mpsc::Receiver<Record>, known: Monitored, log: Logger) -> anyhow::Result<()> {
    let mut hists = Vec::with_capacity(known.beacon_ids.len());
    for _ in 0..known.beacon_ids.len() {
        hists.push(Histogram::<u64>::new(3)?);
    }
    let mut report = String::with_capacity(70 * known.beacon_ids.len());
    let mut interval = interval(LOG_INTERVAL);
    interval.tick().await;

    info!(
        &log,
        "latency will be reported every {} minutes for {:?}",
        LOG_INTERVAL.as_secs() / 60,
        known.beacon_ids,
    );

    loop {
        tokio::select! {
           Some(msg) = rx.recv() => {
            hists[msg.id].record(msg.value_ms)?;

           }
           _ = interval.tick() => {
               for (id, hist) in hists.iter().enumerate() {
                   let id_str = known.as_str(id);
                   write_report(&mut report, id_str, hist)?;
               }
               info!(&log, "all values in ms except rounds_total{report}");
               report.clear();
           }
        }
    }
}

fn write_report(buf: &mut String, id: &str, histogram: &Histogram<u64>) -> anyhow::Result<()> {
    let p50 = histogram.value_at_quantile(0.5);
    let p90 = histogram.value_at_quantile(0.9);
    let p99 = histogram.value_at_quantile(0.99);
    let min = histogram.min();
    let max = histogram.max();
    let len = histogram.len();

    write!(
        buf,
        "\n{id:<10} | P50 {p50:<2} P90 {p90:<2} P99 {p99:<2} min {min:<2} max {max:<2} rounds_total {len}"
    )?;

    Ok(())
}
