//! This module provides API to setup and report metrics.
use crate::{core::beacon::BeaconID, net::utils::Address};
use anyhow::bail;
use metrics::{describe_gauge, gauge};
use metrics_exporter_prometheus::PrometheusBuilder;
use metrics_util::MetricKindMask;
use std::{net::SocketAddr, str::FromStr, time::Duration};

pub fn setup_metrics(address: &str) -> anyhow::Result<()> {
    let address = Address::precheck(address)?;

    let builder = PrometheusBuilder::new();
    if let Err(err) = builder
        .idle_timeout(
            MetricKindMask::COUNTER | MetricKindMask::HISTOGRAM | MetricKindMask::GAUGE,
            Some(Duration::from_secs(35)),
        )
        .with_http_listener(SocketAddr::from_str(address.as_str())?)
        .install()
    {
        bail!("failed to install Prometheus: {err}")
    }

    // Note: Keep same descriptions for same metrics with Drand-go.

    describe_gauge!(
        "beacon_discrepancy_latency",
        "Discrepancy between beacon creation time and calculated round time"
    );

    describe_gauge!("last_beacon_round", "Last locally stored beacon");

    describe_gauge!("group_size", "Number of peers in the current group");

    describe_gauge!(
        "group_threshold",
        "Number of shares needed for beacon reconstruction"
    );

    Ok(())
}

#[inline(always)]
/// Discrepancy between beacon creation time and calculated round time.
fn beacon_discrepancy_latency(id: BeaconID, value: f64) {
    gauge!("beacon_discrepancy_latency", "beacon_id" => id.as_str()).set(value);
}

#[inline(always)]
/// Last locally stored beacon.
fn last_beacon_round(id: BeaconID, value: f64) {
    gauge!("last_beacon_round", "beacon_id" => id.as_str()).set(value);
}

#[inline(always)]
/// Number of shares needed for beacon reconstruction.
fn group_threshold(id: BeaconID, value: f64) {
    gauge!("group_threshold", "beacon_id" => id.as_str()).set(value);
}

#[inline(always)]
/// Number of peers in the current group.
fn group_size(id: BeaconID, value: f64) {
    gauge!("group_size", "beacon_id" => id.as_str()).set(value);
}

/// Helper to report metrics once recovered or synced beacon is saved successfully to persistent storage.
pub fn report_metrics_on_put(
    id: BeaconID,
    discrepancy: u128,
    last_round: u64,
    group_metrics: GroupMetrics,
) {
    beacon_discrepancy_latency(id, discrepancy as f64);
    last_beacon_round(id, last_round as f64);
    group_threshold(id, group_metrics.thr);
    group_size(id, group_metrics.group_size);
}

#[derive(Default, Clone, Copy)]
/// Group metrics are same within an epoch, so we cast them *once* into
/// required `f64` representation to avoid repeated conversions.
pub struct GroupMetrics {
    group_size: f64,
    thr: f64,
}

impl GroupMetrics {
    pub fn new(group_size: usize, group_threshold: u32) -> Self {
        Self {
            group_size: group_size as f64,
            thr: f64::from(group_threshold),
        }
    }
}
