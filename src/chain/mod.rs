// Copyright 2023-2025 StorSwift Inc.
// SPDX-License-Identifier: Apache-2.0

mod cache;
mod epoch;
mod handler;
mod info;
mod registry;
mod store;
mod sync;
mod ticker;
pub mod time;

use energon::drand::traits::BeaconDigest;
pub use handler::{init_chain, ChainCmd, ChainError};
pub use store::{ChainedBeacon, StoreError, StoreStreamResponse, UnChainedBeacon};
pub use sync::SyncError;

/// BLS signature check for aggregated or resynced beacons.
/// Suitable for chained and unchained schemes.
fn is_valid_signature<S: crate::key::Scheme>(
    pub_key: &energon::points::KeyPoint<S>,
    prev_sig: &[u8],
    new_round: u64,
    new_sig: &energon::points::SigPoint<S>,
) -> bool {
    let msg = S::Beacon::digest(prev_sig, new_round);
    S::bls_verify(pub_key, new_sig, &msg).is_ok()
}
