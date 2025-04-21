//! This module includes a roadmap that is executing in [`initial_dkg_n_nodes`] test.
//!
//!
//! ## KEYGEN & START
//! [x] 1. Generate keypairs and start one node-go and (N=10) nodes-rs with default keypair scheme.
//!
//! ## INITIAL DKG
//! [x] 2. Initiate dkg from node-go. Verify the status-rs is changed into Proposed.
//! [x] 3. Join the dkg from nodes-rs. Verify statuses-rs to be Joined.
//! [x] 4. Execute the dkg from node-go. Verify statuses-rs to be Executing.
//! [x] 5. Verify status-go is changed into Complete.
//! [x] 6. Cleanup all nodes filesystem & shutdown.
//! [ ] Finish.
//! [ ] Finish for all supported schemes.
//!
//! ## RESHAPE
//! [ ] Group is same
//! [ ] Group is different: joining/leaving, threshold.
//!
//! ## BEACON
//! [ ] Generation for default scheme
//! [ ] Generation for all supported schemes

use crate::dkg::status::Status;
use crate::test::helpers::*;
use std::time::Duration;
use tokio::time::sleep;

// # Scripts to iteract with Drand-go implementation
//
/// Start node
const START: &str = "src/test/interop/scripts/start.sh";
/// Generate proposal
const PROPOSAL: &str = "src/test/interop/scripts/gen_proposal.sh";
/// Initiate dkg
const INIT_DKG: &str = "src/test/interop/scripts/init_dkg.sh";
/// Execute dkg
const EXECUTE: &str = "src/test/interop/scripts/execute.sh";
/// Stop daemon & cleanup
const STOP: &str = "src/test/interop/scripts/stop.sh";

#[tokio::test]
async fn initial_dkg_n_nodes() {
    // uncomment for logs
    // crate::log::init_log(true).unwrap();

    // group size: 1 Leader-go + 10 members-rs = 11 nodes
    let nodes_rs = 10;
    let control_go = "55555";
    let beacon_id = "AAA";
    let scheme = "pedersen-bls-chained";

    // [1] Generate keypairs and start one node-go and (N=10) nodes-rs with default keypair scheme.
    //
    // GOLANG
    cmd_golang(START).await;
    // sleep 1sec to account slow CI/CD pipelines
    sleep(Duration::from_secs(1)).await;
    // RUST
    let nodes = Nodes::generate(nodes_rs, scheme, beacon_id).await;
    // Dkg status should be Fresh
    nodes.assert_statuses(Status::Fresh, None).await;

    // [2] Initiate dkg from node-go. Verify the status-rs is changed into Proposed.
    //
    // GOLANG
    cmd_golang_args(PROPOSAL, nodes.get_addresses()).await;
    cmd_golang(INIT_DKG).await;
    assert_statuses(control_go, beacon_id, Status::Proposing, None).await;
    // RUST
    nodes.assert_statuses(Status::Proposed, None).await;

    // [3] Join the dkg from nodes-rs. Verify statuses-rs to be Joined.
    //
    // RUST
    nodes.dkg_join().await;
    nodes.assert_statuses(Status::Joined, None).await;

    // [4] Execute the dkg from node-go. Verify statuses-rs to be Executing.
    //
    // GOLANG
    cmd_golang(EXECUTE).await;
    assert_statuses(control_go, beacon_id, Status::Executing, None).await;
    // RUST
    nodes.assert_statuses(Status::Executing, None).await;

    // [5] Verify status-go is changed into Complete.
    //
    // Give protocol the time to finish, (fastsync is always true)
    sleep(Duration::from_secs(10)).await;
    // GOLANG
    assert_statuses(
        control_go,
        beacon_id,
        // current
        Status::Complete,
        // finished
        Some(Status::Complete),
    )
    .await;

    // [6] Cleanup all nodes filesystem & shutdown.
    //
    // GOLANG
    cmd_golang(STOP).await;
    // RUST
    nodes.stop_daemon().await
}
