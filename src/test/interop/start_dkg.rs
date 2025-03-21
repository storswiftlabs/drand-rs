//! DEV Roadmap
//! Completed steps are placed in test [self::initial_dkg]
//! [x] 1. Start golang and rust node with default keypair scheme.
//!
//!  ## DKG
//! [x] 2. Initiate dkg from node-go.
//! [x] 3. Verify the status-rs is changed into Proposed.
//! [x] 4. Join the dkg from node-rs and verify status-rs to be Joined.
//! [x] 5. Execute the dkg from node-go, verify status-rs to be Executing.
//! [x] 6. Shutdown nodes
//! [ ] Finish
//! [ ] Finish for all supported schemes
//!
//! ## BEACON
//! [ ] Generation for default scheme
//! [ ] Generation for all supported schemes
//!
//! ## REPEAT
//! [ ] Repeat above for group sizes > 2

use crate::cli::CLI;
use crate::dkg::status::Status;
use crate::net::dkg_control::DkgControlClient;
use crate::test::utils::TestConfig;

use async_std::process;
use std::time::Duration;
use tokio::time::sleep;

// #Scripts to iteract with Drand golang implementation
//
/// Start node
const START: &str = "src/test/interop/scripts/start.sh";
/// Generate proposal
const PROPOSAL: &str = "src/test/interop/scripts/gen_proposal.sh";
/// Initiate dkg
const INIT_DKG: &str = "src/test/interop/scripts/init_dkg.sh";
/// Execute dkg
const EXECUTE: &str = "src/test/interop/scripts/execute.sh";
/// Stop node and remove base folder
const STOP: &str = "src/test/interop/scripts/stop.sh";

#[tokio::test]
async fn initial_dkg() {
    // Uncomment for logs
    // crate::log::init_log(true).unwrap();

    let control_rs = "8888";
    let control_go = "55555";
    let address_rs = "127.0.0.1:22222";
    let beacon_id = "default";
    let base_config = TestConfig::new(control_rs, address_rs).await;

    //_______ [x] 1. Start golang and rust node with default keypair scheme.
    //
    // Generate keypair and start node-rs
    assert!(CLI::keygen(base_config.keygen_config()).run().await.is_ok());
    let (_fs_guard, config) = base_config.start_drand_node().await;
    //
    // Dkg status for node-rs should be Fresh
    assert_status(&config.control, beacon_id, Status::Fresh).await;
    //
    // Generate keypair and start node-go.
    // Note: Seems no straightforward way to _await_ that daemon
    //       is fully loaded in the background process.
    //       The current approach is to use a simple 1-second sleep to account
    //       slower CI/CD pipelines.
    cmd_golang(START).await;
    sleep(Duration::from_secs(1)).await;

    //_______ [x] 2. Initiate dkg from node-go.
    //
    // Generate proposal and initiate DKG.
    cmd_golang(PROPOSAL).await;
    cmd_golang(INIT_DKG).await;

    //_______ [x] 3. Verify the status-rs is changed into Proposed.
    //
    // DKG statuses should match the expected.
    assert_status(control_go, beacon_id, Status::Proposing).await;
    assert_status(control_rs, beacon_id, Status::Proposed).await;

    //_______ [x] 4. Join the dkg from node-rs and verify status-rs to be Joined.
    assert!(CLI::dkg_join(control_rs, beacon_id).run().await.is_ok());
    assert_status(control_rs, beacon_id, Status::Joined).await;

    //_______ [x] 5. Execute the dkg from node-go.
    cmd_golang(EXECUTE).await;
    // DKG statuses should match the expected.
    assert_status(control_go, beacon_id, Status::Executing).await;
    assert_status(control_rs, beacon_id, Status::Executing).await;

    //_______ [x] 6. Shutdown nodes
    //
    // Stop drand-go & cleanup
    cmd_golang(STOP).await;
    // Stop drand-rs
    assert!(CLI::stop(control_rs, None).run().await.is_ok());
}

/// Asserts that _current_ DKG status is expected
///
/// # Arguments
///
///  * `port` - Node control port
///  * `id` - ID of beacon process
///  * `expected` - Status of the DKG state machine.
async fn assert_status(port: &str, id: &str, expected: Status) {
    let mut client = DkgControlClient::new(port)
        .await
        .expect("connection: dkg_control service should be reachable");

    let current = client
        .dkg_status(id)
        .await
        .expect("rpc Status should not fail")
        .current
        .expect("current dkg state should not be empty")
        .state;

    assert_eq!(current, expected as u32,);
}

/// A helper to run cli for Drand golang implementation
async fn cmd_golang(script: &str) {
    let output = match process::Command::new("/bin/bash")
        .args(["-c", script])
        .spawn()
    {
        Ok(mut cmd) => cmd.status().await.unwrap(),
        Err(err) => panic!("failed to spawn cmd {}, error: {}", script, err),
    };

    if !output.success() {
        panic!(
            "failed to execute script: {script}, code: {:?}",
            output.code()
        )
    }
}
