//! Roadmap [initial]
//! [x] 1. Start golang and rust node with default keypair scheme.
//! [x] 2. Initiate dkg
//! [x] 3. Verify dkg status is changed into proposed.
//! [x] 4. Shutdown nodes
//! [ ] 5. Finish dkg
//! [ ] 6. Finish dkg for all supported schemes
//! [ ] 7. Beacon generation for default scheme
//! [ ] 8. Beacon generation for all supported schemes
//! [ ] 9. Repeat above for group sizes > 2

use crate::cli::CLI;
use crate::dkg::state_machine::status::Status;
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
/// Stop node and remove base folder
const STOP: &str = "src/test/interop/scripts/stop.sh";

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
        .expect("current dkg state can not be empty")
        .state;

    assert_eq!(current, expected as u32,);
}

#[tokio::test]
async fn initial() {
    // Uncomment for logs
    // crate::log::init_log(true).unwrap();

    let control_rs = "8888";
    let control_go = "55555";
    let address_rs = "127.0.0.1:22222";
    let beacon_id = "default";
    let base_config: TestConfig = TestConfig::new(control_rs, address_rs).await;

    // Generate keypair and start node-rs
    assert!(CLI::keygen(base_config.keygen_config()).run().await.is_ok());
    let (_fs_guard, config) = base_config.start_drand_node().await;

    // Dkg status for node-rs should be Fresh
    assert_status(&config.control, beacon_id, Status::Fresh).await;

    // Start node-go.
    // Note: Seems no straightforward way to _await_ that daemon
    //       is fully loaded in the background process.
    //       The current approach is to use a simple 1-second sleep to account
    //       slower CI/CD pipelines.
    cmd_golang(START).await;
    sleep(Duration::from_secs(1)).await;

    // Generate proposal and initiate dkg
    cmd_golang(PROPOSAL).await;
    cmd_golang(INIT_DKG).await;

    // Dkg statuses should match the expected.
    assert_status(control_go, beacon_id, Status::Proposing).await;
    assert_status(control_rs, beacon_id, Status::Proposed).await;

    // Stop drand-go & cleanup
    cmd_golang(STOP).await;
}
