use crate::cli::CLI;
use crate::net::control::ControlClient;
use crate::net::protocol::ProtocolClient;
use crate::net::utils::Address;
use crate::test::utils::TestConfig;

// General purpose test at the CLI level, to be extended.
#[tokio::test]
async fn modules_check() {
    // uncomment for logs
    // crate::log::init_log(true).unwrap();
    let mut base_config: TestConfig = TestConfig::as_default().await;

    let id_a = "AAA";
    let id_b = "BBB";

    // Generate keypair for `id_a`
    let mut keygen = base_config.keygen_config();
    keygen.id = id_a.into();
    assert!(CLI::keygen(keygen.clone()).run().await.is_ok());

    // Start drand node for all beacons, it should load `id_a` or fail,
    // daemon without beacons is not allowed to start.
    base_config.id = None;
    let (_fs_guard, config) = base_config.start_drand_node().await;

    // Generate keys for `id_b` and load them to the daemon
    keygen.id = id_b.into();
    assert!(CLI::keygen(keygen).run().await.is_ok());
    assert!(CLI::load_id(&config.control, id_b).run().await.is_ok());

    // Attempting to load an already running id should fail.
    assert!(CLI::load_id(&config.control, id_b).run().await.is_err());

    let mut control_client = ControlClient::new(&config.control)
        .await
        .expect("node should be reachable by control client");

    assert!(control_client.ping_pong().await.is_ok(), "ping is failed");

    let mut protocol_client =
        ProtocolClient::new(&Address::precheck(&config.private_listen).unwrap())
            .await
            .expect("node should be reachable by protocol client");

    // Make get_identity request for both loaded ids
    assert!(
        protocol_client.get_identity(id_a).await.is_ok(),
        "get_identity failed for '{id_a}'"
    );
    assert!(
        protocol_client.get_identity(id_b).await.is_ok(),
        "get_identity failed for '{id_b}'"
    );

    // The request should fail for non-loaded id
    assert!(
        protocol_client.get_identity("not_loaded").await.is_err(),
        "get_identity should fail for non-loaded"
    );

    // Shutdown id_a
    let is_daemon_running = control_client.shutdown(Some(id_a)).await.unwrap();
    assert!(is_daemon_running, "daemon should still run");

    // Node is stil reachable
    assert!(control_client.ping_pong().await.is_ok(), "ping_pong failed");

    // Shutdown id_b, no more loaded ids.
    let is_daemon_running = control_client
        .shutdown(Some(id_b))
        .await
        .expect("failed to send shutdown request");
    assert!(!is_daemon_running, "failed to shutdown");

    assert!(
        control_client.ping_pong().await.is_err(),
        "node should not be reachable"
    );
}
