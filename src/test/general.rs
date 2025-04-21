use crate::cli::CLI;
use crate::net::control::ControlClient;
use crate::net::protocol::ProtocolClient;
use crate::net::public::PublicClient;
use crate::net::utils::Address;
use crate::test::helpers::TestConfig;

// To be extended
#[tokio::test]
async fn basic_rpc() {
    // Uncomment for logs
    // crate::log::init_log(true).unwrap();
    let mut base_config: TestConfig = TestConfig::as_default().await;

    // Name two beacon ids
    let id_a = "AAA";
    let id_b = "BBB";

    // #Case: Generate keypair and start node
    //
    // 1. Generate keypair for id_a
    let mut keygen = base_config.keygen();
    keygen.id = id_a.into();
    assert!(CLI::keygen(keygen.clone()).run().await.is_ok());
    // 2. Starting node should load id_a or fail. Empty daemon is not allowed
    base_config.id = None;
    let (_fs_guard, config) = base_config.start_node().await;
    let node_address = Address::precheck(&config.private_listen).unwrap();

    // #Case: Load keypair into runninig node
    //
    // 1. Generate keypair for id_b
    keygen.id = id_b.into();
    assert!(CLI::keygen(keygen).run().await.is_ok());
    // 2. Load id_b into running node
    assert!(CLI::load_id(&config.control, id_b).run().await.is_ok());
    // 3. Attempt to load an already loaded id should fail
    assert!(CLI::load_id(&config.control, id_b).run().await.is_err());
    // 4. Attempt to load non-existing id should fail
    assert!(CLI::load_id(&config.control, "unknow").run().await.is_err());

    // #Identity requests
    //
    // 1. Establish connection
    let mut protocol_client = ProtocolClient::new(&node_address)
        .await
        .expect("node should be reachable by protocol client");
    // 2. Get identity for id_a
    assert!(
        protocol_client.get_identity(id_a).await.is_ok(),
        "get_identity failed for {id_a}"
    );
    // 3. Get identity for id_b
    assert!(
        protocol_client.get_identity(id_b).await.is_ok(),
        "get_identity failed for {id_b}"
    );
    // 4. Request should fail for non-loaded id
    assert!(
        protocol_client.get_identity("non-loaded").await.is_err(),
        "get_identity should fail for non-loaded"
    );

    // #Case: Ping-pong request
    //
    // 1. Establish connection
    let mut control_client = ControlClient::new(&config.control)
        .await
        .expect("node should be reachable by control client");
    // 2. Make ping-pong request
    assert!(control_client.ping_pong().await.is_ok(), "ping is failed");

    // #Case: Chain-info requests
    //
    // Note: Fresh node can not return meaningful chain info by definition
    //       but still could reply with default representation and corresponding beacon_id.
    //       This simplifies conditions and useful for tests.
    //
    // 1. Establish connection
    let mut public_client = PublicClient::new(&node_address)
        .await
        .expect("node should be reachable by public client");
    // 2. Get ChainInfo for id_a
    let chain_info = public_client.chain_info(id_a).await.unwrap();
    let info_id = chain_info.metadata.unwrap().beacon_id;
    assert_eq!(
        id_a, info_id,
        "mismatch chain_info id, expected: {id_a}, received: {info_id}"
    );
    // 3. Get ChainInfo for id_b
    let chain_info = public_client.chain_info(id_b).await.unwrap();
    let info_id = chain_info.metadata.unwrap().beacon_id;
    assert_eq!(
        id_b, info_id,
        "mismatch chain_info id, expected: {id_b}, received: {info_id}"
    );
    // 4. Request should fail for non-loaded id
    assert!(public_client.chain_info("non-loaded").await.is_err());

    // #Case: Shutdown
    //
    // 1. Shutdown id_a. Daemon should continue to run, there is stil id_b
    let is_daemon_running = control_client.shutdown(Some(id_a)).await.unwrap();
    assert!(is_daemon_running, "daemon should still run");
    // 2. Node should be reachable
    assert!(control_client.ping_pong().await.is_ok(), "ping_pong failed");
    // 3. Shutdown id_b. Daemon should be also shutted down
    let is_daemon_running = control_client
        .shutdown(Some(id_b))
        .await
        .expect("failed to send shutdown request");
    assert!(!is_daemon_running, "failed to shutdown the daemon");
    // 4. Node should not be reachable
    assert!(
        control_client.ping_pong().await.is_err(),
        "node should not be reachable"
    );
}
