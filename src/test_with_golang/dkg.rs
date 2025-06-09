//! DKG scenarios can be configured manually e.g [`all_roles_dkg`],
//! or generated randomly within the DKG state machine rules (see: [`NodesGroup::generate_roles`]),
//! which is useful for continuous resharing with different thresholds and participant roles (see: [`random_scenarios`]).
use super::utils::*;
use crate::dkg::status::Status;
use std::time::Duration;
use tokio::time::sleep;

/// DKG participants roles coverage:
/// - Joiner[new group]: epoch 1
/// - Remainer: epoch 2,3
/// - Joiner[existing group]: epoch 3
/// - Leaver: epoch 2[online], epoch 3[offline]
///
/// Groupfiles of joiners and remainers are compared with leader-go groupfile after each reshare.
#[tokio::test]
async fn all_roles_dkg() {
    // Epoch: 1
    // Scenario: all nodes joining
    // Setup: group: 9, thr: 7
    //
    // FOLDER[i]_IMPL  ROLE
    //    node0_GO    Joiner
    //    node1_GO    Joiner
    //    node2_GO    Joiner
    //    node3_GO    Joiner
    //    node4_RS    Joiner
    //    node5_RS    Joiner
    //    node6_RS    Joiner
    //    node7_RS    Joiner
    //    node8_RS    Joiner
    let custom_thr = Some(7);
    let mut group = run_fresh_dkg(9, custom_thr).await;

    // Epoch 2
    // Scenario: 7 remainers, 2 leavers(online)
    // Setup: group: 7, thr: 5
    //
    // FOLDER[i]_IMPL   ROLE
    //    node0_GO    remainer
    //    node1_GO    remainer
    //    node2_GO    remainer
    //    node3_GO    leaver (ON)
    //    node4_RS    remainer
    //    node5_RS    remainer
    //    node6_RS    remainer
    //    node7_RS    remainer
    //    node8_RS    leaver (ON)
    let joiners = &[];
    let remainers = &[0, 1, 2, 4, 5, 6, 7];
    let leavers = &[3, 8];
    let thr = 5;
    group.setup_scenario(joiners, remainers, leavers, thr);
    //
    // Start resharing protocol
    group.leader_generate_proposal().await;
    group.members_proceed_proposal().await;
    group.leader_dkg_execute().await;
    // Sleep:
    // 5 until execution time (protocol)
    // + 3 for fast_sync mode (phase_timeout 10)
    // + 5 (CI/CD)
    sleep(Duration::from_secs(15)).await;
    //
    // Check results
    // Get finished state from leader
    let finished = get_finished_state(&group.nodes[0].control, &group.nodes[0].id).await;
    assert_eq!(finished.epoch, 2);
    assert_eq!(finished.state, Status::Complete as u32);
    // Groupfiles for remainers should be equal with leader groupfile.
    group.assert_groupfiles_with_leader();

    // Epoch 3
    // Scenario: 2 joiners, 5 remainers, 2 leavers(offline)
    // Setup: group 7, thr: 5
    //
    // FOLDER[i]_IMPL   ROLE
    //    node0_GO    remainer
    //    node1_GO    remainer
    //    node2_GO    leaver(OFF)
    //    node3_GO    joiner
    //    node4_RS    remainer
    //    node5_RS    remainer
    //    node6_RS    remainer
    //    node7_RS    leaver(OFF)
    //    node8_RS    joiner
    let joiners = &[3, 8];
    let remainers = &[0, 1, 4, 5, 6];
    let leavers = &[2, 7];
    group.setup_scenario(joiners, remainers, leavers, thr);
    // Shutdown leavers: node2_GO, node7_RS
    group.nodes[2].stop().await;
    group.nodes[7].stop().await;
    // Refresh keypairs for joiners: node3_GO, node8_RS
    group.nodes[3].set_to_fresh().await;
    group.nodes[8].set_to_fresh().await;
    //
    // Start resharing protocol
    group.leader_generate_proposal().await;
    group.members_proceed_proposal().await;
    group.leader_dkg_execute().await;
    // Sleep:
    // 5 until execution time (protocol)
    // + 10 * 3 phases timeouts (protocol)
    // + 5 (CI/CD)
    sleep(Duration::from_secs(40)).await;
    //
    // Check results
    // Get finished state from leader
    let finished = get_finished_state(&group.nodes[0].control, &group.nodes[0].id).await;
    assert_eq!(finished.epoch, 3);
    assert_eq!(finished.state, Status::Complete as u32);
    // Groupfiles for remainers and joiners should be equal with leader groupfile.
    group.assert_groupfiles_with_leader();
    remove_nodes_fs()
}

#[ignore = "example for release build"]
#[tokio::test]
async fn random_scenarios() {
    let log_enabled = true;
    let write_statistic = true;
    // Max group size and number of epochs are arbitrary values.
    // Note: for dynamic scenarios at least 3 nodes required.
    let max_group_size = 14;
    // 55 epochs = ~20 minutes to run
    let epochs = 55;

    let mut group = NodesGroup::generate_nodes(max_group_size).await;
    if log_enabled {
        crate::log::init_log(false).unwrap();
    }
    if write_statistic {
        group.sn.enable_frames();
    }

    group.start_daemons().await;
    sleep(Duration::from_secs(5)).await;

    for _ in 0..epochs {
        group.generate_roles();
        group.leader_generate_proposal().await;
        group.members_proceed_proposal().await;
        group.leader_dkg_execute().await;
        // Sleep:
        // 5 sec - waiting for execution (protocol)
        // 7 sec - DKG(fast sync mode). Note: phase timeout = 10 seconds
        sleep(Duration::from_secs(12)).await;
        group.check_results().await;
    }
}
