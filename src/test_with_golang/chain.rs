// Copyright 2023-2025 StorSwift Inc.
// SPDX-License-Identifier: Apache-2.0

use super::utils::*;
use crate::{
    chain::time::*,
    key::{group::Group, toml::Toml},
    net::{control::ControlClient, public::PublicClient, utils::Address},
    protobuf::drand::ChainInfoPacket,
};
use energon::drand::{
    schemes::{BN254UnchainedOnG1Scheme, DefaultScheme, SigsOnG1Scheme},
    traits::DrandScheme,
};
use std::time::Duration;
use tokio::time::sleep;

/// Coverage:
///  - node-rs following node-go
///  - node-rs following node-rs
///
/// FOLDER[i]_IMPL   ROLE
///      ---- Epoch 0 ----
///     node0_GO     joiner
///     node1_RS     joiner
///      ---- Epoch 1 ----
///     node2_RS     follow node0_GO
///     node3_RS     follow node1_RS
#[tokio::test]
#[rustfmt::skip]
async fn follow_chain() {
    let config = GroupConfig {
        period: 3,
        catchup_period: 1,
        genesis_delay: "10s".into(),
        scheme: std::env::var("DRAND_TEST_SCHEME").unwrap(),
        ..Default::default()
    };

    let nodes_total = 4;
    let nodes_go = 1;
    let mut group = NodesGroup::generate_nodes(nodes_total, config.clone(), Some(nodes_go)).await;
    group.start_daemons();
    sleep(Duration::from_secs(10)).await;

    // Setup DKG scenario.
    let thr = 2;
    let joiners = &[0, 1];
    let remainers = &[];
    let leavers = &[];
    group.setup_scenario(joiners, remainers, leavers, thr);

    // Start DKG protocol.
    group.leader_generate_proposal().await;
    group.members_proceed_proposal().await;
    group.leader_dkg_execute().await;
    sleep(Duration::from_secs(20)).await;

    // Chain info from [node0_GO, node1_RS] should be equal.
    let mut info_go = get_chain_info(&group.nodes[0].private_listen, &config.id).await;
    let mut info_rs = get_chain_info(&group.nodes[1].private_listen, &config.id).await;
    assert_eq_chain_info(&mut info_go, &mut info_rs);

    // Follow up to 7 rounds.
    let up_to = 4;
    let hash = hex::encode(info_rs.hash);
    // Let chain grown to the round 7.
    sleep(Duration::from_secs(10)).await;

    // Set sync nodes to follow:
    // - node2_RS follows node0_GO
    // - node3_RS follows node1_RS
    let sync_node2 = vec![group.nodes[0].private_listen.clone()];
    let sync_node3 = vec![group.nodes[1].private_listen.clone()];
    
    group.nodes[2].follow(&config.id, &hash, sync_node2, up_to, false);
    group.nodes[3].follow(&config.id, &hash, sync_node3, up_to, false);
    sleep(Duration::from_secs(5)).await;

    // Get latest stored round from [node2_RS, node3_RS].
    let mut conn2 = ControlClient::new(&group.nodes[2].control).await.unwrap();
    let mut conn3 = ControlClient::new(&group.nodes[3].control).await.unwrap();
    
    assert!(up_to == conn2.status(config.id.clone()).await.unwrap().latest_stored_round);
    assert!(up_to == conn3.status(config.id).await.unwrap().latest_stored_round);
    
    group.stop_all().await;
    remove_nodes_fs();
}

/// Coverage: 
///  - RS node is syncing after joined group at epoch 2.
///  - Chain is not halted: group size = threshold. 
/// 
/// FOLDER[i]_IMPL   DKG Epoch 1   DKG Epoch 2          Epoch 2[thr:3]    
///     node0_GO      joiner        remainer                        |
///     node1_RS      joiner        remainer                        |
///     node2_RS        --          joiner(no follow)   chain is halted if sync is failed.
#[tokio::test]
#[rustfmt::skip]
async fn sync() {
    let config = GroupConfig {
        period: 3,
        catchup_period: 1,
        genesis_delay: "10s".into(),
        scheme: std::env::var("DRAND_TEST_SCHEME").unwrap(),
        ..Default::default()
    };

    let nodes_total = 3;
    let nodes_go = 1;
    let mut group = NodesGroup::generate_nodes(nodes_total, config.clone(), Some(nodes_go)).await;
    group.start_daemons();
    sleep(Duration::from_secs(5)).await;

    // Setup DKG scenario epoch 1.
    let thr = 2;
    let joiners = &[0, 1];
    let remainers = &[];
    let leavers = &[];
    group.setup_scenario(joiners, remainers, leavers, thr);

    // Start DKG protocol for epoch 1.
    group.leader_generate_proposal().await;
    group.members_proceed_proposal().await;
    group.leader_dkg_execute().await;
    sleep(Duration::from_secs(30)).await;
    
    // Setup DKG scenario for epoch 2.
    let thr = 3;
    let joiners = &[2];
    let remainers = &[0,1];
    let leavers = &[];
    group.setup_scenario(joiners, remainers, leavers, thr);

    // Start DKG protocol for epoch 2.
    group.leader_generate_proposal().await;
    group.members_proceed_proposal().await;
    group.leader_dkg_execute().await;
    sleep(Duration::from_secs(30)).await;

    // Get transition time from node2_RS groupfile.
    let group_str = async_std::fs::read_to_string(&group.nodes[2].groupfile_path).await.unwrap();
    let (transition_time, genesis_time)=match config.scheme.as_str(){
        SigsOnG1Scheme::ID => {
            let group:Group<SigsOnG1Scheme> = Toml::toml_decode(&group_str.parse().unwrap()).unwrap();
            (group.transition_time, group.genesis_time)
        },
        DefaultScheme::ID => {
            let group:Group<DefaultScheme> = Toml::toml_decode(&group_str.parse().unwrap()).unwrap();
            (group.transition_time, group.genesis_time)
        }
        BN254UnchainedOnG1Scheme::ID => {
            let group:Group<BN254UnchainedOnG1Scheme> = Toml::toml_decode(&group_str.parse().unwrap()).unwrap();
            (group.transition_time, group.genesis_time)
        }
        _=> panic!("unknown scheme from group file")
    };

    // Sleep until transition time + 1 period.
    sleep(Duration::from_secs(transition_time - time_now().as_secs() + u64::from(config.period))).await;

    // Sleep until next round + 1 sec.
    let now=time_now().as_secs();
    let (next_round, next_time) = next_round(now, config.period.into(), genesis_time);
    sleep(Duration::from_secs(next_time - now + 1)).await;

    // Latest stored round should match next_round.
    let mut conn = ControlClient::new(&group.nodes[2].control).await.unwrap();
    let latest= conn.status(config.id).await.unwrap().latest_stored_round;
    assert!(latest == next_round);

    group.stop_all().await;
    remove_nodes_fs();

}

async fn get_chain_info(addr: &str, id: &str) -> ChainInfoPacket {
    let addr = Address::precheck(addr).unwrap();
    let mut client = PublicClient::new(&addr).await.unwrap();
    client.chain_info(id.to_string()).await.unwrap()
}

/// Panics if packets are not same, `NodeVersion` is not checked.
fn assert_eq_chain_info(info1: &mut ChainInfoPacket, info2: &mut ChainInfoPacket) {
    let m1 = info1.metadata.take().unwrap();
    let m2 = info2.metadata.take().unwrap();

    assert!((info1 == info2), "info1 != info2");
    assert!(
        (m1.beacon_id == m2.beacon_id && m1.chain_hash == m2.chain_hash),
        "info1.meta != info2.meta"
    );

    info1.metadata = Some(m1);
    info2.metadata = Some(m2);
}
