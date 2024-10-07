use super::keys::Identity;
use super::keys::PublicKey;
use super::node::Node;
use crate::core::beacon::BeaconID;
use anyhow::bail;
use energon::drand::Scheme;
use sha2::Digest;
use sha2::Sha256;

use anyhow::Result;

#[derive(PartialEq)]
pub struct Group<S: Scheme> {
    pub nodes: Vec<Node<S>>,
    pub threshold: u32,
    /// period in seconds
    pub period: u32,
    pub genesis_time: u64,
    pub transition_time: u64,
    pub genesis_seed: Vec<u8>,
    pub dist_key: Vec<PublicKey<S>>,
    /// catchup_period in seconds
    pub catchup_period: u32,
    pub beacon_id: BeaconID,
}

impl<S: Scheme> Group<S> {
    pub fn new(
        nodes: Vec<Node<S>>,
        threshold: u32,
        period: u32,
        genesis_time: u64,
        transition_time: u64,
        genesis_seed: Vec<u8>,
        dist_key: Vec<PublicKey<S>>,
        catchup_period: u32,
        beacon_id: BeaconID,
    ) -> Self {
        Self {
            nodes,
            threshold,
            period,
            genesis_time,
            transition_time,
            genesis_seed,
            dist_key,
            catchup_period,
            beacon_id,
        }
    }

    pub fn get_nonce(&self) -> Vec<u8> {
        let mut hasher = Sha256::new();
        hasher.update(self.genesis_time.to_be_bytes());
        hasher.finalize().to_vec()
    }

    pub fn find_index(&self, identity: &Identity<S>) -> Option<u32> {
        self.nodes
            .iter()
            .find(|node| node.identity() == identity)
            .map(|node| node.index())
    }

    pub fn get_key(&self, index: u32) -> Result<&PublicKey<S>> {
        match self.nodes.iter().find(|node| node.index() == index) {
            Some(node) => Ok(node.identity().key()),
            None => bail!("public key not found for index {index}"),
        }
    }

    pub fn get_uris(&self, index: u32) -> Vec<&str> {
        self.nodes
            .iter()
            .filter(|node| node.index() != index)
            .map(|node| node.identity().address())
            .collect::<Vec<&str>>()
    }
}
