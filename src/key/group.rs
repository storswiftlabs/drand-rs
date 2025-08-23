use super::keys::DistPublic;
use super::Hash;

use super::Scheme;
use crate::key::node::Node;
use crate::net::utils::Seconds;
use energon::traits::Affine;
use sha2::Digest;

/// Group holds all information about a group of drand nodes.
#[derive(Debug, Default, PartialEq)]
pub struct Group<S: Scheme> {
    /// Threshold to setup during the DKG or resharing protocol.
    pub threshold: u32,
    /// Period to use for the beacon randomness generation
    pub period: Seconds,
    /// `CatchupPeriod` is a delay to insert while in a catchup mode
    /// also can be thought of as the minimum period allowed between
    /// beacon and subsequent partial generation
    pub catchup_period: Seconds,
    /// Time at which the first round of the chain is mined
    pub genesis_time: u64,
    // In case of a resharing, this is the time at which the network will
    // transition from the old network to the new network.
    pub transition_time: u64,
    /// Seed of the genesis block. When doing a DKG from scratch, it will be
    /// populated directly from the list of nodes and other parameters. When
    /// doing a resharing, this seed is taken from the first group of the network.
    pub genesis_seed: Vec<u8>,
    /// ID is the unique identifier for this group
    pub beacon_id: String,
    /// List of nodes forming this group
    pub nodes: Vec<Node<S>>,
    /// The distributed public key of this group. It is empty - the group has not
    /// ran a DKG protocol yet.
    pub dist_key: DistPublic<S>,
}

impl<S: Scheme> Group<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        threshold: u32,
        period: Seconds,
        catchup_period: Seconds,
        genesis_time: u64,
        transition_time: u64,
        genesis_seed: Vec<u8>,
        beacon_id: String,
        nodes: Vec<Node<S>>,
        dist_key: DistPublic<S>,
    ) -> Self {
        Self {
            threshold,
            period,
            catchup_period,
            genesis_time,
            transition_time,
            genesis_seed,
            beacon_id,
            nodes,
            dist_key,
        }
    }

    pub fn nodes(&self) -> &[Node<S>] {
        &self.nodes
    }
}

impl<S: Scheme> Hash for Group<S> {
    type Hasher = crev_common::Blake2b256;

    fn hash(&self) -> [u8; 32] {
        let mut h = Self::Hasher::new();

        for node in &self.nodes {
            h.update({
                let mut hh = Self::Hasher::new();
                hh.update(node.index().to_le_bytes());
                hh.update(node.public().key().serialize().unwrap());
                hh.finalize().as_slice()
            });
        }

        h.update(self.threshold.to_le_bytes());
        h.update(self.genesis_time.to_le_bytes());

        if self.transition_time != 0 {
            h.update(self.transition_time.to_le_bytes());
        }

        if !self.dist_key.commits().is_empty() {
            h.update({
                let mut hh = Self::Hasher::new();
                for commit in self.dist_key.commits() {
                    let bytes = commit.serialize().unwrap();
                    hh.update(bytes);
                }
                hh.finalize().as_slice()
            });
        }

        if !crate::core::beacon::is_default_beacon_id(&self.beacon_id) {
            h.update(self.beacon_id.as_bytes());
        }

        h.finalize().into()
    }
}

/// Calculates the threshold needed for the group to produce sufficient shares to decode
pub fn minimum_t(n: usize) -> usize {
    (n >> 1) + 1
}
