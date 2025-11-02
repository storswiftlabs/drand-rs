use super::handler::ChainError;
use crate::{
    key::{node::Node, Scheme},
    net::{metrics::GroupMetrics, utils::Address},
    protobuf::drand::PartialBeaconPacket,
};
use energon::{
    drand::traits::BeaconDigest,
    kyber::{
        dkg,
        poly::{PubPoly, PubShare},
        tbls::{self, SigShare, TBlsError},
    },
};

/// Remote node representation per epoch.
pub struct EpochNode<S: Scheme> {
    /// Node address.
    peer: Address,
    /// Node public share is computed by evaluating public sharing polynomial at the node index.
    share: PubShare<S>,
}

impl<S: Scheme> EpochNode<S> {
    pub fn peer(&self) -> &Address {
        &self.peer
    }
}

/// Epoch config is representation of [`dkg::DkgOutput`] where QUAL nodes are mapped into list of [`EpochNode`].
pub struct EpochConfig<S: Scheme> {
    share: dkg::DistKeyShare<S>,
    remote_nodes: Vec<EpochNode<S>>,
    group_metrics: GroupMetrics,
}

impl<S: Scheme> EpochConfig<S> {
    pub fn new(
        nodes: Vec<Node<S>>,
        share: dkg::DistKeyShare<S>,
        group_metrics: GroupMetrics,
    ) -> Self {
        let poly = PubPoly {
            commits: share.commitments().to_vec(),
        };

        let remote_nodes = nodes
            .into_iter()
            // Skip our index.
            .filter(|n| n.index() != share.pri_share.index())
            // Map QUAL into EpochNode.
            .map(|n| EpochNode {
                share: poly.eval(n.index()),
                peer: n.into_peer(),
            })
            .collect();

        Self {
            share,
            remote_nodes,
            group_metrics,
        }
    }

    pub fn sign_partial(&self, msg: &[u8]) -> Result<SigShare<S>, TBlsError> {
        tbls::sign(&self.share.pri_share, msg)
    }

    /// Returns [`SigShare`] with node authority if partial signature is valid.
    pub fn verify_partial(
        &self,
        p: &PartialBeaconPacket,
    ) -> Result<(SigShare<S>, &Address), ChainError> {
        let sig_share = SigShare::deserialize(&p.partial_sig).map_err(ChainError::TBlsError)?;

        let node = self
            .remote_nodes
            .iter()
            .find(|n| n.share.i == sig_share.index())
            .ok_or(ChainError::UnknownIndex(sig_share.index()))?;

        let msg = S::Beacon::digest(&p.previous_signature, p.round);

        if S::bls_verify(&node.share.v, sig_share.value(), &msg).is_err() {
            return Err(ChainError::InvalidPartialSignature);
        }

        Ok((sig_share, node.peer()))
    }

    pub fn nodes(&self) -> &[EpochNode<S>] {
        &self.remote_nodes
    }

    pub fn our_index(&self) -> u32 {
        self.share.pri_share.index()
    }

    pub fn metrics_for_group(&self) -> GroupMetrics {
        self.group_metrics
    }
}
