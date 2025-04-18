use super::broadcast::Convert;
use super::ActionsError;

use crate::key::KeyPoint;
use crate::key::Scheme;
use crate::key::SigPoint;
use crate::protobuf::dkg::packet::Bundle as ProtoBundle;
use crate::protobuf::dkg::DkgPacket;
use crate::transport::dkg::GossipPacket;
use crate::transport::dkg::Participant;

use energon::kyber::dkg::protocol::Bundle;
use energon::kyber::dkg::BundleReceiver;
use energon::kyber::dkg::BundleSender;
use energon::traits::Affine;

use std::collections::HashSet;
use tokio::sync::mpsc;
use tracing::debug;
use tracing::Span;

pub const SHORT_SIG_LEN: usize = 8;

impl Participant {
    pub fn is_valid_signature<S: Scheme>(&self) -> bool {
        use crev_common::Blake2b256;
        use sha2::Digest;

        if let Ok(key) = KeyPoint::<S>::deserialize(&self.key) {
            if let Ok(signature) = SigPoint::<S>::deserialize(&self.signature) {
                let mut hasher = Blake2b256::new();
                hasher.update(&self.key);
                let msg = [S::ID.as_bytes(), hasher.finalize().as_slice()].concat();

                if S::bls_verify(&key, &signature, &msg).is_ok() {
                    return true;
                }
            }
        }

        false
    }
}

pub struct GateKeeper<S: Scheme> {
    seen_gossip: HashSet<String>,
    bundle_sender: Option<BundleSender<S>>,
    log: Span,
}

impl<S: Scheme> GateKeeper<S> {
    pub fn new(log: &Span) -> Self {
        Self {
            seen_gossip: HashSet::new(),
            bundle_sender: None,
            log: log.to_owned(),
        }
    }

    /// The channel exist only within DKG execution stage, see [super::execution::ExecuteDkg]
    pub fn create_channel(&mut self) -> Result<BundleReceiver<S>, ActionsError> {
        if self.bundle_sender.is_some() {
            Err(ActionsError::ProtocolAlreadyRunning)
        } else {
            let (tx, rx) = mpsc::channel(1);
            self.bundle_sender = Some(tx);

            Ok(rx)
        }
    }

    /// Resets keeper into empty state.
    pub fn set_empty(&mut self) {
        self.seen_gossip.clear();

        assert!(
            self.bundle_sender.is_some(),
            "gate keeper: sender is missing"
        );
        self.bundle_sender = None
    }

    /// Returns `true` if gossip packet is not seen and its signature is not less than [SHORT_SIG_LEN].
    pub fn is_new_packet(&mut self, p: &GossipPacket) -> bool {
        let sig_hex = hex::encode(&p.metadata.signature);
        let mut is_new = false;

        if let Some(short_sig) = sig_hex.get(..SHORT_SIG_LEN) {
            if self.seen_gossip.contains(&sig_hex) {
                debug!(parent: &self.log, "gatekeeper: ignoring duplicate gossip packet, type: {} sig: {short_sig}", p.data);
            } else {
                debug!(parent: &self.log, "gatekeeper: processing DKG gossip packet, type: {}, sig: {short_sig}, id: {}, allegedly from: {}",
                      p.data, p.metadata.beacon_id, p.metadata.address);
                is_new = self.seen_gossip.insert(sig_hex)
            }
        } else {
            tracing::warn!(parent: &self.log, "gatekeeper: ignoring gossip packet with too short signature, allegedly from: {}", p.metadata.address);
        }

        is_new
    }

    pub async fn broadcast(&mut self, proto: DkgPacket) -> Result<(), ActionsError> {
        if let Some(ref tx) = &self.bundle_sender {
            let bundle = bundle_from_proto(proto).ok_or(ActionsError::InvalidProtoBundle)?;
            tx.send(bundle)
                .await
                .map_err(|_| ActionsError::ProtocolIsNotRunning)
        } else {
            Err(ActionsError::ProtocolIsNotRunning)
        }
    }
}

fn bundle_from_proto<S: Scheme>(proto: DkgPacket) -> Option<Bundle<S>> {
    let bundle = match proto.dkg.and_then(|packet| packet.bundle)? {
        ProtoBundle::Deal(d) => Bundle::Deal(Convert::from_proto(d).ok()?),
        ProtoBundle::Response(r) => Bundle::Response(Convert::from_proto(r).ok()?),
        ProtoBundle::Justification(j) => Bundle::Justification(Convert::from_proto(j).ok()?),
    };

    Some(bundle)
}
