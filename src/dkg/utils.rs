use super::{broadcast::Convert, ActionsError};
use crate::{
    key::{KeyPoint, Scheme, SigPoint},
    protobuf::dkg::{packet::Bundle as ProtoBundle, DkgPacket},
    transport::dkg::{GossipPacket, Participant},
};
use energon::{
    kyber::dkg::{Bundle, BundleSender},
    traits::Affine,
};
use std::collections::HashSet;
use tabled::{settings::Style, Table, Tabled};
use tracing::{debug, trace, Span};

const SHORT_SIG_BYTES: usize = 3;

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

    /// The channel exist only within DKG execution stage, see [`super::execution::ExecuteDkg`].
    pub fn open_gate(&mut self, tx: BundleSender<S>) -> Result<(), ActionsError> {
        if self.bundle_sender.is_some() {
            Err(ActionsError::ProtocolAlreadyRunning)
        } else {
            self.bundle_sender = Some(tx);

            Ok(())
        }
    }

    /// Resets keeper into empty state.
    pub fn set_empty(&mut self) {
        self.seen_gossip.clear();
        self.bundle_sender = None;
    }

    /// Returns `true` if gossip packet is not seen and its signature is not less than [`SHORT_SIG_LEN`].
    pub fn is_new_packet(&mut self, p: &GossipPacket) -> bool {
        let mut is_new = false;

        if let Some(short_sig) = p.metadata.signature.get(..SHORT_SIG_BYTES) {
            let sig_hex = hex::encode(short_sig);
            if self.seen_gossip.contains(&sig_hex) {
                trace!(parent: &self.log, "gatekeeper: ignoring duplicate gossip packet, type: {} sig: {sig_hex}, from: {}", p.data, p.metadata.address);
            } else {
                debug!(parent: &self.log, "gatekeeper: processing DKG gossip packet, type: {}, sig: {sig_hex}, id: {}, allegedly from: {}",
                      p.data, p.metadata.beacon_id, p.metadata.address);
                is_new = self.seen_gossip.insert(sig_hex);
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

/// Row for DKG status table.
#[derive(Tabled)]
struct Row {
    #[tabled(rename = "FIELD")]
    field: String,
    #[tabled(rename = "CURRENT")]
    current: String,
    #[tabled(rename = "FINISHED")]
    finished: String,
}

#[rustfmt::skip]
pub fn print_dkg_status(responce: crate::transport::dkg::DkgStatusResponse) {
    let c = responce.current;
    let f = responce.finished.unwrap_or_default();

    let mut table = Table::new( [
        Row { field: "Status".into(),      current: c.state,        finished: f.state },
        Row { field: "Epoch".into(),       current: c.epoch,        finished: f.epoch },
        Row { field: "BeaconID".into(),    current: c.beacon_id,    finished: f.beacon_id },
        Row { field: "Threshold".into(),   current: c.threshold,    finished: f.threshold },
        Row { field: "Timeout".into(),     current: c.timeout,      finished: f.timeout },
        Row { field: "GenesisTime".into(), current: c.genesis_time, finished: f.genesis_time },
        Row { field: "GenesisSeed".into(), current: c.genesis_seed, finished: f.genesis_seed },
        Row { field: "Leader".into(),      current: c.leader,       finished: f.leader },
        Row { field: "Joining".into(),     current: c.joining,      finished: f.joining },
        Row { field: "Remaining".into(),   current: c.remaining,    finished: f.remaining },
        Row { field: "Leaving".into(),     current: c.leaving,      finished: f.leaving },
        Row { field: "Accepted".into(),    current: c.acceptors,    finished: f.acceptors },
        Row { field: "Rejected".into(),    current: c.rejectors,    finished: f.rejectors },
        Row { field: "FinalGroup".into(),  current: c.final_group,  finished: f.final_group },
    ]);

    table.with(Style::sharp());

    println!("{table}");
}
