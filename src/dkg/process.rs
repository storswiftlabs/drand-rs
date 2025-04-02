use super::ActionsError;
use crate::transport::dkg::GossipPacket;

use std::collections::HashSet;
use tracing::debug;

/// Contains signatures of seen packets.
#[derive(Default)]
pub struct SeenPackets {
    seen: HashSet<String>,
}
/// Short signature of [`GossipMetadata`]
pub const SHORT_SIG_LEN: usize = 8;

impl SeenPackets {
    pub(super) fn is_new_packet(&mut self, p: &GossipPacket) -> Result<bool, ActionsError> {
        let sig_hex = hex::encode(&p.metadata.signature);

        match sig_hex.get(..SHORT_SIG_LEN) {
            Some(short_sig) => {
                if self.seen.contains(&sig_hex) {
                    debug!(
                        "ignoring duplicate gossip packet, type: {} sig: {short_sig}",
                        p.data
                    );

                    Ok(false)
                } else {
                    tracing::debug!("processing DKG gossip packet, type: {}, sig: {short_sig}, id: {}, allegedly from: {}",
                        p.data, p.metadata.beacon_id, p.metadata.address);

                    // always true
                    Ok(self.seen.insert(sig_hex))
                }
            }
            None => Err(ActionsError::GossipSignatureLen),
        }
    }
}
