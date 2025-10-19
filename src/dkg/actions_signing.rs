use super::state::State;
use super::ActionsError;

use crate::core::beacon::BeaconProcess;
use crate::key::Scheme;

use crate::protobuf::dkg::AbortDkg;
use crate::transport::dkg::AcceptProposal;
use crate::transport::dkg::GossipData;
use crate::transport::dkg::GossipPacket;
use crate::transport::dkg::Participant;
use crate::transport::dkg::ProposalTerms;
use crate::transport::dkg::RejectProposal;
use crate::transport::dkg::StartExecution;

use energon::traits::Affine;
use prost_types::Timestamp;
use std::future::Future;
use tracing::debug;

/// Contains logic for signing and validation packets
pub(super) trait ActionsSigning {
    type Scheme: Scheme;

    fn verify_msg(
        &self,
        packet: &GossipPacket,
        state: &State<Self::Scheme>,
    ) -> impl Future<Output = Result<(), ActionsError>>;

    fn msg_for_signing(&self, packet: &GossipPacket, enc_state: &[u8]) -> Vec<u8> {
        let mut msg = packet.encode();
        msg.extend_from_slice(enc_state);

        msg
    }
}

/// Helper trait to encode data for DKG command validation  
pub(super) trait GossipAuth {
    fn encode(&self) -> Vec<u8>;
}

impl<S: Scheme> ActionsSigning for BeaconProcess<S> {
    type Scheme = S;

    async fn verify_msg(&self, gp: &GossipPacket, state: &State<S>) -> Result<(), ActionsError> {
        debug!(parent: self.log(), "Verifying gossip packet with beaconID: {}, from: {}", 
               gp.metadata.beacon_id, gp.metadata.address, );

        // Find the participant signature is allegedly from.
        // Return error if participant is not found in `remaining` or `joining`.
        if let Some(participant) = state
            .joining
            .iter()
            .find(|p| p.address == gp.metadata.address)
            .or_else(|| {
                state
                    .remaining
                    .iter()
                    .find(|p| p.address == gp.metadata.address)
            })
        {
            // Verify signature
            {
                let msg = self.msg_for_signing(gp, &state.encode());
                if is_valid_signature::<S>(&participant.key, &gp.metadata.signature, &msg) {
                    Ok(())
                } else {
                    Err(ActionsError::InvalidSignature)
                }
            }
        } else {
            Err(ActionsError::MissingParticipant)
        }
    }
}

fn is_valid_signature<S: Scheme>(key: &[u8], sig: &[u8], msg: &[u8]) -> bool {
    if let Ok(key) = Affine::deserialize(key) {
        if let Ok(sig) = Affine::deserialize(sig) {
            return S::bls_verify(&key, &sig, msg).is_ok();
        };
    }
    false
}

/// Implementation for UTC 0, aligned to <https://pkg.go.dev/time#Time.MarshalBinary> [go 1.22.10]
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
pub(super) fn enc_timestamp(t: Timestamp) -> [u8; 15] {
    // Add delta(year 1, unix epoch)
    let sec = t.seconds + 62135596800;
    let nsec = t.nanos;

    [
        1, // timeBinaryVersionV1
        (sec >> 56) as u8,
        (sec >> 48) as u8,
        (sec >> 40) as u8,
        (sec >> 32) as u8,
        (sec >> 24) as u8,
        (sec >> 16) as u8,
        (sec >> 8) as u8,
        (sec) as u8,
        (nsec >> 24) as u8,
        (nsec >> 16) as u8,
        (nsec >> 8) as u8,
        (nsec) as u8,
        // zone offset
        0xFF,
        0xFF,
    ]
}

impl GossipAuth for GossipPacket {
    fn encode(&self) -> Vec<u8> {
        [
            "beaconID:".as_bytes(),
            // Using the `beacon_id` from `packet.metadata` is safe due to earlier authentication, see `MultiBeacon::cmd`.
            self.metadata.beacon_id.as_bytes(),
            "\n".as_bytes(),
            &match &self.data {
                GossipData::Proposal(terms) => terms.encode(),
                GossipData::Accept(accept) => accept.encode(),
                GossipData::Reject(reject) => reject.encode(),
                GossipData::Abort(abort) => abort.encode(),
                GossipData::Execute(execute) => execute.encode(),
                GossipData::Dkg(..) => "Gossip packet".as_bytes().to_vec(),
            },
        ]
        .concat()
    }
}

impl GossipAuth for StartExecution {
    fn encode(&self) -> Vec<u8> {
        ["Execute:".as_bytes(), &enc_timestamp(self.time)].concat()
    }
}

impl GossipAuth for AbortDkg {
    fn encode(&self) -> Vec<u8> {
        [
            "Aborted:".as_bytes(),
            self.reason.as_bytes(),
            "\n".as_bytes(),
        ]
        .concat()
    }
}

impl GossipAuth for AcceptProposal {
    fn encode(&self) -> Vec<u8> {
        [
            "Accepted:".as_bytes(),
            self.acceptor.address.as_str().as_bytes(),
            "\n".as_bytes(),
        ]
        .concat()
    }
}

impl GossipAuth for RejectProposal {
    fn encode(&self) -> Vec<u8> {
        [
            "Rejected:".as_bytes(),
            self.rejector.address.as_str().as_bytes(),
            "\n".as_bytes(),
        ]
        .concat()
    }
}

impl GossipAuth for ProposalTerms {
    fn encode(&self) -> Vec<u8> {
        [
            "Proposal:".as_bytes(),
            self.beacon_id.as_bytes(),
            "\n".as_bytes(),
            &self.epoch.to_le_bytes(),
            "\nLeader:".as_bytes(),
            self.leader.address.as_str().as_bytes(),
            "\n".as_bytes(),
            &self.leader.signature,
        ]
        .concat()
    }
}

pub(super) fn enc_participant(role: &str, identity: &Participant) -> Vec<u8> {
    [
        role.as_bytes(),
        identity.address.as_str().as_bytes(),
        "\nSig:".as_bytes(),
        &identity.signature,
    ]
    .concat()
}

#[test]
fn enc_timeout() {
    // Test data from runtime https://github.com/drand/drand/blob/v2.1.0/internal/dkg/actions_signing.go#L130
    struct Vector {
        plain: Timestamp,
        enc: &'static str,
    }

    let vectors = [
        Vector {
            plain: Timestamp {
                seconds: 1741744764,
                nanos: 594379669,
            },
            enc: "010000000edf62e17c236d8395ffff",
        },
        Vector {
            plain: Timestamp {
                seconds: 1741745624,
                nanos: 86180161,
            },
            enc: "010000000edf62e4d805230141ffff",
        },
        Vector {
            plain: Timestamp {
                seconds: 1741745738,
                nanos: 467268517,
            },
            enc: "010000000edf62e54a1bd9f3a5ffff",
        },
        Vector {
            plain: Timestamp {
                seconds: 1741745868,
                nanos: 483295650,
            },
            enc: "010000000edf62e5cc1cce81a2ffff",
        },
        Vector {
            plain: Timestamp {
                seconds: 1741745941,
                nanos: 752070485,
            },
            enc: "010000000edf62e6152cd3af55ffff",
        },
        Vector {
            plain: Timestamp {
                seconds: 1741746194,
                nanos: 656876372,
            },
            enc: "010000000edf62e71227272354ffff",
        },
    ];

    vectors
        .iter()
        .for_each(|v| assert!(enc_timestamp(v.plain) == *hex::decode(v.enc).unwrap()));
}
