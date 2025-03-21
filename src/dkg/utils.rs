use crate::key::KeyPoint;
use crate::key::Scheme;
use crate::key::SigPoint;

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

/// Implementation for UTC 0, aligned to https://pkg.go.dev/time#Time.MarshalBinary [go 1.22.10]
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

/// Helper trait to encode data for DKG command validation  
pub(super) trait GossipAuth {
    fn encode(&self) -> Vec<u8>;
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
        ["Aborted:".as_bytes(), self.reason.as_bytes()].concat()
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
