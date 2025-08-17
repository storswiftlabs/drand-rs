use crate::key::Scheme;
use crate::net::utils::Seconds;
use crate::protobuf::drand::ChainInfoPacket;
use crate::protobuf::drand::Metadata;

use energon::points::KeyPoint;
use energon::traits::Affine;
use sha2::Digest;
use tracing::error;

/// Public information that is necessary for a client to verify any beacon present in a randomness chain.
#[derive(Default, Clone, PartialEq)]
pub struct ChainInfo<S: Scheme> {
    pub public_key: KeyPoint<S>,
    pub beacon_id: String,
    pub period: Seconds,
    pub genesis_time: u64,
    pub genesis_seed: Vec<u8>,
}

impl<S: Scheme> ChainInfo<S> {
    pub fn from_packet(packet: &ChainInfoPacket, id: String) -> Option<Self> {
        if S::ID != packet.scheme_id {
            error!(
                "ChainInfo: [{id}]: scheme expected {}, received {}",
                S::ID,
                packet.scheme_id
            );
            return None;
        }

        let Ok(public_key) = Affine::deserialize(&packet.public_key) else {
            error!(
                "ChainInfo: [{id}]: failed to deserialize group key: {}\nscheme: {}",
                hex::encode(&packet.public_key),
                S::ID
            );
            return None;
        };

        #[allow(clippy::cast_sign_loss, reason = "checked")]
        let genesis_time = if packet.genesis_time > 0 {
            packet.genesis_time as u64
        } else {
            error!(
                "ChainInfo: [{id}]: invalid genesis time: {}",
                packet.genesis_time
            );
            return None;
        };

        let info = Self {
            public_key,
            beacon_id: id,
            period: Seconds::new(packet.period),
            genesis_time,
            genesis_seed: packet.group_hash.clone(),
        };

        Some(info)
    }

    pub fn as_packet(&self) -> Option<ChainInfoPacket> {
        let public_key = Affine::serialize(&self.public_key).ok()?.into();
        let hash: Vec<u8> = self.hash()?.into();
        let genesis_time = i64::try_from(self.genesis_time).ok()?;

        let info = ChainInfoPacket {
            public_key,
            period: self.period.get_value(),
            genesis_time,
            hash: hash.clone(),
            group_hash: self.genesis_seed.clone(),
            scheme_id: S::ID.to_string(),
            metadata: Some(Metadata {
                node_version: None,
                beacon_id: self.beacon_id.to_string(),
                chain_hash: hash,
            }),
        };

        Some(info)
    }

    pub fn hash(&self) -> Option<[u8; 32]> {
        let pk_bytes = self.public_key.serialize().ok()?;

        let mut h = sha2::Sha256::new();
        h.update(self.period.get_value().to_be_bytes());
        h.update(self.genesis_time.to_be_bytes());
        h.update(&pk_bytes);
        h.update(&self.genesis_seed);
        if !crate::core::beacon::is_default_beacon_id(&self.beacon_id) {
            h.update(self.beacon_id.as_bytes());
        }

        Some(h.finalize().into())
    }
}

/// Returns canonical hash of protobuf encoded info packet for given beacon ID.
pub fn hash_packet(proto: &ChainInfoPacket, beacon_id: &str) -> [u8; 32] {
    let mut h = sha2::Sha256::new();
    h.update(proto.period.to_be_bytes());
    h.update(proto.genesis_time.to_be_bytes());
    h.update(&proto.public_key);
    h.update(&proto.group_hash);

    if !crate::core::beacon::is_default_beacon_id(beacon_id) {
        h.update(beacon_id.as_bytes());
    }
    h.finalize().into()
}
