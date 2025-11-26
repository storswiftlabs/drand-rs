use crate::{
    core::beacon::BeaconID,
    key::Scheme,
    net::utils::Seconds,
    protobuf::drand::{ChainInfoPacket, Metadata},
};
use energon::{points::KeyPoint, traits::Affine};
use sha2::Digest;

/// Public information that is necessary for a client
/// to verify any beacon present in a randomness chain.
#[derive(Default, Clone, PartialEq)]
pub struct ChainInfo<S: Scheme> {
    pub public_key: KeyPoint<S>,
    pub beacon_id: BeaconID,
    pub period: Seconds,
    pub genesis_time: u64,
    pub genesis_seed: Vec<u8>,
}

#[derive(thiserror::Error, Debug)]
#[error("chain_info: {}")]
pub enum ChainInfoError {
    #[error("received scheme should match the scheme of beacon process")]
    Scheme,
    #[error("failed to deserialize group key")]
    PublicKey,
    #[error("genesis time cannot be zero")]
    GenesisTime,
}

impl<S: Scheme> ChainInfo<S> {
    pub fn from_packet(packet: &ChainInfoPacket, id: BeaconID) -> Result<Self, ChainInfoError> {
        if S::ID != packet.scheme_id {
            return Err(ChainInfoError::Scheme);
        }

        let Ok(public_key) = Affine::deserialize(&packet.public_key) else {
            return Err(ChainInfoError::PublicKey);
        };

        let genesis_time = check_genesis_time(packet.genesis_time)?;
        let info = Self {
            public_key,
            beacon_id: id,
            period: Seconds::new(packet.period),
            genesis_time,
            genesis_seed: packet.group_hash.clone(),
        };

        Ok(info)
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
        if !BeaconID::is_default(self.beacon_id.as_str()) {
            h.update(self.beacon_id.as_bytes());
        }

        Some(h.finalize().into())
    }
}

/// Returns canonical hash of protobuf encoded info packet for given beacon ID.
pub fn hash_packet(proto: &ChainInfoPacket, id: BeaconID) -> [u8; 32] {
    let mut h = sha2::Sha256::new();
    h.update(proto.period.to_be_bytes());
    h.update(proto.genesis_time.to_be_bytes());
    h.update(&proto.public_key);
    h.update(&proto.group_hash);

    if !BeaconID::is_default(id.as_str()) {
        h.update(id.as_bytes());
    }
    h.finalize().into()
}

/// Returns `None` if genesis time is equal or less then zero.
fn check_genesis_time(genesis_time: i64) -> Result<u64, ChainInfoError> {
    if genesis_time > 0 {
        #[allow(clippy::cast_sign_loss, reason = "checked")]
        Ok(genesis_time as u64)
    } else {
        Err(ChainInfoError::GenesisTime)
    }
}
