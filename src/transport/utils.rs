use crate::dkg::state_machine::status::StateError;
use crate::net::utils::InvalidAddress;

use tonic::Status;

/// Trait for conversion from protobuf.
///
/// Abstracts validation boundary for *inner non-generic types*.
pub trait ConvertProto: Sized {
    type Inner;

    /// Returns inner type if data is valid.
    fn validate(self) -> Result<Self::Inner, TransportError>;
}

/// Helper trait to handle optional fields related to prost implementation
///  <https://github.com/tokio-rs/prost?tab=readme-ov-file#field-modifiers>.
pub(super) trait RequireSome<T> {
    /// Returns error [`TransportError::DataIsMissing`] if `Option<T>` is `None`
    ///
    /// Used to bypass <https://github.com/tokio-rs/prost/issues/521>
    fn require_some(self) -> Result<T, TransportError>;
}

impl<T> RequireSome<T> for Option<T> {
    #[inline(always)]
    fn require_some(self) -> Result<T, TransportError> {
        self.ok_or_else(|| TransportError::DataIsMissing(core::any::type_name::<T>().into()))
    }
}

#[inline(always)]
pub(super) fn from_vec<I, T, U>(data: I) -> Vec<U>
where
    I: IntoIterator<Item = T>,
    T: Into<U>,
{
    data.into_iter().map(Into::into).collect()
}

#[inline(always)]
pub(super) fn try_from_vec<I, T, U>(data: I) -> Result<Vec<U>, TransportError>
where
    I: IntoIterator<Item = T>,
    T: ConvertProto<Inner = U>,
{
    data.into_iter().map(ConvertProto::validate).collect()
}

#[derive(thiserror::Error, Debug)]
#[error("transport error: {0}")]
pub enum TransportError {
    #[error(transparent)]
    InvalidAddress(#[from] InvalidAddress),
    #[error(transparent)]
    DkgState(#[from] StateError),
    #[error("data is missing: {0}")]
    DataIsMissing(String),
}

impl From<TransportError> for Status {
    fn from(value: TransportError) -> Self {
        match value {
            TransportError::DataIsMissing(msg) => Status::data_loss(msg),
            _ => Status::invalid_argument(value.to_string()),
        }
    }
}

mod proto_impl {
    use super::super::drand::GroupPacket;
    use crate::net::utils::Address;
    use crate::net::utils::Seconds;
    use crate::protobuf::drand::ChainInfoPacket;
    use crate::protobuf::drand::Metadata;
    use crate::transport::dkg::Bundle;
    use crate::transport::dkg::Command;
    use crate::transport::dkg::GossipData;
    use crate::transport::drand::Identity;
    use crate::transport::drand::Node;

    use crev_common::blake2b256;
    use sha2::Digest;
    use sha2::Sha256;
    use std::fmt;
    use std::fmt::Display;
    use std::str::FromStr;
    use toml_edit::DocumentMut;
    use toml_edit::Table;
    use tracing::error;

    impl Identity {
        fn from_toml(table: &Table) -> Option<Self> {
            let address = Address::precheck(table.get("Address")?.as_str()?).ok()?;
            let key = hex::decode(table.get("Key")?.as_str()?).ok()?;
            let signature = hex::decode(table.get("Signature")?.as_str()?).ok()?;
            Some(Self {
                address,
                key,
                signature,
            })
        }
    }

    impl Node {
        fn from_toml(table: &Table) -> Option<Self> {
            let index = table.get("Index")?.as_integer()? as u32;
            let public = Identity::from_toml(table)?;

            Some(Self { public, index })
        }
    }

    impl GroupPacket {
        pub fn from_toml(group_toml: &str) -> Option<Self> {
            let table: DocumentMut = group_toml.parse().ok()?;
            let scheme_id = table.get("SchemeID")?.as_str()?.into();
            let threshold = table.get("Threshold")?.as_integer()? as u32;
            let period = table.get("Period")?.as_str().map(Seconds::from_str)?.ok()?;
            let genesis_time = table.get("GenesisTime")?.as_integer()? as u64;
            let transition_time = table.get("TransitionTime")?.as_integer()? as u64;
            let genesis_seed = table.get("GenesisSeed")?.as_str().map(hex::decode)?.ok()?;
            let beacon_id = table.get("ID")?.as_str()?.into();

            let catchup_period = table
                .get("CatchupPeriod")?
                .as_str()
                .map(Seconds::from_str)?
                .ok()?;

            let nodes = table
                .get("Nodes")?
                .as_array_of_tables()?
                .iter()
                .map(Node::from_toml)
                .collect::<Option<Vec<_>>>()?;

            let dist_coeffs = table
                .get("PublicKey")?
                .as_table()?
                .get("Coefficients")?
                .as_array()?;
            let mut dist_key = Vec::with_capacity(dist_coeffs.len());
            for commit in dist_coeffs.iter() {
                dist_key.push(hex::decode(commit.as_str()?).ok()?);
            }

            Some(Self {
                nodes,
                threshold,
                period,
                genesis_time,
                transition_time,
                genesis_seed,
                dist_key,
                catchup_period,
                scheme_id,
                metadata: Metadata {
                    node_version: None,
                    beacon_id,
                    chain_hash: vec![],
                },
            })
        }

        pub fn get_chain_info(&self) -> ChainInfoPacket {
            let mut h = Sha256::new();
            h.update(self.period.get_value().to_be_bytes());
            h.update(self.genesis_time.to_be_bytes());

            let public_key = match self.dist_key.first() {
                Some(key) => key.to_owned(),
                None => {
                    error!("chain info: failed to hash pubkey, the key is missing, this should be possible only in fresh-node tests");
                    vec![]
                }
            };

            h.update(&public_key);
            h.update(&self.genesis_seed);
            if !crate::core::beacon::is_default_beacon_id(&self.metadata.beacon_id) {
                h.update(self.metadata.beacon_id.as_bytes())
            }
            let hash = h.finalize().to_vec();
            let group_hash = self.hash();

            ChainInfoPacket {
                public_key,
                period: self.period.get_value(),
                genesis_time: self.genesis_time as i64,
                hash: hash.to_owned(),
                group_hash: group_hash.into(),
                scheme_id: self.scheme_id.to_owned(),

                metadata: Some(Metadata::mimic_version(&self.metadata.beacon_id, &hash)),
            }
        }
    }

    impl GroupPacket {
        pub fn hash(&self) -> [u8; 32] {
            let mut h = blake2b256::Blake2b256::new();

            for node in self.nodes.iter() {
                h.update({
                    let mut hh = blake2b256::Blake2b256::new();
                    hh.update(node.index.to_le_bytes());
                    hh.update(&node.public.key);
                    hh.finalize().as_slice()
                })
            }

            h.update(self.threshold.to_le_bytes());
            h.update(self.genesis_time.to_le_bytes());

            if self.transition_time != 0 {
                h.update(self.transition_time.to_le_bytes());
            }

            if !self.dist_key.is_empty() {
                h.update({
                    let mut hh = blake2b256::Blake2b256::new();
                    self.dist_key.iter().for_each(|key| hh.update(key));
                    hh.finalize().as_slice()
                })
            }

            if !crate::core::beacon::is_default_beacon_id(&self.metadata.beacon_id) {
                h.update(self.metadata.beacon_id.as_bytes())
            }

            h.finalize().into()
        }
    }

    impl fmt::Display for ChainInfoPacket {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            let ChainInfoPacket {
                public_key,
                period,
                genesis_time,
                hash,
                group_hash,
                scheme_id,
                metadata: _,
            } = self;

            write!(
                f,
                "PublicKey: {}\nPeriod: {}\nGenesis Time: {}\nHash: {}\nGroup Hash: {}\nSchemeID: {}",
                hex::encode(public_key),
                period,
                genesis_time,
                hex::encode(hash),
                hex::encode(group_hash),
                scheme_id,
            )
        }
    }

    impl Display for Command {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Command::Initial(..) => f.write_str("Initial"),
                Command::Resharing(..) => f.write_str("Resharing"),
                Command::Join(..) => f.write_str("Join"),
                Command::Accept(..) => f.write_str("Accept"),
                Command::Reject(..) => f.write_str("Reject"),
                Command::Execute(..) => f.write_str("Execute"),
                Command::Abort(..) => f.write_str("Abort"),
            }
        }
    }

    impl Display for GossipData {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                GossipData::Proposal(..) => f.write_str("Proposal"),
                GossipData::Accept(..) => f.write_str("Accept"),
                GossipData::Reject(..) => f.write_str("Reject"),
                GossipData::Execute(..) => f.write_str("Execute"),
                GossipData::Abort(..) => f.write_str("Abort"),
                GossipData::Dkg(packet) => write!(f, "bundle: {}", packet.dkg.bundle),
            }
        }
    }

    impl Display for Bundle {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Bundle::Deal(..) => f.write_str("Deal"),
                Bundle::Response(..) => f.write_str("Response"),
                Bundle::Justification(..) => f.write_str("Justification"),
            }
        }
    }
}
