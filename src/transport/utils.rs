use crate::{dkg::status::StateError, net::utils::InvalidAddress};
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
    fn require_some(self) -> Result<T, TransportError> {
        self.ok_or_else(|| TransportError::DataIsMissing(core::any::type_name::<T>().into()))
    }
}

pub(super) fn from_vec<I, T, U>(data: I) -> Vec<U>
where
    I: IntoIterator<Item = T>,
    T: Into<U>,
{
    data.into_iter().map(Into::into).collect()
}

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
    use crate::protobuf::dkg::DkgPacket;
    use crate::protobuf::drand::ChainInfoPacket;

    use crate::transport::dkg::Bundle;
    use crate::transport::dkg::Command;
    use crate::transport::dkg::GossipData;

    use prost_types::Timestamp;
    use std::fmt;
    use std::fmt::Display;

    impl DkgPacket {
        pub fn get_id(&self) -> Result<String, tonic::Status> {
            self.dkg
                .as_ref()
                .and_then(|p| p.metadata.as_ref())
                .map(|m| m.beacon_id.clone())
                .ok_or_else(|| {
                    tonic::Status::data_loss("could not find packet metadata to read beaconID")
                })
        }
    }

    impl GossipData {
        pub fn get_execute(&self) -> Option<Timestamp> {
            match self {
                GossipData::Execute(execute) => Some(execute.time),
                _ => None,
            }
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
