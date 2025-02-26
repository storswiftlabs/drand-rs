//! Types are re-exported directly if their fields DO NOT contain:
//!  - option<T> instead of T
//!  - protected new pattern types  
//!  
//! Note: generic types might be converted directly into protobuf later.

pub use protobuf::drand::BeaconStatus;
pub use protobuf::drand::ChainStoreStatus;
pub use protobuf::drand::ListBeaconIDsRequest;
pub use protobuf::drand::ListBeaconIDsResponse;
pub use protobuf::drand::ListSchemesRequest;
pub use protobuf::drand::MetricsRequest;
pub use protobuf::drand::MetricsResponse;
pub use protobuf::drand::NodeVersion;
pub use protobuf::drand::SyncProgress;

use super::utils::*;
use crate::dkg::state_machine::state::Status as DkgStatus;
use crate::net::utils::Address;
use crate::net::utils::Seconds;
use crate::protobuf;
use crate::protobuf::drand::Metadata;

impl ConvertProto for protobuf::drand::Address {
    type Inner = Address;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        let data = Address::precheck(&self.address)?;

        Ok(data)
    }
}

impl From<Address> for crate::protobuf::drand::Address {
    fn from(value: Address) -> Self {
        Self {
            address: value.as_str().into(),
        }
    }
}

impl ConvertProto for crate::protobuf::drand::DkgStatus {
    type Inner = DkgStatus;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        self.status.try_into().map_err(TransportError::DkgState)
    }
}

impl From<DkgStatus> for crate::protobuf::drand::DkgStatus {
    fn from(value: DkgStatus) -> Self {
        Self {
            status: value as u32,
        }
    }
}

pub struct StatusRequest {
    pub check_conn: Vec<Address>,
    pub metadata: Metadata,
}

impl ConvertProto for crate::protobuf::drand::StatusRequest {
    type Inner = StatusRequest;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        let Self {
            check_conn,
            metadata,
        } = self;

        Ok(Self::Inner {
            check_conn: try_from_vec(check_conn)?,
            metadata: metadata.require_some()?,
        })
    }
}

impl From<StatusRequest> for crate::protobuf::drand::StatusRequest {
    fn from(value: StatusRequest) -> Self {
        let StatusRequest {
            check_conn,
            metadata,
        } = value;

        Self {
            check_conn: from_vec(check_conn),
            metadata: Some(metadata),
        }
    }
}

#[derive(Debug)]
pub struct StatusResponse {
    pub dkg: DkgStatus,
    pub epoch: u32,
    pub beacon: BeaconStatus,
    pub chain_store: ChainStoreStatus,
    pub connections: std::collections::HashMap<String, bool>,
}

impl ConvertProto for crate::protobuf::drand::StatusResponse {
    type Inner = StatusResponse;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        let Self {
            dkg,
            epoch,
            beacon,
            chain_store,
            connections,
        } = self;

        Ok(Self::Inner {
            dkg: dkg.require_some()?.validate()?,
            epoch,
            beacon: beacon.require_some()?,
            chain_store: chain_store.require_some()?,
            connections,
        })
    }
}

impl From<StatusResponse> for crate::protobuf::drand::StatusResponse {
    fn from(value: StatusResponse) -> Self {
        let StatusResponse {
            dkg,
            epoch,
            beacon,
            chain_store,
            connections,
        } = value;

        Self {
            dkg: Some(dkg.into()),
            epoch,
            beacon: Some(beacon),
            chain_store: Some(chain_store),
            connections,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Identity {
    pub address: Address,
    pub key: Vec<u8>,
    pub signature: Vec<u8>,
}

impl ConvertProto for crate::protobuf::dkg::Participant {
    type Inner = Identity;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        let Self {
            ref address,
            key,
            signature,
        } = self;

        Ok(Self::Inner {
            address: Address::precheck(address)?,
            key,
            signature,
        })
    }
}

impl From<Identity> for crate::protobuf::dkg::Participant {
    fn from(value: Identity) -> Self {
        let Identity {
            ref address,
            key,
            signature,
        } = value;

        Self {
            address: address.as_str().into(),
            key,
            signature,
        }
    }
}

impl ConvertProto for crate::protobuf::drand::Identity {
    type Inner = Identity;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        let Self {
            ref address,
            key,
            signature,
        } = self;

        Ok(Self::Inner {
            address: Address::precheck(address)?,
            key,
            signature,
        })
    }
}

impl From<Identity> for crate::protobuf::drand::Identity {
    fn from(value: Identity) -> Self {
        let Identity {
            ref address,
            key,
            signature,
        } = value;

        Self {
            address: address.as_str().into(),
            key,
            signature,
        }
    }
}

pub struct Node {
    pub public: Identity,
    pub index: u32,
}

impl Node {
    pub fn new(public: Identity, index: u32) -> Self {
        Self { public, index }
    }

    pub fn into_parts(self) -> (Identity, u32) {
        (self.public, self.index)
    }
}

impl ConvertProto for crate::protobuf::drand::Node {
    type Inner = Node;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        let Self { public, index } = self;

        Ok(Node {
            public: public.require_some()?.validate()?,
            index,
        })
    }
}

impl From<Node> for crate::protobuf::drand::Node {
    fn from(value: Node) -> Self {
        let Node { public, index } = value;

        Self {
            public: Some(public.into()),
            index,
        }
    }
}

#[derive(Default)]
pub struct GroupPacket {
    pub nodes: Vec<Node>,
    pub threshold: u32,
    pub period: Seconds,
    pub genesis_time: u64,
    pub transition_time: u64,
    pub genesis_seed: Vec<u8>,
    pub dist_key: Vec<Vec<u8>>,
    pub catchup_period: Seconds,
    pub scheme_id: String,
    pub metadata: Metadata,
}

impl ConvertProto for crate::protobuf::drand::GroupPacket {
    type Inner = GroupPacket;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        let Self {
            nodes,
            threshold,
            period,
            genesis_time,
            transition_time,
            genesis_seed,
            dist_key,
            catchup_period,
            scheme_id,
            metadata,
        } = self;

        Ok(Self::Inner {
            nodes: try_from_vec(nodes)?,
            threshold,
            period: period.into(),
            genesis_time,
            transition_time,
            genesis_seed,
            dist_key,
            catchup_period: catchup_period.into(),
            scheme_id,
            metadata: metadata.require_some()?,
        })
    }
}

impl From<GroupPacket> for crate::protobuf::drand::GroupPacket {
    fn from(value: GroupPacket) -> Self {
        let GroupPacket {
            nodes,
            threshold,
            period,
            genesis_time,
            transition_time,
            genesis_seed,
            dist_key,
            catchup_period,
            scheme_id,
            metadata,
        } = value;

        Self {
            nodes: from_vec(nodes),
            threshold,
            period: period.into(),
            genesis_time,
            transition_time,
            genesis_seed,
            dist_key,
            catchup_period: catchup_period.into(),
            scheme_id,
            metadata: Some(metadata),
        }
    }
}

pub struct ChainInfoPacket {
    pub public_key: Vec<u8>,
    pub period: u32,
    pub genesis_time: i64,
    pub hash: Vec<u8>,
    pub group_hash: Vec<u8>,
    pub scheme_id: String,
    pub metadata: Metadata,
}

impl ConvertProto for crate::protobuf::drand::ChainInfoPacket {
    type Inner = ChainInfoPacket;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        let Self {
            public_key,
            period,
            genesis_time,
            hash,
            group_hash,
            scheme_id,
            metadata,
        } = self;

        Ok(Self::Inner {
            public_key,
            period,
            genesis_time,
            hash,
            group_hash,
            scheme_id,
            metadata: metadata.require_some()?,
        })
    }
}

impl From<ChainInfoPacket> for crate::protobuf::drand::ChainInfoPacket {
    fn from(value: ChainInfoPacket) -> Self {
        let ChainInfoPacket {
            public_key,
            period,
            genesis_time,
            hash,
            group_hash,
            scheme_id,
            metadata,
        } = value;

        Self {
            public_key,
            period,
            genesis_time,
            hash,
            group_hash,
            scheme_id,
            metadata: Some(metadata),
        }
    }
}

pub struct RemoteStatusRequest {
    pub metadata: Metadata,
    pub addresses: Vec<Address>,
}

impl ConvertProto for crate::protobuf::drand::RemoteStatusRequest {
    type Inner = RemoteStatusRequest;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        let Self {
            metadata,
            addresses,
        } = self;

        Ok(Self::Inner {
            metadata: metadata.require_some()?,
            addresses: try_from_vec(addresses)?,
        })
    }
}

impl From<RemoteStatusRequest> for crate::protobuf::drand::RemoteStatusRequest {
    fn from(value: RemoteStatusRequest) -> Self {
        let RemoteStatusRequest {
            metadata,
            addresses,
        } = value;

        Self {
            metadata: Some(metadata),
            addresses: from_vec(addresses),
        }
    }
}

pub struct RemoteStatusResponse {
    pub statuses: std::collections::HashMap<String, StatusResponse>,
}

impl ConvertProto for crate::protobuf::drand::RemoteStatusResponse {
    type Inner = RemoteStatusResponse;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        let mut statuses = std::collections::HashMap::with_capacity(self.statuses.len());

        for (key, value) in self.statuses.into_iter() {
            let _ = statuses.insert(key, value.validate()?);
        }

        Ok(Self::Inner { statuses })
    }
}

impl From<RemoteStatusResponse> for crate::protobuf::drand::RemoteStatusResponse {
    fn from(value: RemoteStatusResponse) -> Self {
        Self {
            statuses: value
                .statuses
                .into_iter()
                .map(|(key, value)| (key, value.into()))
                .collect(),
        }
    }
}

pub struct ListSchemesResponse {
    pub ids: Vec<String>,
    pub metadata: Metadata,
}

impl ConvertProto for crate::protobuf::drand::ListSchemesResponse {
    type Inner = ListSchemesResponse;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        let Self { ids, metadata } = self;

        Ok(Self::Inner {
            ids,
            metadata: metadata.require_some()?,
        })
    }
}

impl From<ListSchemesResponse> for crate::protobuf::drand::ListSchemesResponse {
    fn from(value: ListSchemesResponse) -> Self {
        let ListSchemesResponse { ids, metadata } = value;

        Self {
            ids,
            metadata: Some(metadata),
        }
    }
}

pub struct PublicKeyResponse {
    pub pub_key: Vec<u8>,
    pub address: Address,
    pub signature: Vec<u8>,
    pub metadata: Metadata,
    pub scheme_name: String,
}

impl ConvertProto for crate::protobuf::drand::PublicKeyResponse {
    type Inner = PublicKeyResponse;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        let Self {
            pub_key,
            ref addr,
            signature,
            metadata,
            scheme_name,
        } = self;

        Ok(Self::Inner {
            pub_key,
            address: Address::precheck(addr)?,
            signature,
            metadata: metadata.require_some()?,
            scheme_name,
        })
    }
}

impl From<PublicKeyResponse> for crate::protobuf::drand::PublicKeyResponse {
    fn from(value: PublicKeyResponse) -> Self {
        let PublicKeyResponse {
            pub_key,
            ref address,
            signature,
            metadata,
            scheme_name,
        } = value;

        Self {
            pub_key,
            addr: address.as_str().into(),
            signature,
            metadata: Some(metadata),
            scheme_name,
        }
    }
}

pub struct StartSyncRequest {
    pub nodes: Vec<String>,
    pub up_to: u64,
    pub metadata: Metadata,
}

impl ConvertProto for crate::protobuf::drand::StartSyncRequest {
    type Inner = StartSyncRequest;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        let Self {
            nodes,
            up_to,
            metadata,
        } = self;

        Ok(Self::Inner {
            nodes,
            up_to,
            metadata: metadata.require_some()?,
        })
    }
}

impl From<StartSyncRequest> for crate::protobuf::drand::StartSyncRequest {
    fn from(value: StartSyncRequest) -> Self {
        let StartSyncRequest {
            nodes,
            up_to,
            metadata,
        } = value;

        Self {
            nodes,
            up_to,
            metadata: Some(metadata),
        }
    }
}

#[derive(Debug)]
pub struct IdentityResponse {
    pub address: Address,
    pub key: Vec<u8>,
    pub signature: Vec<u8>,
    pub metadata: Metadata,
    pub scheme_name: String,
}

impl ConvertProto for crate::protobuf::drand::IdentityResponse {
    type Inner = IdentityResponse;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        let Self {
            ref address,
            key,
            signature,
            metadata,
            scheme_name,
        } = self;

        Ok(Self::Inner {
            address: Address::precheck(address)?,
            key,
            signature,
            metadata: metadata.require_some()?,
            scheme_name,
        })
    }
}

impl From<IdentityResponse> for crate::protobuf::drand::IdentityResponse {
    fn from(value: IdentityResponse) -> Self {
        let IdentityResponse {
            ref address,
            key,
            signature,
            metadata,
            scheme_name,
        } = value;

        Self {
            address: address.as_str().into(),
            key,
            signature,
            metadata: Some(metadata),
            scheme_name,
        }
    }
}

pub struct PartialBeaconPacket {
    pub round: u64,
    pub previous_signature: Vec<u8>,
    pub partial_sig: Vec<u8>,
    pub metadata: Metadata,
}

impl ConvertProto for crate::protobuf::drand::PartialBeaconPacket {
    type Inner = PartialBeaconPacket;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        let Self {
            round,
            previous_signature,
            partial_sig,
            metadata,
        } = self;

        Ok(Self::Inner {
            round,
            previous_signature,
            partial_sig,
            metadata: metadata.require_some()?,
        })
    }
}

impl From<PartialBeaconPacket> for crate::protobuf::drand::PartialBeaconPacket {
    fn from(value: PartialBeaconPacket) -> Self {
        let PartialBeaconPacket {
            round,
            previous_signature,
            partial_sig,
            metadata,
        } = value;

        Self {
            round,
            previous_signature,
            partial_sig,
            metadata: Some(metadata),
        }
    }
}

pub struct SyncRequest {
    pub from_round: u64,
    pub metadata: Metadata,
}

impl ConvertProto for crate::protobuf::drand::SyncRequest {
    type Inner = SyncRequest;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        let Self {
            from_round,
            metadata,
        } = self;

        Ok(Self::Inner {
            from_round,
            metadata: metadata.require_some()?,
        })
    }
}

impl From<SyncRequest> for crate::protobuf::drand::SyncRequest {
    fn from(value: SyncRequest) -> Self {
        let SyncRequest {
            from_round,
            metadata,
        } = value;

        Self {
            from_round,
            metadata: Some(metadata),
        }
    }
}

pub struct BeaconPacket {
    pub previous_signature: Vec<u8>,
    pub round: u64,
    pub signature: Vec<u8>,
    pub metadata: Metadata,
}

impl ConvertProto for crate::protobuf::drand::BeaconPacket {
    type Inner = BeaconPacket;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        let Self {
            previous_signature,
            round,
            signature,
            metadata,
        } = self;

        Ok(Self::Inner {
            previous_signature,
            round,
            signature,
            metadata: metadata.require_some()?,
        })
    }
}

impl From<BeaconPacket> for crate::protobuf::drand::BeaconPacket {
    fn from(value: BeaconPacket) -> Self {
        let BeaconPacket {
            previous_signature,
            round,
            signature,
            metadata,
        } = value;

        Self {
            previous_signature,
            round,
            signature,
            metadata: Some(metadata),
        }
    }
}

pub struct PublicRandRequest {
    pub round: u64,
    pub metadata: Metadata,
}

impl ConvertProto for crate::protobuf::drand::PublicRandRequest {
    type Inner = PublicRandRequest;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        let Self { round, metadata } = self;

        Ok(Self::Inner {
            round,
            metadata: metadata.require_some()?,
        })
    }
}

impl From<PublicRandRequest> for crate::protobuf::drand::PublicRandRequest {
    fn from(value: PublicRandRequest) -> Self {
        let PublicRandRequest { round, metadata } = value;

        Self {
            round,
            metadata: Some(metadata),
        }
    }
}

pub struct PublicRandResponse {
    pub round: u64,
    pub signature: Vec<u8>,
    pub previous_signature: Vec<u8>,
    pub metadata: Metadata,
}

impl ConvertProto for crate::protobuf::drand::PublicRandResponse {
    type Inner = PublicRandResponse;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        let Self {
            round,
            signature,
            previous_signature,
            metadata,
        } = self;

        Ok(Self::Inner {
            round,
            signature,
            previous_signature,
            metadata: metadata.require_some()?,
        })
    }
}

impl From<PublicRandResponse> for crate::protobuf::drand::PublicRandResponse {
    fn from(value: PublicRandResponse) -> Self {
        let PublicRandResponse {
            round,
            signature,
            previous_signature,
            metadata,
        } = value;

        Self {
            round,
            signature,
            previous_signature,
            metadata: Some(metadata),
        }
    }
}
