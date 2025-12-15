// Copyright 2023-2025 StorSwift Inc.
// SPDX-License-Identifier: Apache-2.0

//! Types are re-exported directly if their fields DO NOT contain:
//!  - option<T> instead of T
//!  - protected new pattern types
use super::utils::{ConvertProto, RequireSome, TransportError};
use crate::{
    dkg::status::Status as DkgStatus,
    net::utils::Address,
    protobuf::{self, drand::Metadata},
};

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

#[derive(Debug, PartialEq)]
pub struct Identity {
    pub address: Address,
    pub key: Vec<u8>,
    pub signature: Vec<u8>,
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
