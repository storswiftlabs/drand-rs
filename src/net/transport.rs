// Copyright (C) 2023-2024 StorSwift Inc.
// This file is part of the Drand-RS library.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Display;

use crate::core::beacon;
use crate::core::beacon::BeaconID;
use crate::key::common::Group;
use crate::key::common::Identity;
use crate::key::common::Node;

use crate::protobuf::common::Metadata;
use crate::protobuf::drand::DkgPacket;
use crate::protobuf::drand::SetupInfoPacket;
use tonic::Status;

// Conversion between inner non-generic types and protobuf

/// ref: https://github.com/tokio-rs/prost/issues/945
pub const ERR_EMPTY_DATA: &str = "empty data";

pub trait ProtoConvert: Sized {
    type Proto;

    fn to_proto(self) -> Self::Proto;
    fn from_proto(pb: Self::Proto) -> Result<Self, Status>;
}

impl ProtoConvert for Identity {
    type Proto = crate::protobuf::common::Identity;

    fn to_proto(self) -> Self::Proto {
        let Identity {
            address,
            key,
            tls,
            signature,
            scheme_name: _,
        } = self;

        Self::Proto {
            address,
            key,
            tls,
            signature,
        }
    }

    fn from_proto(pb: Self::Proto) -> Result<Self, Status> {
        let Self::Proto {
            address,
            key,
            tls,
            signature,
        } = pb;

        Ok(Identity {
            address,
            key,
            tls,
            signature,
            scheme_name: None,
        })
    }
}

impl ProtoConvert for Node {
    type Proto = crate::protobuf::common::Node;

    fn to_proto(self) -> Self::Proto {
        let Node { identity, index } = self;

        Self::Proto {
            public: Some(identity.to_proto()),
            index,
        }
    }

    fn from_proto(pb: Self::Proto) -> Result<Self, Status> {
        let Self::Proto { public, index } = pb;
        let identity =
            Identity::from_proto(public.ok_or_else(|| Status::data_loss(ERR_EMPTY_DATA))?)?;

        Ok(Node { identity, index })
    }
}

impl ProtoConvert for Group {
    type Proto = crate::protobuf::common::GroupPacket;

    fn to_proto(self) -> Self::Proto {
        todo!()
    }

    fn from_proto(pb: Self::Proto) -> Result<Self, Status> {
        let Self::Proto {
            nodes,
            threshold,
            period,
            genesis_time,
            transition_time,
            genesis_seed,
            dist_key,
            catchup_period,
            scheme_id,
            metadata: _,
        } = pb;

        let mut common_nodes: Vec<Node> = Vec::with_capacity(nodes.len());
        for node in nodes.into_iter() {
            let identity = Identity::from_proto(
                node.public
                    .ok_or_else(|| Status::data_loss(ERR_EMPTY_DATA))?,
            )?;

            common_nodes.push(Node {
                identity,
                index: node.index,
            })
        }
        common_nodes.sort_by_key(|node| node.index);

        let beacon_id = match pb.metadata {
            Some(m) => m.beacon_id,
            None => beacon::DEFAULT_BEACON_ID.into(),
        };

        Ok(Self {
            nodes: common_nodes,
            threshold,
            period,
            genesis_time,
            transition_time,
            genesis_seed,
            dist_key,
            catchup_period,
            scheme_id,
            beacon_id,
        })
    }
}

pub struct DkgInfo {
    pub new_group: Group,
    pub secret_proof: Vec<u8>,
    pub dkg_timeout: u32,
    pub signature: Vec<u8>,
}

impl ProtoConvert for DkgInfo {
    type Proto = crate::protobuf::drand::DkgInfoPacket;

    fn to_proto(self) -> Self::Proto {
        unimplemented!("Leader logic not implemented yet")
    }

    fn from_proto(pb: Self::Proto) -> Result<Self, Status> {
        let new_group = Group::from_proto(
            pb.new_group
                .ok_or_else(|| Status::data_loss(ERR_EMPTY_DATA))?,
        )?;

        Ok(Self {
            new_group,
            secret_proof: pb.secret_proof,
            dkg_timeout: pb.dkg_timeout,
            signature: pb.signature,
        })
    }
}

pub struct SetupInfo {
    pub leader_address: String,
    pub leader_tls: bool,
    pub secret: Vec<u8>,
}

impl SetupInfoPacket {
    pub fn from_proto(self) -> SetupInfo {
        SetupInfo {
            leader_address: self.leader_address,
            leader_tls: self.leader_tls,
            secret: self.secret,
        }
    }
}

pub struct IdentityResponse {
    pub address: String,
    pub key: Vec<u8>,
    pub tls: bool,
    pub signature: Vec<u8>,
    pub scheme_name: String,
}

type ProtoIdentityResponse = crate::protobuf::drand::IdentityResponse;
impl IdentityResponse {
    pub fn to_proto(self) -> ProtoIdentityResponse {
        let IdentityResponse {
            address,
            key,
            tls,
            signature,
            scheme_name,
        } = self;

        ProtoIdentityResponse {
            address,
            key,
            tls,
            signature,
            metadata: None,
            scheme_name,
        }
    }

    pub fn from_proto(pb: ProtoIdentityResponse) -> Self {
        let ProtoIdentityResponse {
            address,
            key,
            tls,
            signature,
            metadata: _,
            scheme_name,
        } = pb;

        Self {
            address,
            key,
            tls,
            signature,
            scheme_name,
        }
    }
}

pub struct DkgMsg {
    pub bundle: Bundle,
    pub beacon_id: String,
    pub info: String,
}

impl DkgMsg {
    pub fn new(bundle: Bundle, bundle_hash: &[u8], beacon_id: &BeaconID) -> Self {
        Self {
            info: format!("{bundle} bundle, hash:{}", &hex::encode(bundle_hash)[..10]),
            bundle,
            beacon_id: beacon_id.to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Deal {
    pub share_index: u32,
    pub encrypted_share: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct Response {
    pub dealer_index: u32,
    pub status: bool,
}

#[derive(Debug, Clone)]
pub struct Justification {
    pub share_index: u32,
    pub share: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ResponseBundle {
    pub share_index: u32,
    pub responses: Vec<Response>,
    pub session_id: Vec<u8>,
    pub signature: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct JustificationBundle {
    pub dealer_index: u32,
    pub justifications: Vec<Justification>,
    pub session_id: Vec<u8>,
    pub signature: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct DealBundle {
    pub dealer_index: u32,
    pub commits: Vec<Vec<u8>>,
    pub deals: Vec<Deal>,
    pub session_id: Vec<u8>,
    pub signature: Vec<u8>,
}

pub enum Bundle {
    Deal(DealBundle),
    Response(ResponseBundle),
    Justification(JustificationBundle),
}

type ProtoBundle = crate::protobuf::dkg::packet::Bundle;

impl Bundle {
    pub fn from_proto(pb: DkgPacket) -> Result<Self, Status> {
        let bundle = pb
            .dkg
            .ok_or_else(|| Status::data_loss(ERR_EMPTY_DATA))?
            .bundle
            .ok_or_else(|| Status::data_loss(ERR_EMPTY_DATA))?;

        let bundle = match bundle {
            ProtoBundle::Deal(deal_bundle) => {
                let crate::protobuf::dkg::DealBundle {
                    dealer_index,
                    commits,
                    deals,
                    session_id,
                    signature,
                } = deal_bundle;

                let deals = deals
                    .into_iter()
                    .map(|d| Deal {
                        share_index: d.share_index,
                        encrypted_share: d.encrypted_share,
                    })
                    .collect();

                Bundle::Deal(DealBundle {
                    dealer_index,
                    commits,
                    deals,
                    session_id,
                    signature,
                })
            }
            ProtoBundle::Response(response_bundle) => {
                let crate::protobuf::dkg::ResponseBundle {
                    share_index,
                    responses,
                    session_id,
                    signature,
                } = response_bundle;

                let responses = responses
                    .into_iter()
                    .map(|r| Response {
                        dealer_index: r.dealer_index,
                        status: r.status,
                    })
                    .collect();

                Bundle::Response(ResponseBundle {
                    share_index,
                    responses,
                    session_id,
                    signature,
                })
            }
            ProtoBundle::Justification(justification_bundle) => {
                let crate::protobuf::dkg::JustificationBundle {
                    dealer_index,
                    justifications,
                    session_id,
                    signature,
                } = justification_bundle;

                let justifications = justifications
                    .into_iter()
                    .map(|j| Justification {
                        share_index: j.share_index,
                        share: j.share,
                    })
                    .collect();

                Bundle::Justification(JustificationBundle {
                    dealer_index,
                    justifications,
                    session_id,
                    signature,
                })
            }
        };

        Ok(bundle)
    }

    pub fn to_proto(self, beacon_id: String) -> DkgPacket {
        let bundle = match self {
            Bundle::Deal(deal_bundle) => {
                let DealBundle {
                    dealer_index,
                    commits,
                    deals,
                    session_id,
                    signature,
                } = deal_bundle;

                let deals = deals
                    .into_iter()
                    .map(|d| crate::protobuf::dkg::Deal {
                        share_index: d.share_index,
                        encrypted_share: d.encrypted_share,
                    })
                    .collect();

                ProtoBundle::Deal(crate::protobuf::dkg::DealBundle {
                    dealer_index,
                    commits,
                    deals,
                    session_id,
                    signature,
                })
            }
            Bundle::Response(response_bundle) => {
                let ResponseBundle {
                    share_index,
                    responses,
                    session_id,
                    signature,
                } = response_bundle;

                let responses = responses
                    .into_iter()
                    .map(|r| crate::protobuf::dkg::Response {
                        dealer_index: r.dealer_index,
                        status: r.status,
                    })
                    .collect();

                ProtoBundle::Response(crate::protobuf::dkg::ResponseBundle {
                    share_index,
                    responses,
                    session_id,
                    signature,
                })
            }
            Bundle::Justification(justification_bundle) => {
                let JustificationBundle {
                    dealer_index,
                    justifications,
                    session_id,
                    signature,
                } = justification_bundle;

                let justifications = justifications
                    .into_iter()
                    .map(|j| crate::protobuf::dkg::Justification {
                        share_index: j.share_index,
                        share: j.share,
                    })
                    .collect();

                ProtoBundle::Justification(crate::protobuf::dkg::JustificationBundle {
                    dealer_index,
                    justifications,
                    session_id,
                    signature,
                })
            }
        };

        DkgPacket {
            dkg: Some(crate::protobuf::dkg::Packet {
                metadata: None,
                bundle: Some(bundle),
            }),
            metadata: Some(Metadata {
                beacon_id,
                ..Default::default()
            }),
        }
    }
}

impl Display for Bundle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let variant = match self {
            Bundle::Deal(_) => "deal",
            Bundle::Response(_) => "response",
            Bundle::Justification(_) => "justification",
        };
        write!(f, "{variant}")
    }
}
