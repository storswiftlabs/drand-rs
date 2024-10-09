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

use crate::core::beacon;
use crate::core::beacon::BeaconID;
use crate::core::beacon::DEFAULT_BEACON_ID;

use crate::key::group::Group;
use crate::key::keys::Identity;
use crate::key::node::Node;

use crate::protobuf::common::GroupPacket;
use crate::protobuf::common::Identity as ProtoIdentity;
use crate::protobuf::common::Metadata;
use crate::protobuf::common::Node as ProtoNode;
use crate::protobuf::dkg::DealBundle;
use crate::protobuf::dkg::JustificationBundle;
use crate::protobuf::dkg::ResponseBundle;

use energon::drand::Scheme;
use energon::traits::Affine;
use std::fmt::Display;

/// Callback is dropped, something is very broken.
pub const INTERNAL_ERR: &str = "INTERNAL_ERR";

pub fn get_ref_id(meta: Option<&Metadata>) -> &str {
    match meta {
        Some(m) => {
            if m.beacon_id.is_empty() {
                DEFAULT_BEACON_ID
            } else {
                m.beacon_id.as_ref()
            }
        }
        None => DEFAULT_BEACON_ID,
    }
}

pub trait ToProto {
    type Packet;
    fn to_proto(&self) -> Self::Packet;
}

impl<S: Scheme> ToProto for Identity<S> {
    type Packet = ProtoIdentity;

    fn to_proto(&self) -> Self::Packet {
        Self::Packet {
            address: self.address().into(),
            key: self.key().serialize().unwrap(),
            tls: self.tls(),
            signature: self.signature().serialize().unwrap(),
        }
    }
}

impl<S: Scheme> ToProto for Node<S> {
    type Packet = ProtoNode;

    fn to_proto(&self) -> Self::Packet {
        Self::Packet {
            public: Some(self.identity().to_proto()),
            index: self.index(),
        }
    }
}

impl<S: Scheme> ToProto for Group<S> {
    type Packet = GroupPacket;

    fn to_proto(&self) -> Self::Packet {
        let mut nodes = Vec::with_capacity(self.nodes.len());
        self.nodes.iter().for_each(|n| nodes.push(n.to_proto()));

        let dist_key = {
            if self.dist_key.is_empty() {
                vec![]
            } else {
                let mut dist_key = Vec::with_capacity(self.dist_key.len());
                self.dist_key
                    .iter()
                    .for_each(|key| dist_key.push(key.serialize().unwrap()));
                dist_key
            }
        };

        Self::Packet {
            nodes,
            threshold: self.threshold,
            period: self.period,
            genesis_time: self.genesis_time,
            transition_time: self.transition_time,
            genesis_seed: self.genesis_seed.clone(), // ;(
            dist_key,
            catchup_period: self.catchup_period,
            scheme_id: S::ID.into(),
            metadata: From::from(&self.beacon_id),
        }
    }
}

impl From<Option<&Metadata>> for BeaconID {
    fn from(value: Option<&Metadata>) -> Self {
        match value {
            Some(m) => BeaconID::new(m.beacon_id.as_str()),
            None => BeaconID::new(beacon::DEFAULT_BEACON_ID),
        }
    }
}

impl From<&BeaconID> for Option<Metadata> {
    fn from(id: &BeaconID) -> Self {
        let mut beacon_id = beacon::DEFAULT_BEACON_ID;
        if !id.is_default() {
            beacon_id = id.as_str();
        }

        Some(Metadata {
            beacon_id: beacon_id.to_string(),
            ..Default::default()
        })
    }
}

impl Display for DealBundle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "deal")
    }
}
impl Display for ResponseBundle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "response")
    }
}
impl Display for JustificationBundle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "response")
    }
}
