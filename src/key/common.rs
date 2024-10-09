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

use super::keys::PublicKey;
use crate::core::beacon;

use energon::drand::Scheme;
use energon::traits::Affine;
use energon::traits::ScalarField;

use anyhow::Error;
use anyhow::Result;
use sha2::digest::Digest;

pub type DistributedKey = Vec<Vec<u8>>;

#[derive(PartialEq)]
pub struct Identity {
    pub address: String,
    pub key: Vec<u8>,
    pub tls: bool,
    pub signature: Vec<u8>,
    pub scheme_name: Option<String>,
}

pub struct Pair {
    private: Vec<u8>,
    pub public: Identity,
}

impl Pair {
    pub fn init(private: Vec<u8>, public: Identity) -> Self {
        Self { private, public }
    }
}

pub struct Node {
    pub identity: Identity,
    pub index: u32,
}

pub struct Group {
    pub nodes: Vec<Node>,
    pub threshold: u32,
    /// period in seconds
    pub period: u32,
    pub genesis_time: u64,
    pub transition_time: u64,
    pub genesis_seed: Vec<u8>,
    pub dist_key: DistributedKey,
    /// catchup_period in seconds
    pub catchup_period: u32,
    pub beacon_id: String,
    pub scheme_id: String,
}

impl<S: Scheme> TryInto<super::keys::Identity<S>> for Identity {
    type Error = Error;

    fn try_into(self) -> Result<super::keys::Identity<S>, Self::Error> {
        let address = self.address;
        let key = Affine::deserialize(&self.key)?;
        let tls = self.tls;
        let signature = Affine::deserialize(&self.signature)?;
        Ok(super::keys::Identity::new(address, tls, key, signature))
    }
}

impl<S: Scheme> TryInto<super::keys::Pair<S>> for Pair {
    type Error = Error;

    fn try_into(self) -> Result<super::keys::Pair<S>, Self::Error> {
        let private = ScalarField::from_bytes_be(&self.private)?;
        let public = self.public.try_into()?;
        Ok(super::keys::Pair::set(private, public))
    }
}

impl<S: Scheme> TryInto<super::node::Node<S>> for Node {
    type Error = Error;

    fn try_into(self) -> Result<super::node::Node<S>, Self::Error> {
        let identity = self.identity.try_into()?;
        Ok(super::node::Node::new(identity, self.index))
    }
}

impl<S: Scheme> TryInto<super::group::Group<S>> for Group {
    type Error = Error;

    fn try_into(self) -> Result<super::group::Group<S>, Self::Error> {
        let nodes: Result<Vec<_>> = self.nodes.into_iter().map(|node| node.try_into()).collect();

        let mut dist_key: Vec<PublicKey<S>> = vec![];
        if !self.dist_key.is_empty() {
            for key in self.dist_key.iter() {
                dist_key.push(Affine::deserialize(key)?)
            }
        };

        let group = super::group::Group::new(
            nodes?,
            self.threshold,
            self.period,
            self.genesis_time,
            self.transition_time,
            self.genesis_seed,
            dist_key,
            self.catchup_period,
            self.beacon_id.into(),
        );

        Ok(group)
    }
}

/// Provides common representation for generic types
pub trait ToCommon {
    type Target;
    fn to_common(&self) -> Self::Target;
}

impl<S: Scheme> ToCommon for super::keys::Identity<S> {
    type Target = Identity;

    fn to_common(&self) -> Self::Target {
        Self::Target {
            address: self.address().to_owned(),
            key: self.key().serialize().unwrap(),
            tls: self.tls(),
            signature: self.signature().serialize().unwrap(),
            scheme_name: Some(S::ID.to_owned()),
        }
    }
}

pub trait Hash {
    type Hasher;
    fn hash(&self) -> Vec<u8>;
}

impl Hash for Group {
    type Hasher = crev_common::Blake2b256;

    fn hash(&self) -> Vec<u8> {
        let mut h = Self::Hasher::new();

        for node in self.nodes.iter() {
            h.update({
                let mut hh = Self::Hasher::new();
                hh.update(node.index.to_le_bytes());
                hh.update(&node.identity.key);
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
                let mut hh = Self::Hasher::new();
                self.dist_key.iter().for_each(|key| hh.update(key));
                hh.finalize().as_slice()
            })
        }

        if !beacon::is_default_beacon_id(&self.beacon_id) {
            h.update(self.beacon_id.as_bytes())
        }

        h.finalize().to_vec()
    }
}
