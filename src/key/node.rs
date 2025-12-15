// Copyright 2023-2025 StorSwift Inc.
// SPDX-License-Identifier: Apache-2.0

use super::{keys::Identity, Scheme};
use crate::net::utils::Address;

/// Node is a wrapper around identity that additionally includes the index that
/// the node has within this group. The index is computed initially when the
/// group is first created. The index is useful only for drand nodes, and
/// shouldn't be taken into account from an external point of view.
/// The index is useful to be able to reshare correctly, and gives the ability to
/// have a better logging: packets sent during DKG only contain an index, from
/// which we can derive the actual address from the index.
#[derive(PartialEq, Debug)]
pub struct Node<S: Scheme> {
    identity: Identity<S>,
    /// Index is an alias to designate the index of a node. The index is used to evaluate
    /// the share of a node, and is thereafter fixed. A node will use the same index for
    /// generating a partial signature afterwards for example.
    index: u32,
}

impl<S: Scheme> Node<S> {
    pub fn new(identity: Identity<S>, index: u32) -> Self {
        Self { identity, index }
    }

    pub fn public(&self) -> &Identity<S> {
        &self.identity
    }

    pub fn index(&self) -> u32 {
        self.index
    }

    pub fn into_peer(self) -> Address {
        self.identity.address
    }
}
