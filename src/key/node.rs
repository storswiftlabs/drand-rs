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

use super::keys::Identity;
use crate::core::Scheme;

#[derive(PartialEq)]
pub struct Node<S: Scheme> {
    identity: Identity<S>,
    index: u32,
}

impl<S: Scheme> Node<S> {
    pub fn into_parts(self) -> (Identity<S>, u32) {
        (self.identity, self.index)
    }

    pub fn new(identity: Identity<S>, index: u32) -> Self {
        Self { identity, index }
    }

    pub fn identity(&self) -> &Identity<S> {
        &self.identity
    }

    pub fn index(&self) -> u32 {
        self.index
    }
}
