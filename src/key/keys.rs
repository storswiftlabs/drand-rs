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

use energon::drand::poly::PriShare;
use energon::drand::poly::PubPoly;
use energon::drand::Scheme;
use energon::traits::Affine;
use energon::traits::Group;
use energon::traits::ScalarField;

use std::fmt::Display;

pub type PublicKey<S> = <<S as Scheme>::Key as Group>::Affine;
pub type Signature<S> = <<S as Scheme>::Sig as Group>::Affine;
pub struct PrivateKey<S: Scheme>(S::Scalar);

#[derive(Debug, PartialEq)]
pub struct Pair<S: Scheme> {
    private: S::Scalar,
    public: Identity<S>,
}

impl<S: Scheme> Pair<S> {
    pub fn new(address: String, tls: bool) -> Self {
        let private = S::Scalar::random();
        let key = S::sk_to_pk(&private);
        let signature = S::bls_sign(&key.hash().unwrap(), &private).unwrap();
        Self {
            private,
            public: Identity {
                key,
                address,
                tls,
                signature,
            },
        }
    }

    pub fn set(private: S::Scalar, public: Identity<S>) -> Self {
        Self { private, public }
    }

    pub fn private(&self) -> S::Scalar {
        self.private
    }

    pub fn public(&self) -> &Identity<S> {
        &self.public
    }
}

#[derive(Debug, PartialEq)]
pub struct Identity<S: Scheme> {
    address: String,
    key: PublicKey<S>,
    tls: bool,
    signature: Signature<S>,
}

impl<S: Scheme> Identity<S> {
    pub fn new(address: String, tls: bool, key: PublicKey<S>, signature: Signature<S>) -> Self {
        Self {
            address,
            key,
            tls,
            signature,
        }
    }

    pub fn key(&self) -> &PublicKey<S> {
        &self.key
    }

    pub fn tls(&self) -> bool {
        self.tls
    }

    pub fn signature(&self) -> &Signature<S> {
        &self.signature
    }

    pub fn scheme_id(&self) -> &str {
        S::ID
    }

    pub fn address(&self) -> &str {
        self.address.as_str()
    }
}

pub struct DistKeyShare<S: Scheme> {
    commits: PubPoly<S>,
    pri_share: PriShare<S>,
}

impl<S: Scheme> DistKeyShare<S> {
    pub fn new(commits: Vec<PublicKey<S>>, pri_share: PriShare<S>) -> Self {
        Self {
            commits: PubPoly { commits },
            pri_share,
        }
    }

    pub fn public(&self) -> &PubPoly<S> {
        &self.commits
    }

    pub fn private(&self) -> &PriShare<S> {
        &self.pri_share
    }
}

impl<S: Scheme> Display for Identity<S> {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        write!(
            f,
            "Address = {}\nKey = {}\nTLS = {}\nSignature = {}\nSchemeName = {}",
            self.address,
            self.key,
            self.tls,
            self.signature,
            S::ID
        )
    }
}
