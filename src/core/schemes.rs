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

pub use energon::schemes::drand::BeaconDigest;
pub use energon::schemes::drand::DefaultScheme;
pub use energon::schemes::drand::SchortSigScheme;
pub use energon::schemes::drand::UnchainedScheme;

use super::beacon::Beacon;
use super::beacon::BeaconCmd;
use super::beacon::BeaconID;
use super::Scheme;

use crate::key::common::Pair;
use crate::key::store::FileStore;
use crate::net::pool::PoolCmd;

use anyhow::bail;
use anyhow::Result;

use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;

#[derive(Debug, PartialEq)]
pub enum Schemes {
    Default,
    Unchained,
    SchortSig,
}

impl Schemes {
    pub fn from_str(scheme: &str) -> Result<Self> {
        let s = match scheme {
            DefaultScheme::ID => Self::Default,
            UnchainedScheme::ID => Self::Unchained,
            SchortSigScheme::ID => Self::SchortSig,
            _ => bail!("Unknown scheme"),
        };
        Ok(s)
    }

    pub fn as_str(&self) -> &str {
        match self {
            Schemes::Default => DefaultScheme::ID,
            Schemes::Unchained => UnchainedScheme::ID,
            Schemes::SchortSig => SchortSigScheme::ID,
        }
    }

    pub fn list_schemes() -> Vec<String> {
        vec![DefaultScheme::ID.into(), UnchainedScheme::ID.into(), SchortSigScheme::ID.into()]
    }
}

pub fn run_beacon(
    pair: Pair,
    id: BeaconID,
    fs: FileStore,
    rx: Receiver<BeaconCmd>,
    private_listen: &str,
    pool: mpsc::Sender<PoolCmd>,
) -> Result<(), anyhow::Error> {
    if let Some(scheme) = pair.public.scheme_name.as_ref() {
        match scheme.as_str() {
            DefaultScheme::ID => {
                Beacon::<DefaultScheme>::init(pair.try_into()?, id, fs, private_listen, pool)
                    .start(rx)
            }
            UnchainedScheme::ID => {
                Beacon::<UnchainedScheme>::init(pair.try_into()?, id, fs, private_listen, pool)
                    .start(rx)
            }
            SchortSigScheme::ID => {
                Beacon::<SchortSigScheme>::init(pair.try_into()?, id, fs, private_listen, pool)
                    .start(rx)
            }
            _ => bail!("unknown scheme"),
        }
    } else {
        bail!("should not be empty")
    }

    Ok(())
}

pub fn gen_keypair(
    scheme: String,
    folder: &str,
    tls: bool,
    id: String,
    address: String,
) -> Result<()> {
    use crate::key::keys::Pair;
    println!("Generating private / public key pair, TLS:{tls}");

    // basic sanity checks
    // TODO: duplicated, use [`Address`] instead
    let uri = address.parse::<tonic::transport::Uri>()?;
    if uri.host().is_none()
        || uri.port().is_none()
        || uri.scheme().is_some()
        || uri.path_and_query().is_some()
    {
        bail!("expected HOST:PORT, received: {address}",)
    }
    let path = FileStore::verify_path(folder, &id)?;

    match scheme.as_str() {
        DefaultScheme::ID => {
            let fs = FileStore::create_new(path)?;
            fs.save_pair(&Pair::<DefaultScheme>::new(address, tls))?
        }
        UnchainedScheme::ID => {
            let fs = FileStore::create_new(path)?;
            fs.save_pair(&Pair::<UnchainedScheme>::new(address, tls))?
        }
        SchortSigScheme::ID => {
            let fs = FileStore::create_new(path)?;
            fs.save_pair(&Pair::<SchortSigScheme>::new(address, tls))?
        }
        _ => bail!("Unknown scheme"),
    };

    Ok(())
}
