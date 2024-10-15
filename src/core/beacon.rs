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

use super::chain::ChainSender;
use super::dkg::DkgCmd;
use super::dkg::DkgConfig;
use super::dkg::DkgState;
use super::Scheme;

use crate::core::chain::ChainHandler;
use crate::core::dkg::Generator;

use crate::key::common::Hash;
use crate::key::common::ToCommon;
use crate::key::group::Group;
use crate::key::keys::Pair;
use crate::key::store::FileStore;

use crate::net::client::ProtocolClient;
use crate::net::pool::PoolCmd;
use crate::net::transport::DkgInfo;
use crate::net::transport::IdentityResponse;
use crate::net::transport::SetupInfo;
use crate::protobuf::drand::PartialBeaconPacket;

use anyhow::anyhow;
use anyhow::bail;
use anyhow::Result;
use core::fmt;
use energon::traits::Affine;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Receiver;

use tracing::info_span;
use tracing::Span;
use tracing::*;

/// DefaultBeaconID is the value used when beacon id has an empty value. This value should not be changed for backward-compatibility reasons
pub const DEFAULT_BEACON_ID: &str = "default";
// Direct relationship between an empty string and the reserved id "default".
pub fn is_default_beacon_id(beacon_id: &str) -> bool {
    beacon_id == DEFAULT_BEACON_ID || beacon_id.is_empty()
}

pub type Callback<T> = tokio::sync::oneshot::Sender<T>;

#[derive(PartialEq, Clone, Eq, Hash, PartialOrd, Ord)]
pub struct BeaconID {
    inner: Arc<str>,
}

impl BeaconID {
    pub fn is_default(&self) -> bool {
        &*self.inner == DEFAULT_BEACON_ID
    }

    pub fn new(id: impl Into<Arc<str>>) -> Self {
        Self { inner: id.into() }
    }

    pub fn as_str(&self) -> &str {
        self.inner.as_ref()
    }

    pub fn is_eq(&self, id: &str) -> bool {
        self.inner.as_ref() == id
    }
}

impl fmt::Display for BeaconID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl fmt::Debug for BeaconID {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.inner.fmt(f)
    }
}

pub enum BeaconCmd {
    Partial(PartialBeaconPacket, Callback<Result<()>>),
    ShowPublicKey(Callback<Vec<u8>>),
    Shutdown(Callback<Result<()>>),
    IdentityRequest(Callback<IdentityResponse>),
    Dkg(DkgCmd),
}

pub struct InnerNode<S: Scheme> {
    pub beacon_id: BeaconID,
    pub fs: FileStore,
    pub keypair: Pair<S>,
    pub pool: mpsc::Sender<PoolCmd>,
    pub span: Span,
}

impl<S: Scheme> InnerNode<S> {
    pub fn span(&self) -> &Span {
        &self.span
    }
}

pub struct Beacon<S: Scheme>(Arc<InnerNode<S>>);

impl<S: Scheme> Beacon<S> {
    pub fn init(
        keypair: Pair<S>,
        id: BeaconID,
        fs: FileStore,
        private_listen: &str,
        pool: mpsc::Sender<PoolCmd>,
    ) -> Self {
        let span = info_span!("", id = format!("{private_listen}.{id}"));

        Self(Arc::new(InnerNode {
            beacon_id: id.clone(),
            fs,
            keypair,
            pool,
            span,
        }))
    }

    pub fn start(self, mut rx: Receiver<BeaconCmd>) {
        tokio::spawn(async move {
            let mut _dkg_state = DkgState::NonActive;
            // TODO: here should be attempt to load database and distributed keys, groupfile.
            let mut chain: Option<ChainSender> = None;

            if chain.is_none() {
                info!(parent:self.span(), "beacon id [{}]: will run as fresh install -> expect to run DKG.", self.beacon_id);
            }

            while let Some(c) = rx.recv().await {
                match c {
                    BeaconCmd::Partial(packet, callback) => match &chain {
                        Some(chain) => {
                            let _ = chain.send((packet, callback)).await;
                        }
                        None => {
                            let _ = callback
                                .send(Err(anyhow!("chain is not running: {}", self.beacon_id)));
                        }
                    },
                    BeaconCmd::ShowPublicKey(callback) => {
                        let _ = callback.send(self.keypair.public().key().serialize().unwrap());
                    }
                    BeaconCmd::Shutdown(callback) => {
                        let _ = callback.send(Ok(()));
                        break;
                    }
                    BeaconCmd::IdentityRequest(callback) => {
                        // TODO: missing impl: Identity to IdentityResponse
                        let _ = callback.send(IdentityResponse {
                            address: self.keypair.public().address().into(),
                            key: self.keypair.public().key().serialize().unwrap(),
                            tls: self.keypair.public().tls(),
                            signature: self.keypair.public().signature().serialize().unwrap(),
                            scheme_name: S::ID.into(),
                        });
                    }
                    BeaconCmd::Dkg(cmd) => match cmd {
                        DkgCmd::Broadcast(callback, bundle) => {
                            if let DkgState::Running(ref tx) = _dkg_state {
                                let _ = tx.send((bundle, callback)).await;
                            } else {
                                let _ = callback
                                    .send(Err(anyhow!("dkg is not running: {}", self.beacon_id)));
                            }
                        }
                        DkgCmd::Init(callback, setup_info, _reloader) => {
                            if DkgState::NonActive == _dkg_state {
                                let _ = callback.send(self.send_keys(&setup_info).await.map(
                                    |leader_key| {
                                        _dkg_state = DkgState::Initiated {
                                            leader_key,
                                            dkg_secret: setup_info.secret,
                                            _reloader,
                                        };

                                        self.fs.show_dist_key_path().to_string()
                                    },
                                ));
                            } else {
                                let _ = callback.send(Err(anyhow!("dkg already initiated")));
                            }
                        }
                        DkgCmd::NewGroup(callback, dkg_info) => {
                            match _dkg_state {
                                DkgState::Initiated {
                                    ref leader_key,
                                    ref dkg_secret,
                                    ref _reloader,
                                } => {
                                    let (tx, rx) = mpsc::channel(100);
                                    if let Err(e) = self
                                        .auth_dkg_info(dkg_info, leader_key, dkg_secret)
                                        .and_then(|dkg_config| {
                                            Generator::run(
                                                dkg_config,
                                                Arc::clone(&self.0),
                                                rx,
                                                _reloader.clone(),
                                            )
                                        })
                                    {
                                        let _ = callback
                                            .send(Err(anyhow!("dkg_info is not authorized: {e}")));
                                    } else {
                                        _dkg_state = DkgState::Running(tx);
                                        let _ = callback.send(Ok(()));
                                    }
                                }
                                _ => {
                                    let _ = callback
                                        .send(Err(anyhow!("dkg is not at initiated state")));
                                }
                            };
                        }

                        DkgCmd::Finish(dkg_result) => {
                            _dkg_state = DkgState::NonActive;
                            match dkg_result {
                                Ok((config, pool_uris)) => {
                                    info!(parent:self.span(),"beacon id [{}] dkg is successful", self.beacon_id );

                                    chain = match ChainHandler::start(config, Arc::clone(&self.0)) {
                                        Ok(sender) => Some(sender),
                                        Err(err) => {
                                            error!(parent:self.span(),"chain start: {err}");
                                            None
                                        }
                                    };

                                    if self
                                        .pool
                                        .send(PoolCmd::AddID(self.beacon_id.clone(), pool_uris))
                                        .await
                                        .is_err()
                                    {
                                        error!(parent:self.span(),"pool channel is closed")
                                    }
                                }
                                Err(e) => {
                                    error!(parent:self.span(),"beacon id [{}] dkg is failed: {}", self.beacon_id, e )
                                }
                            }
                        }
                    },
                }
            }
        });
    }

    /// Returns leader public key if dkg registration is succesful
    async fn send_keys(&self, info: &SetupInfo) -> Result<Vec<u8>> {
        // Verify leader identity
        let mut conn = ProtocolClient::new(&info.leader_address, info.leader_tls).await?;
        let identity = conn.get_identity(self.beacon_id.as_str()).await?;
        if info.leader_address != identity.address {
            bail!(
                "identity response: address mismatch: expected {} received {}",
                info.leader_address,
                identity.address
            )
        }

        conn.signal_dkg_participant(
            self.keypair.public().to_common(),
            &info.secret,
            self.beacon_id.as_str(),
        )
        .await?;

        tracing::debug!(parent: self.0.span(), "sent keys to the leader {}, is OK",&info.leader_address,);

        Ok(identity.key)
    }

    /// Returns DkgHandler if dkg_info is authorized
    /// WARN: some checks might be missing
    pub fn auth_dkg_info(
        &self,
        p: DkgInfo,
        leader_key: &[u8],
        secret: &[u8],
    ) -> Result<DkgConfig<S>> {
        if secret != p.secret_proof {
            bail!(
                "secret_proof is not valid, beacon_id: {}",
                self.beacon_id.as_str()
            );
        }

        if p.new_group.scheme_id != S::ID {
            bail!(
                "invalid scheme, expected: {}, received: {}",
                S::ID,
                p.new_group.scheme_id,
            );
        }
        let leader_key = Affine::deserialize(leader_key)?;

        if let Err(err) = S::schnorr_verify(&leader_key, &p.new_group.hash(), &p.signature) {
            {
                bail!("Signature for group packet not valid: {err:?}")
            }
        }
        let group: Group<S> = p.new_group.try_into()?;
        let Some(dkg_index) = group.find_index(self.keypair.public()) else {
            bail!(
                "local node {} is not found in group_file",
                self.keypair.public().address()
            )
        };

        Ok(DkgConfig::new(leader_key, group, p.dkg_timeout, dkg_index))
    }
}

impl From<String> for BeaconID {
    fn from(value: String) -> Self {
        Self {
            inner: value.into(),
        }
    }
}

impl<S: Scheme> std::ops::Deref for Beacon<S> {
    type Target = InnerNode<S>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
