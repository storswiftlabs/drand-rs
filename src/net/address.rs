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

use super::transport::Bundle;
use crate::core::beacon::BeaconID;
use crate::protobuf::drand::protocol_client::ProtocolClient;
use crate::protobuf::drand::DkgPacket;

use anyhow::bail;
use http::uri::Authority;
use http::uri::PathAndQuery;
use http::uri::Scheme;
use http::uri::Uri;
use std::fmt::Debug;
use std::fmt::Display;
use tokio::sync::broadcast;
use tracing::*;

/// Wrapper over URI which satisfies additional prechecks
// TODO: this type should be used across APP
pub struct Address(pub Uri);

impl Address {
    pub fn new_checked(addr: &str, tls: bool) -> anyhow::Result<Self> {
        let mut parts = http::uri::Parts::default();

        if let Ok(a) = addr.parse::<Authority>() {
            if !a.host().is_empty() && a.port().is_some() {
                parts.authority = Some(a)
            }
        };

        if parts.authority.is_none() {
            bail!("parsed address err: expected $HOST:PORT, received {addr}",)
        }

        parts.scheme = match tls {
            true => Some(Scheme::HTTPS),
            false => Some(Scheme::HTTP),
        };

        parts.path_and_query = Some(PathAndQuery::from_static("/"));
        let uri = Uri::from_parts(parts)?;

        Ok(Self(uri))
    }
}

impl Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let authority = self.0.authority().expect("always contains authority");
        let scheme = self.0.scheme_str().expect("always contains scheme");
        write!(f, "{scheme}://{authority}")
    }
}

impl Debug for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let authority = self.0.authority().expect("always contains authority");
        write!(f, "{authority}")
    }
}

pub trait Pool: Sized {
    type Msg;
    const CHANNEL_CAPACITY: usize;
    fn enable(span: &Span, addresses: Vec<&str>, tls: bool) -> anyhow::Result<Self>;
    fn add_connection(span: tracing::Span, addr: Address, rx: broadcast::Receiver<DkgPacket>);
}

pub struct DkgBroadcast {
    pub tx: broadcast::Sender<DkgPacket>,
}

impl DkgBroadcast {
    pub fn send(&self, bundle: Bundle, beacon_id: &BeaconID) -> anyhow::Result<()> {
        if self
            .tx
            .send(bundle.to_proto(beacon_id.to_string()))
            .is_err()
        {
            bail!("DkgBroadcast: channel closed: {beacon_id}")
        }
        Ok(())
    }
}

pub struct ChainBroadcast;

impl Pool for DkgBroadcast {
    type Msg = DkgPacket;
    const CHANNEL_CAPACITY: usize = 3;

    fn enable(span: &Span, addresses: Vec<&str>, tls: bool) -> anyhow::Result<Self> {
        debug!(parent: span ,"dkg pool: received nodes to connect: {addresses:?}");

        if addresses.is_empty() {
            bail!("dkg pool: no addresses provided")
        };
        let mut uris = Vec::with_capacity(addresses.len());

        for addr in addresses.iter() {
            match Address::new_checked(addr, tls) {
                Ok(addr) => uris.push(addr),
                Err(e) => warn!(parent: span, "dkg pool: invalid address: {addr}: {e:?}"),
            };
        }

        if uris.is_empty() {
            bail!("dkg pool: no valid addresses provided")
        }
        let (tx, _) = broadcast::channel::<Self::Msg>(Self::CHANNEL_CAPACITY);

        for uri in uris.into_iter() {
            Self::add_connection(span.to_owned(), uri, tx.subscribe());
        }

        Ok(Self { tx })
    }

    fn add_connection(span: tracing::Span, addr: Address, mut rx: broadcast::Receiver<DkgPacket>) {
        tokio::spawn(async move {
            let mut conn_unchecked = ProtocolClient::connect(addr.to_string()).await;

            while let Ok(msg) = rx.recv().await {
                if let Err(e) = conn_unchecked {
                    warn!(parent: &span, "failed to connect {addr:?}: {e}, retrying...");
                    conn_unchecked = ProtocolClient::connect(addr.to_string()).await
                }

                match conn_unchecked {
                    Ok(ref mut conn) => {
                        if let Err(e) = conn.broadcast_dkg(msg).await {
                            error!(parent: &span, "sending out to: {addr:?}, status: {e}")
                        }
                    }
                    Err(ref e) => {
                        error!(parent: &span, "failed to connect {addr:?}: {e}")
                    }
                }
            }
            debug!(parent: &span, "DkgBroadcast disabled for {addr}")
        });
    }
}
