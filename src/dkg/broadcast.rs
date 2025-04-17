use crate::key::Scheme;
use crate::net::utils::Address;
use crate::net::utils::URI_SCHEME;

use crate::protobuf::dkg::dkg_public_client::DkgPublicClient;
use crate::protobuf::dkg::packet::Bundle as ProtoBundle;
use crate::protobuf::dkg::DkgPacket;
use crate::protobuf::drand::Metadata;
use crate::transport::dkg::Participant;

use energon::backends::error::BackendsError;
use energon::kyber::dkg::protocol::Bundle;
use energon::kyber::dkg::structs::*;
use energon::kyber::dkg::BundleReceiver;
use energon::points::KeyPoint;
use energon::traits::Affine;
use energon::traits::ScalarField;

use std::str::FromStr;
use tokio::sync::broadcast;
use tokio_util::task::TaskTracker;
use tonic::transport::Channel;
use tracing::debug;
use tracing::error;
use tracing::Span;

#[derive(Clone)]
pub(super) enum BroadcastCmd {
    /// Stop broadcast once dkg is finished or aborted.
    Stop,
    /// Packet to broadcast for other nodes
    Packet(DkgPacket),
}

pub(super) struct Broadcast {
    sender: broadcast::Sender<BroadcastCmd>,
    beacon_id: String,
    log: Span,
}

impl Broadcast {
    pub(super) fn init(id: &str, log: &Span) -> Self {
        let (sender, _) = broadcast::channel::<BroadcastCmd>(1);

        Self {
            sender,
            beacon_id: id.to_owned(),
            log: log.to_owned(),
        }
    }

    pub(super) fn register_nodes<S: Scheme>(
        self,
        t: &TaskTracker,
        participants: &[&Participant],
        mut rx: BundleReceiver<S>,
        me: &Address,
    ) {
        for p in participants {
            if &p.address == me {
                continue;
            }
            if let Some(uri) = to_uri(&p.address) {
                let addr = p.address.to_string();
                let mut rx = self.sender.subscribe();

                t.spawn(async move {
                    let mut client = DkgPublicClient::new(Channel::builder(uri).connect_lazy());

                    while let Ok(msg) = rx.recv().await {
                        match msg {
                            BroadcastCmd::Stop => break,
                            BroadcastCmd::Packet(packet) => {
                                if let Err(err) = client.broadcast_dkg(packet).await {
                                    error!("failed to send dkg packet to {addr}, error: {err}")
                                }
                            }
                        }
                    }
                });
            }
        }

        t.spawn(async move {
            while let Some(bundle) = rx.recv().await {
                match into_proto(bundle, &self.beacon_id) {
                    Ok(proto) => {
                        debug!(parent: &self.log,"push ");
                        if self.sender.send(BroadcastCmd::Packet(proto)).is_err() {
                            error!(parent: &self.log, "BUG: broadcast: failed to send proto bundle, channel is closed")
                        }
                    }
                    Err(err) => {
                         error!(parent: &self.log, "broadcast: failed to convert bundle to proto: {err:?}")
                    }
                }
            }
        });
    }
}

/// Helper trait to convert [`Bundle`] from/into generic protocol type.
pub(super) trait Convert: Sized {
    type Proto;

    fn from_proto(p: Self::Proto) -> Result<Self, ConvertError>;
    fn into_proto(self) -> Result<Self::Proto, ConvertError>;
}

#[derive(Debug)]
pub enum ConvertError {
    /// Optional fields related to prost implementation.
    /// See <https://github.com/tokio-rs/prost?tab=readme-ov-file#field-modifiers>
    EmptyProto,
    #[allow(non_camel_case_types)]
    BUG_FromInner(BackendsError),
}

/// Convert bundle into protobuf representation
fn into_proto<S: Scheme>(bundle: Bundle<S>, id: &str) -> Result<DkgPacket, ConvertError> {
    let proto_bundle = match bundle {
        Bundle::Deal(d) => ProtoBundle::Deal(d.into_proto()?),
        Bundle::Response(r) => ProtoBundle::Response(r.into_proto()?),
        Bundle::Justification(j) => ProtoBundle::Justification(j.into_proto()?),
    };

    let packet = DkgPacket {
        dkg: Some(crate::protobuf::dkg::Packet {
            metadata: Metadata::with_id(id),
            bundle: Some(proto_bundle),
        }),
    };

    Ok(packet)
}

impl<S: Scheme> Convert for Bundle<S> {
    type Proto = DkgPacket;

    fn from_proto(p: Self::Proto) -> Result<Self, ConvertError> {
        let proto_bundle = p
            .dkg
            .ok_or(ConvertError::EmptyProto)?
            .bundle
            .ok_or(ConvertError::EmptyProto)?;

        let inner = match proto_bundle {
            ProtoBundle::Deal(d) => Bundle::Deal(Convert::from_proto(d)?),
            ProtoBundle::Response(r) => Bundle::Response(Convert::from_proto(r)?),
            ProtoBundle::Justification(j) => Bundle::Justification(Convert::from_proto(j)?),
        };

        Ok(inner)
    }

    fn into_proto(self) -> Result<Self::Proto, ConvertError> {
        let bundle = match self {
            Bundle::Deal(d) => ProtoBundle::Deal(d.into_proto()?),
            Bundle::Response(r) => ProtoBundle::Response(r.into_proto()?),
            Bundle::Justification(j) => ProtoBundle::Justification(j.into_proto()?),
        };

        Ok(Self::Proto {
            dkg: Some(crate::protobuf::dkg::Packet {
                metadata: Metadata::with_id("kva"),
                bundle: Some(bundle),
            }),
        })
    }
}

impl<S: Scheme> Convert for JustificationBundle<S> {
    type Proto = crate::protobuf::dkg::JustificationBundle;

    fn from_proto(p: Self::Proto) -> Result<Self, ConvertError> {
        let mut justifications = Vec::with_capacity(p.justifications.len());

        for j in p.justifications {
            justifications.push(Justification::<S> {
                share_index: j.share_index,
                share: S::Scalar::from_bytes_be(&j.share).map_err(ConvertError::BUG_FromInner)?,
            });
        }

        let inner = Self {
            dealer_index: p.dealer_index,
            justifications,
            session_id: p.session_id,
            signature: p.signature,
        };

        Ok(inner)
    }

    fn into_proto(self) -> Result<Self::Proto, ConvertError> {
        let mut justifications = Vec::with_capacity(self.justifications.len());

        for j in self.justifications {
            justifications.push(crate::protobuf::dkg::Justification {
                share_index: j.share_index,
                share: j
                    .share
                    .to_bytes_be()
                    .map_err(ConvertError::BUG_FromInner)?
                    .into(),
            })
        }

        let proto = Self::Proto {
            dealer_index: self.dealer_index,
            justifications,
            session_id: self.session_id,
            signature: self.signature,
        };

        Ok(proto)
    }
}

impl<S: Scheme> Convert for DealBundle<S> {
    type Proto = crate::protobuf::dkg::DealBundle;

    fn from_proto(p: Self::Proto) -> Result<Self, ConvertError> {
        let inner = Self {
            dealer_index: p.dealer_index,
            public: p
                .commits
                .iter()
                .map(|commit| KeyPoint::<S>::deserialize(commit))
                .collect::<Result<Vec<_>, _>>()
                .map_err(ConvertError::BUG_FromInner)?,
            deals: p
                .deals
                .into_iter()
                .map(|d| Deal {
                    share_index: d.share_index,
                    encrypted_share: d.encrypted_share,
                })
                .collect(),
            session_id: p.session_id,
            signature: p.signature,
        };

        Ok(inner)
    }

    fn into_proto(self) -> Result<Self::Proto, ConvertError> {
        let mut commits = Vec::with_capacity(self.public.len());

        for c in self.public {
            commits.push(c.serialize().map_err(ConvertError::BUG_FromInner)?.into());
        }

        let proto = Self::Proto {
            dealer_index: self.dealer_index,
            commits,
            deals: self
                .deals
                .into_iter()
                .map(|d| crate::protobuf::dkg::Deal {
                    share_index: d.share_index,
                    encrypted_share: d.encrypted_share,
                })
                .collect(),
            session_id: self.session_id,
            signature: self.signature,
        };

        Ok(proto)
    }
}

impl Convert for ResponseBundle {
    type Proto = crate::protobuf::dkg::ResponseBundle;

    fn from_proto(p: Self::Proto) -> Result<Self, ConvertError> {
        let inner = Self {
            share_index: p.share_index,
            responses: p
                .responses
                .into_iter()
                .map(|r| Response {
                    dealer_index: r.dealer_index,
                    status: r.status,
                })
                .collect(),
            session_id: p.session_id,
            signature: p.signature,
        };

        Ok(inner)
    }

    fn into_proto(self) -> Result<Self::Proto, ConvertError> {
        let proto = Self::Proto {
            share_index: self.share_index,
            responses: self
                .responses
                .into_iter()
                .map(|r| crate::protobuf::dkg::Response {
                    dealer_index: r.dealer_index,
                    status: r.status,
                })
                .collect(),
            session_id: self.session_id,
            signature: self.signature,
        };

        Ok(proto)
    }
}

pub fn to_uri(addr: &Address) -> Option<http::Uri> {
    // TODO: change to builder after TLS is added
    http::Uri::from_str(&format!("{URI_SCHEME}://{}", addr.as_str())).ok()
}
