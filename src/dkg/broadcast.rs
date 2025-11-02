use crate::{
    key::Scheme,
    net::{dkg_public::DkgPublicClient, utils::Address},
    protobuf::{
        dkg::{packet::Bundle as ProtoBundle, DkgPacket},
        drand::Metadata,
    },
    transport::dkg::Participant,
};
use energon::{
    kyber::dkg::{
        Bundle, BundleReceiver, Deal, DealBundle, Justification, JustificationBundle, Response,
        ResponseBundle,
    },
    points::KeyPoint,
    traits::{Affine, ScalarField},
};
use tokio::sync::broadcast;
use tokio_util::task::TaskTracker;
use tracing::{debug, error, Span};

#[derive(Clone)]
pub(super) enum BroadcastCmd {
    /// Stop broadcast once dkg is finished or aborted.
    // TODO: abort DKG
    #[allow(dead_code)]
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

            let mut rx = self.sender.subscribe();
            debug!(parent: &self.log, "dkg broadcast: added new address [{}]", p.address);
            let peer = p.address.clone();
            t.spawn(async move {
                let mut conn_result = DkgPublicClient::new(&peer).await;

                while let Ok(msg) = rx.recv().await {
                    match msg {
                        BroadcastCmd::Stop => break,
                        BroadcastCmd::Packet(packet) => {
                            if conn_result.is_err() {
                                conn_result = DkgPublicClient::new(&peer).await;
                            }

                            match conn_result {
                                Ok(ref mut client) => {
                                    if let Err(err) = client.broadcast_dkg(packet).await {
                                        error!("dkg broadcast: send packet to {peer}: {err}");
                                    }
                                }
                                Err(ref err) => {
                                    error!("dkg broadcast: connect to {peer}: {err}");
                                }
                            };
                        }
                    }
                }
            });
        }

        t.spawn(async move {
            while let Some(bundle) = rx.recv().await {
                let Ok(proto) = into_proto(bundle, &self.beacon_id) else {
                    error!(parent: &self.log, "dkg broadcast: failed to convert bundle to proto");
                    return;
                };
                if self.sender.send(BroadcastCmd::Packet(proto)).is_err() {
                    error!(parent: &self.log, "dkg broadcast: channel is closed");
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

pub enum ConvertError {
    /// Optional fields related to prost implementation.
    /// See <https://github.com/tokio-rs/prost?tab=readme-ov-file#field-modifiers>
    EmptyProto,
    FromInner,
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
            metadata: Some(Metadata::with_id(id.to_string())),
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
                metadata: None,
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
                share: S::Scalar::from_bytes_be(&j.share).map_err(|_| ConvertError::FromInner)?,
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
                    .map_err(|_| ConvertError::FromInner)?
                    .into(),
            });
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
                .map_err(|_| ConvertError::FromInner)?,
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
            commits.push(c.serialize().map_err(|_| ConvertError::FromInner)?.into());
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
