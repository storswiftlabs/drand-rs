//! Client and server implementations for RPC [`Protocol`] service.
use super::{
    dkg_public::DkgPublicHandler,
    public::PublicHandler,
    utils::{Address, Callback, NewTcpListener, ToStatus, ERR_METADATA_IS_MISSING},
};
use crate::{
    chain::ChainError,
    core::{beacon::BeaconCmd, daemon::Daemon},
    debug,
    protobuf::{
        dkg::dkg_public_server::DkgPublicServer,
        drand::{
            protocol_client::ProtocolClient as ProtocolClientInner,
            protocol_server::{Protocol, ProtocolServer},
            public_server::PublicServer,
            BeaconPacket, Empty, IdentityRequest, IdentityResponse, Metadata, PartialBeaconPacket,
            SyncRequest,
        },
    },
    transport::utils::ConvertProto,
};
use anyhow::anyhow;
use std::{ops::Deref, pin::Pin, sync::Arc};
use tokio_stream::{
    wrappers::{ReceiverStream, TcpListenerStream},
    Stream,
};
use tonic::{
    transport::{Channel, Server},
    Request, Response, Status, Streaming,
};

/// Alias for partial beacon packet with callback.
pub type PartialMsg = (PartialPacket, Callback<bool, ChainError>);

/// Implementor for [`Protocol`] trait for use with [`ProtocolServer`].
pub struct ProtocolHandler(Arc<Daemon>);

/// Contains partial beacon packet and sender IP.
pub struct PartialPacket {
    pub packet: PartialBeaconPacket,
    // X-REAL-IP from request metadata.
    pub from: String,
}

#[tonic::async_trait]
impl Protocol for ProtocolHandler {
    /// Server streaming response type for the `sync_chain` method.
    type SyncChainStream = Pin<Box<dyn Stream<Item = Result<BeaconPacket, Status>> + Send>>;

    /// Returns the identity of beacon id.
    async fn get_identity(
        &self,
        request: Request<IdentityRequest>,
    ) -> Result<Response<IdentityResponse>, Status> {
        let id = request.get_ref().metadata.as_ref().map_or_else(
            || Err(Status::data_loss(ERR_METADATA_IS_MISSING)),
            |meta| Ok(meta.beacon_id.as_str()),
        )?;

        let (tx, rx) = Callback::new();
        self.beacons()
            .cmd(BeaconCmd::IdentityRequest(tx), id)
            .await
            .map_err(|err| err.to_status(id))?;

        let mut identity = rx
            .await
            .map_err(|recv_err| recv_err.to_status(id))?
            .map_err(|cmd_err| cmd_err.to_status(id))?;
        identity.metadata = Some(Metadata::with_id(id.to_string()));

        Ok(Response::new(identity))
    }

    async fn partial_beacon(
        &self,
        request: Request<PartialBeaconPacket>,
    ) -> Result<Response<Empty>, Status> {
        let from = request
            .metadata()
            .get("x-real-ip")
            .map_or_else(|| "", |v| v.to_str().unwrap_or_default())
            .to_string();
        let packet = request.into_inner();
        let round = packet.round;
        let id = packet.metadata.as_ref().map_or_else(
            || Err(Status::data_loss(ERR_METADATA_IS_MISSING)),
            |meta| Ok(meta.beacon_id.as_str()),
        )?;
        debug!(
            self.log(),
            "received partial: id {id}, round {round}, from {from}"
        );
        let request_latency_metadata: Option<_> = self.latency_tx().check_id(id);
        let (tx, rx) = Callback::new();

        self.beacons()
            .send_partial((PartialPacket { packet, from }, tx))
            .await
            .map_err(|err| Status::unknown(err.to_string()))?;
        let is_aggregated = rx
            .await
            .map_err(|err| Status::unknown(err.to_string()))?
            .map_err(|err| Status::unknown(err.to_string()))?;

        if is_aggregated {
            if let Some(tracked_id) = request_latency_metadata {
                let delay = tracked_id.elapsed_ms();
                self.latency_tx().send(tracked_id, delay, round).await;
            }
        }

        Ok(Response::new(Empty { metadata: None }))
    }

    async fn sync_chain(
        &self,
        request: Request<SyncRequest>,
    ) -> Result<Response<Self::SyncChainStream>, Status> {
        let request = request.into_inner();

        let id = request.metadata.as_ref().map_or_else(
            || Err(Status::data_loss(ERR_METADATA_IS_MISSING)),
            |meta| Ok(meta.beacon_id.as_str()),
        )?;
        let (tx, rx) = Callback::new();

        self.beacons()
            .cmd(BeaconCmd::Sync(request.from_round, tx), id)
            .await
            .map_err(|err| Status::unknown(err.to_string()))?;
        let stream_rx = rx
            .await
            .map_err(|err| Status::unknown(err.to_string()))?
            .map_err(|err| Status::unknown(err.to_string()))?;

        Ok(Response::new(Box::pin(ReceiverStream::new(stream_rx))))
    }
}

pub async fn start_node<N: NewTcpListener>(
    daemon: Arc<Daemon>,
    node_listener: N::Config,
) -> anyhow::Result<()> {
    let listener = N::bind(node_listener)
        .await
        .map_err(|err| anyhow!("failed to start node: {err}"))?;
    let token = daemon.cancellation_token();

    let (_health_reporter, health_service) = tonic_health::server::health_reporter();
    Server::builder()
        .add_service(ProtocolServer::new(ProtocolHandler(daemon.clone())))
        .add_service(PublicServer::new(PublicHandler::new(daemon.clone())))
        .add_service(DkgPublicServer::new(DkgPublicHandler::new(daemon)))
        .add_service(health_service)
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
            let () = token.cancelled().await;
        })
        .await?;

    Ok(())
}

#[derive(Clone)]
pub struct ProtocolClient {
    client: ProtocolClientInner<Channel>,
}

impl ProtocolClient {
    pub async fn new(address: &Address) -> anyhow::Result<Self> {
        let channel = super::utils::connect(address).await?;
        let client = ProtocolClientInner::new(channel);

        Ok(Self { client })
    }

    /// Does not attempt to connect to the endpoint until first use.
    /// Should be used only in pool for partial packets.
    pub fn new_lazy(address: &Address) -> anyhow::Result<Self> {
        let channel = super::utils::connect_lazy(address)?;
        let client = ProtocolClientInner::new(channel);

        Ok(Self { client })
    }

    pub async fn get_identity(
        &mut self,
        beacon_id: String,
    ) -> anyhow::Result<crate::transport::drand::IdentityResponse> {
        let request = IdentityRequest {
            metadata: Some(Metadata::golang_node_version(beacon_id, None)),
        };
        let response = self.client.get_identity(request).await?;
        let inner = response.into_inner().validate()?;

        Ok(inner)
    }

    pub async fn sync_chain(
        &mut self,
        from_round: u64,
        beacon_id: String,
    ) -> anyhow::Result<Streaming<BeaconPacket>> {
        let request = SyncRequest {
            from_round,
            metadata: Some(Metadata::with_id(beacon_id)),
        };
        let stream = self.client.sync_chain(request).await?.into_inner();

        Ok(stream)
    }

    pub async fn partial_beacon(&mut self, packet: PartialBeaconPacket) -> anyhow::Result<()> {
        if let Err(err) = self.client.partial_beacon(packet).await {
            anyhow::bail!("{}", err.message());
        };

        Ok(())
    }
}

impl Deref for ProtocolHandler {
    type Target = Daemon;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
