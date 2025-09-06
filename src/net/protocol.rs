//! This module provides server and client implementations for Protocol.
use super::dkg_public::DkgPublicHandler;
use super::public::PublicHandler;
use super::utils::Address;
use super::utils::Callback;
use super::utils::NewTcpListener;
use super::utils::StartServerError;
use super::utils::ToStatus;
use super::utils::ERR_METADATA_IS_MISSING;

use crate::chain::ChainError;
use crate::core::beacon::BeaconCmd;
use crate::core::daemon::Daemon;
use crate::protobuf::dkg::dkg_public_server::DkgPublicServer;
use crate::protobuf::drand as protobuf;
use crate::transport::utils::ConvertProto;

use protobuf::protocol_client::ProtocolClient as _ProtocolClient;
use protobuf::protocol_server::Protocol;
use protobuf::protocol_server::ProtocolServer;
use protobuf::public_server::PublicServer;
use protobuf::BeaconPacket;
use protobuf::Empty;
use protobuf::IdentityRequest;
use protobuf::IdentityResponse;
use protobuf::PartialBeaconPacket;
use protobuf::StatusRequest;
use protobuf::StatusResponse;
use protobuf::SyncRequest;

use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::Stream;
use tonic::transport::Channel;
use tonic::transport::Server;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;
use tracing::error;

/// Contains partial beacon packet and sender IP.
pub struct PartialPacket {
    pub packet: PartialBeaconPacket,
    // X-REAL-IP from request metadata.
    pub from: String,
}

/// Alias for partial beacon packet with callback.
pub type PartialMsg = (PartialPacket, Callback<(), ChainError>);

/// Implementor for [`Protocol`] trait for use with `ProtocolServer`.
pub struct ProtocolHandler(Arc<Daemon>);

#[tonic::async_trait]
impl Protocol for ProtocolHandler {
    /// Server streaming response type for the `sync_chain` method
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
        identity.metadata = Some(protobuf::Metadata::with_id(id.to_string()));

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

        let partial = PartialPacket {
            packet: request.into_inner(),
            from,
        };
        let (tx, rx) = Callback::new();

        self.beacons()
            .send_partial((partial, tx))
            .await
            .map_err(|err| Status::unknown(err.to_string()))?;
        rx.await
            .map_err(|err| Status::unknown(err.to_string()))?
            .map_err(|err| Status::unknown(err.to_string()))?;

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

    async fn status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        Err(Status::unimplemented("status: StatusRequest"))
    }
}

pub async fn start_server<N: NewTcpListener>(
    daemon: Arc<Daemon>,
    node_listener: N::Config,
) -> Result<(), StartServerError> {
    let listener = N::bind(node_listener).await.map_err(|err| {
        error!("listener: {}, {}", StartServerError::FailedToStartNode, err);
        StartServerError::FailedToStartNode
    })?;
    let cancel = daemon.token.clone();

    let (_health_reporter, health_service) = tonic_health::server::health_reporter();
    Server::builder()
        .add_service(ProtocolServer::new(ProtocolHandler(daemon.clone())))
        .add_service(PublicServer::new(PublicHandler::new(daemon.clone())))
        .add_service(DkgPublicServer::new(DkgPublicHandler::new(daemon)))
        .add_service(health_service)
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
            let () = cancel.cancelled().await;
        })
        .await
        .map_err(|err| {
            error!("{}, {err}", StartServerError::FailedToStartNode);
            StartServerError::FailedToStartNode
        })?;

    Ok(())
}

#[derive(Clone)]
pub struct ProtocolClient {
    client: _ProtocolClient<Channel>,
}

impl ProtocolClient {
    pub async fn new(address: &Address) -> anyhow::Result<Self> {
        let channel = super::utils::connect(address).await?;
        let client = _ProtocolClient::new(channel);

        Ok(Self { client })
    }

    pub async fn get_identity(
        &mut self,
        beacon_id: String,
    ) -> anyhow::Result<crate::transport::drand::IdentityResponse> {
        let request = IdentityRequest {
            metadata: Some(protobuf::Metadata::golang_node_version(beacon_id, None)),
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
            metadata: Some(protobuf::Metadata::with_id(beacon_id)),
        };
        let stream = self.client.sync_chain(request).await?.into_inner();

        Ok(stream)
    }

    pub async fn partial_beacon(&mut self, packet: PartialBeaconPacket) -> anyhow::Result<()> {
        let _ = self.client.partial_beacon(packet).await?;

        Ok(())
    }
}

impl Deref for ProtocolHandler {
    type Target = Daemon;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
