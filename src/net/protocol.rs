//! This module provides server implementations for Protocol.
use crate::net::public::PublicHandler;
use crate::net::utils::Address;
use crate::net::utils::ConnectionError;
use crate::net::utils::NewTcpListener;
use crate::net::utils::StartServerError;
use crate::net::utils::ToStatus;
use crate::net::utils::ERR_METADATA_IS_MISSING;
use crate::net::utils::URI_SCHEME;

use crate::protobuf::drand as protobuf;
use crate::protobuf::drand::public_server::PublicServer;
use crate::protobuf::drand::Metadata;
use crate::store::Store;
use crate::transport::utils::ConvertProto;

use crate::core::beacon::BeaconCmd;
use crate::core::daemon::Daemon;

use futures::SinkExt;
use protobuf::protocol_client::ProtocolClient as _ProtocolClient;
use protobuf::protocol_server::Protocol;
use protobuf::protocol_server::ProtocolServer;
use protobuf::BeaconPacket;
use protobuf::Empty;
use protobuf::IdentityRequest;
use protobuf::IdentityResponse;
use protobuf::PartialBeaconPacket;
use protobuf::StatusRequest;
use protobuf::StatusResponse;
use protobuf::SyncRequest;

use tonic::transport::Channel;
use tonic::transport::Server;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;
use tracing::debug;
use tracing::error;

use http::Uri;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;

type ResponseStream = futures::channel::mpsc::UnboundedReceiver<Result<BeaconPacket, Status>>;

/// Implementor for [`Protocol`] trait for use with ProtocolServer
pub struct ProtocolHandler(Arc<Daemon>);

#[tonic::async_trait]
impl Protocol for ProtocolHandler {
    /// Server streaming response type for the `sync_chain` method
    type SyncChainStream = ResponseStream;

    /// Returns the identity of beacon id
    /// Returns the identity of beacon id.
    async fn get_identity(
        &self,
        request: Request<IdentityRequest>,
    ) -> Result<Response<IdentityResponse>, Status> {
        debug!("Received identity request");
        // Borrow id from metadata.
        let id = request.get_ref().metadata.as_ref().map_or_else(
            || Err(Status::data_loss(ERR_METADATA_IS_MISSING)),
            |meta| Ok(meta.beacon_id.as_str()),
        )?;

        // Send identity request to multibeacon handlers.
        let (tx, rx) = oneshot::channel();
        self.beacons()
            .cmd(BeaconCmd::IdentityRequest(tx), id)
            .await
            .map_err(|err| err.to_status(id))?;

        // Await response from callback
        let mut identity = rx
            .await
            .map_err(|recv_err| recv_err.to_status(id))?
            .map_err(|cmd_err| cmd_err.to_status(id))?;
        identity.metadata = Metadata::with_id(id);

        Ok(Response::new(identity))
    }

    async fn partial_beacon(
        &self,
        _request: Request<PartialBeaconPacket>,
    ) -> Result<Response<Empty>, Status> {
        Err(Status::unimplemented("partial_beacon: PartialBeaconPacket"))
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
        debug!("protocol: received follow_chain request for {id}");

        // Get store pointer from beacon process
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.beacons()
            .cmd(BeaconCmd::Sync(tx), id)
            .await
            .map_err(|err| err.to_status(id))?;
        let store = rx
            .await
            .map_err(|err| err.to_status(id))?
            .map_err(Status::aborted)?;

        let (mut tx, rx) = futures::channel::mpsc::unbounded();

        debug!("starting sync chain for {id}");
        tokio::spawn(async move {
            let mut start = request.from_round;

            while let Ok(beacon) = store.get(start).await {
                let packet = BeaconPacket {
                    previous_signature: beacon.previous_sig,
                    round: beacon.round,
                    signature: beacon.signature,
                    metadata: request.metadata.clone(),
                };
                if tx.send(Ok(packet)).await.is_err() {
                    let _ = tx.send(Err(Status::ok(""))).await;
                    return;
                }
                start += 1;
            }

            let _ = tx.send(Err(Status::data_loss("end of stream"))).await;
        });

        // let out_stream = tokio_stream::wrappers::ReceiverStream::new(rx);

        Ok(Response::new(rx))
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

    // TODO: update health_service with _health_reporter
    let (_health_reporter, health_service) = tonic_health::server::health_reporter();
    Server::builder()
        .add_service(ProtocolServer::new(ProtocolHandler(daemon.clone())))
        .add_service(PublicServer::new(PublicHandler::new(daemon)))
        .add_service(health_service)
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
            debug!("Node server started");
            let _ = cancel.cancelled().await;
            debug!("Node server: received shutdown request");
        })
        .await
        .map_err(|err| {
            error!("{}, {err}", StartServerError::FailedToStartNode);
            StartServerError::FailedToStartNode
        })?;
    debug!("Node server is shutting down");
    Ok(())
}

pub struct ProtocolClient {
    client: _ProtocolClient<Channel>,
}

impl ProtocolClient {
    pub async fn new(address: &Address) -> anyhow::Result<Self> {
        let address = format!("{URI_SCHEME}://{}", address.as_str());
        let uri = Uri::from_str(&address)?;
        let channel = Channel::builder(uri)
            .connect()
            .await
            .map_err(|error| ConnectionError { address, error })?;
        let client = _ProtocolClient::new(channel);

        Ok(Self { client })
    }

    pub async fn get_identity(
        &mut self,
        beacon_id: &str,
    ) -> anyhow::Result<crate::transport::drand::IdentityResponse> {
        let request = IdentityRequest {
            metadata: Metadata::with_id(beacon_id),
        };
        let response = self.client.get_identity(request).await?;
        let inner = response.into_inner().validate()?;

        Ok(inner)
    }

    /// SyncRequest forces a daemon to sync up its chain with other nodes
    pub async fn sync_chain(
        &mut self,
        from_round: u64,
        beacon_id: &str,
    ) -> Result<Streaming<BeaconPacket>, tonic::Status> {
        let request = SyncRequest {
            from_round,
            metadata: Metadata::with_id(beacon_id),
        };
        let stream = self.client.sync_chain(request).await?.into_inner();

        Ok(stream)
    }
}

impl Deref for ProtocolHandler {
    type Target = Daemon;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
