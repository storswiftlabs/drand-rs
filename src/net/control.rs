//! This module provides client and server implementations for Control.

use crate::net::utils::ConnectionError;
use crate::protobuf::drand as protobuf;

use protobuf::control_client::ControlClient as _ControlClient;
use protobuf::control_server::Control;
use protobuf::BackupDbRequest;
use protobuf::BackupDbResponse;
use protobuf::ChainInfoPacket;
use protobuf::ChainInfoRequest;
use protobuf::GroupPacket;
use protobuf::GroupRequest;
use protobuf::ListSchemesRequest;
use protobuf::ListSchemesResponse;
use protobuf::LoadBeaconRequest;
use protobuf::LoadBeaconResponse;
use protobuf::Metadata;
use protobuf::Ping;
use protobuf::Pong;
use protobuf::PublicKeyRequest;
use protobuf::PublicKeyResponse;
use protobuf::RemoteStatusRequest;
use protobuf::RemoteStatusResponse;
use protobuf::ShutdownRequest;
use protobuf::ShutdownResponse;
use protobuf::StartSyncRequest;
use protobuf::StatusRequest;
use protobuf::StatusResponse;
use protobuf::SyncProgress;

use anyhow::Result;
use http::Uri;
use std::str::FromStr;
use tokio_stream::Stream;
use tonic::transport::Channel;
use tonic::Request;
use tonic::Response;
use tonic::Status;

type ResponseStream = std::pin::Pin<Box<dyn Stream<Item = Result<SyncProgress, Status>> + Send>>;

pub const DEFAULT_CONTROL_PORT: &str = "8888";
pub const CONTROL_HOST: &str = "127.0.0.1";

/// Implementor for [`Control`] trait for use with `ControlServer`
pub struct ControlHandler;

#[tonic::async_trait]
impl Control for ControlHandler {
    /// Server streaming response type for the `start_check_chain` method
    type StartCheckChainStream = ResponseStream;

    /// Server streaming response type for the `start_follow_chain` method
    type StartFollowChainStream = ResponseStream;

    /// PingPong simply responds with an empty packet,
    /// proving that this drand node is up and alive.
    async fn ping_pong(&self, _request: Request<Ping>) -> Result<Response<Pong>, Status> {
        Err(Status::unimplemented("ping_pong: Ping"))
    }

    /// Status responds with the actual status of drand process
    async fn status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        Err(Status::unimplemented("status: StatusRequest"))
    }

    /// ListSchemes responds with the list of ids for the available schemes
    async fn list_schemes(
        &self,
        _request: Request<ListSchemesRequest>,
    ) -> Result<Response<ListSchemesResponse>, Status> {
        Err(Status::unimplemented("list_schemes: ListSchemesRequest"))
    }

    /// PublicKey returns the longterm public key of the drand node
    async fn public_key(
        &self,
        _request: Request<PublicKeyRequest>,
    ) -> Result<Response<PublicKeyResponse>, Status> {
        Err(Status::unimplemented("public_key: PublicKeyRequest"))
    }

    /// ChainInfo returns the chain info for the chain hash or beacon id requested in the metadata
    async fn chain_info(
        &self,
        _request: Request<ChainInfoRequest>,
    ) -> Result<Response<ChainInfoPacket>, Status> {
        Err(Status::unimplemented("chain_info: ChainInfoRequest"))
    }

    /// GroupFile returns the TOML-encoded group file, containing the group public key and coefficients
    async fn group_file(
        &self,
        _request: Request<GroupRequest>,
    ) -> Result<Response<GroupPacket>, Status> {
        Err(Status::unimplemented("group_file: GroupRequest"))
    }

    // Metadata is None: stop the daemon
    // Metadata is Some: stop the beacon_id, if stopped beacon_id was the last - stop the daemon.
    async fn shutdown(
        &self,
        _request: Request<ShutdownRequest>,
    ) -> Result<Response<ShutdownResponse>, Status> {
        Err(Status::unimplemented("shutdown: ShutdownRequest"))
    }

    async fn load_beacon(
        &self,
        _request: Request<LoadBeaconRequest>,
    ) -> Result<Response<LoadBeaconResponse>, Status> {
        Err(Status::unimplemented("load_beacon: LoadBeaconRequest"))
    }

    async fn start_follow_chain(
        &self,
        _request: Request<StartSyncRequest>,
    ) -> Result<Response<Self::StartFollowChainStream>, Status> {
        Err(Status::unimplemented(
            "start_follow_chain: StartSyncRequest",
        ))
    }

    async fn start_check_chain(
        &self,
        _request: Request<StartSyncRequest>,
    ) -> Result<Response<Self::StartCheckChainStream>, Status> {
        Err(Status::unimplemented("start_check_chain: StartSyncRequest"))
    }

    async fn backup_database(
        &self,
        _request: Request<BackupDbRequest>,
    ) -> Result<Response<BackupDbResponse>, Status> {
        Err(Status::unimplemented("backup_database: BackupDbRequest"))
    }

    async fn remote_status(
        &self,
        _request: Request<RemoteStatusRequest>,
    ) -> Result<Response<RemoteStatusResponse>, Status> {
        Err(Status::unimplemented("remote_status: RemoteStatusRequest"))
    }
}

/// Control client capable of issuing proto commands to a [`DEFAULT_CONTROL_HOST`] running daemon
pub struct ControlClient {
    client: _ControlClient<Channel>,
}

impl ControlClient {
    pub async fn new(port: &str) -> Result<Self> {
        let address = format!("grpc://{CONTROL_HOST}:{port}");
        let uri = Uri::from_str(&address)?;
        let channel = Channel::builder(uri)
            .connect()
            .await
            .map_err(|error| ConnectionError { address, error })?;
        let client = _ControlClient::new(channel);

        Ok(Self { client })
    }

    pub async fn ping_pong(&mut self) -> Result<()> {
        let request = Ping {
            metadata: Metadata::with_default(),
        };
        let _ = self.client.ping_pong(request).await?;

        Ok(())
    }

    pub async fn load_beacon(&mut self, beacon_id: &str) -> Result<()> {
        let request = LoadBeaconRequest {
            metadata: Metadata::with_id(beacon_id),
        };
        let _ = self.client.load_beacon(request).await?;

        Ok(())
    }
}
