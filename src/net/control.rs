//! This module provides server implementations for Control.

use crate::protobuf::drand as protobuf;
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

use tokio_stream::Stream;
use tonic::Request;
use tonic::Response;
use tonic::Status;

type ResponseStream = std::pin::Pin<Box<dyn Stream<Item = Result<SyncProgress, Status>> + Send>>;

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
