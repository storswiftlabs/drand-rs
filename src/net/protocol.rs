//! This module provides server implementations for Protocol.

use crate::protobuf::drand as protobuf;
use protobuf::protocol_server::Protocol;
use protobuf::BeaconPacket;
use protobuf::Empty;
use protobuf::IdentityRequest;
use protobuf::IdentityResponse;
use protobuf::PartialBeaconPacket;
use protobuf::StatusRequest;
use protobuf::StatusResponse;
use protobuf::SyncRequest;

use std::pin::Pin;
use tokio_stream::Stream;
use tonic::Request;
use tonic::Response;
use tonic::Status;

type ResponseStream = Pin<Box<dyn Stream<Item = Result<BeaconPacket, Status>> + Send>>;

/// Implementor for [`Protocol`] trait for use with ProtocolServer
pub struct ProtocolHandler;

#[tonic::async_trait]
impl Protocol for ProtocolHandler {
    /// Server streaming response type for the `sync_chain` method
    type SyncChainStream = ResponseStream;

    /// Returns the identity of beacon id
    async fn get_identity(
        &self,
        _request: Request<IdentityRequest>,
    ) -> Result<Response<IdentityResponse>, Status> {
        Err(Status::unimplemented("get_identity: IdentityRequest"))
    }

    async fn partial_beacon(
        &self,
        _request: Request<PartialBeaconPacket>,
    ) -> Result<Response<Empty>, Status> {
        Err(Status::unimplemented("partial_beacon: PartialBeaconPacket"))
    }

    async fn sync_chain(
        &self,
        _request: Request<SyncRequest>,
    ) -> Result<Response<Self::SyncChainStream>, Status> {
        Err(Status::unimplemented("sync_chain: SyncRequest"))
    }

    async fn status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        Err(Status::unimplemented("status: StatusRequest"))
    }
}
