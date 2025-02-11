//! This module provides server implementations for Public.

use crate::core::daemon::Daemon;
use crate::protobuf::drand as protobuf;
use protobuf::public_server::Public;
use protobuf::ChainInfoPacket;
use protobuf::ChainInfoRequest;
use protobuf::ListBeaconIDsRequest;
use protobuf::ListBeaconIDsResponse;
use protobuf::PublicRandRequest;
use protobuf::PublicRandResponse;

use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;

use tokio_stream::Stream;
use tonic::Request;
use tonic::Response;
use tonic::Status;

type ResponseStream = Pin<Box<dyn Stream<Item = Result<PublicRandResponse, Status>> + Send>>;

/// Implementor for [`Public`] trait for use with PublicServer
pub struct PublicHandler(Arc<Daemon>);

#[tonic::async_trait]
impl Public for PublicHandler {
    /// Server streaming response type for the `public_rand_stream` method
    type PublicRandStreamStream = ResponseStream;

    async fn public_rand(
        &self,
        _request: Request<PublicRandRequest>,
    ) -> Result<Response<PublicRandResponse>, Status> {
        Err(Status::unimplemented("public_rand: PublicRandRequest"))
    }

    async fn public_rand_stream(
        &self,
        _request: Request<PublicRandRequest>,
    ) -> Result<Response<Self::PublicRandStreamStream>, Status> {
        Err(Status::unimplemented(
            "public_rand_stream: PublicRandRequest",
        ))
    }

    async fn chain_info(
        &self,
        _request: Request<ChainInfoRequest>,
    ) -> Result<Response<ChainInfoPacket>, Status> {
        Err(Status::unimplemented("chain_info: ChainInfoRequest"))
    }

    async fn list_beacon_i_ds(
        &self,
        _request: Request<ListBeaconIDsRequest>,
    ) -> Result<Response<ListBeaconIDsResponse>, Status> {
        Err(Status::unimplemented(
            "list_beacon_i_ds: ListBeaconIDsRequest",
        ))
    }
}

impl Deref for PublicHandler {
    type Target = Daemon;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
