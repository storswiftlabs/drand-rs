//! This module provides server and client implementations for RPC Public.

use super::utils::Address;
use super::utils::Callback;
use super::utils::ToStatus;
use super::utils::ERR_METADATA_IS_MISSING;
use crate::core::beacon::BeaconCmd;
use crate::core::daemon::Daemon;
use crate::protobuf::drand as protobuf;
use crate::protobuf::drand::Metadata;

use protobuf::public_client::PublicClient as _PublicClient;
use protobuf::public_server::Public;
use protobuf::ChainInfoPacket;
use protobuf::ChainInfoRequest;
use protobuf::ListBeaconIDsRequest;
use protobuf::ListBeaconIDsResponse;
use protobuf::PublicRandRequest;
use protobuf::PublicRandResponse;

use anyhow::bail;
use anyhow::Context;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::Stream;
use tonic::transport::Channel;
use tonic::Request;
use tonic::Response;
use tonic::Status;

type ResponseStream = Pin<Box<dyn Stream<Item = Result<PublicRandResponse, Status>> + Send>>;

/// Implementor for [`Public`] trait for use with `PublicServer`.
pub struct PublicHandler(pub(super) Arc<Daemon>);

impl PublicHandler {
    pub fn new(daemon: Arc<Daemon>) -> Self {
        Self(daemon)
    }
}

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
        request: Request<ChainInfoRequest>,
    ) -> Result<Response<ChainInfoPacket>, Status> {
        let id = request.get_ref().metadata.as_ref().map_or_else(
            || Err(Status::data_loss(ERR_METADATA_IS_MISSING)),
            |meta| Ok(meta.beacon_id.as_str()),
        )?;

        let (tx, rx) = Callback::new();
        self.beacons()
            .cmd(BeaconCmd::ChainInfo(tx), id)
            .await
            .map_err(|err| err.to_status(id))?;

        let chain_info = rx
            .await
            .map_err(|recv_err| recv_err.to_status(id))?
            .map_err(|chain_info_err| chain_info_err.to_status(id))?;

        Ok(Response::new(chain_info))
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

pub struct PublicClient {
    client: _PublicClient<Channel>,
}

impl PublicClient {
    pub async fn new(address: &Address) -> anyhow::Result<Self> {
        let channel = super::utils::connect(address).await?;
        let client = _PublicClient::new(channel);
        Ok(Self { client })
    }

    pub async fn chain_info(&mut self, beacon_id: String) -> anyhow::Result<ChainInfoPacket> {
        let metadata = Some(Metadata::golang_node_version(beacon_id.clone(), None));
        let request = ChainInfoRequest { metadata };
        let response = self.client.chain_info(request).await?.into_inner();

        // Add error context if metadata is not consistent.
        let metadata = response
            .metadata
            .as_ref()
            .context("received chain_info response without metadata")?;
        if metadata.beacon_id != beacon_id {
            bail!(
                "received chain_info response with invalid beacon id, expected: {beacon_id}, received: {}",
                metadata.beacon_id
            )
        }
        if metadata.chain_hash.len() != 32 {
            bail!(
                "invalid chain-hash of received chain_info response: expected len 32, received {}",
                metadata.chain_hash.len()
            )
        }

        Ok(response)
    }
}

impl Deref for PublicHandler {
    type Target = Daemon;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
