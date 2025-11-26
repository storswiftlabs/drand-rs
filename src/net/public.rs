//! Client and server implementations for RPC [`Public`] service.
use super::utils::{Address, Callback, ToStatus, ERR_METADATA_IS_MISSING};
use crate::{
    core::{beacon::BeaconCmd, daemon::Daemon},
    protobuf::drand::{
        public_client::PublicClient as PublicClientInner, public_server::Public, ChainInfoPacket,
        ChainInfoRequest, ListBeaconIDsRequest, ListBeaconIDsResponse, Metadata, PublicRandRequest,
        PublicRandResponse,
    },
};
use anyhow::{bail, Context};
use std::{ops::Deref, pin::Pin, sync::Arc};
use tokio_stream::Stream;
use tonic::{transport::Channel, Request, Response, Status};

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
    /// Server streaming response type for the `public_rand_stream` method.
    type PublicRandStreamStream = ResponseStream;

    /// TODO: implement if we need to support relays in same way.
    async fn public_rand(
        &self,
        _request: Request<PublicRandRequest>,
    ) -> Result<Response<PublicRandResponse>, Status> {
        Err(Status::unimplemented("public_rand: PublicRandRequest"))
    }

    /// TODO: implement if we need to support relays in same way.
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

    // TODO: this method required.
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
    client: PublicClientInner<Channel>,
}

impl PublicClient {
    pub async fn new(address: &Address) -> anyhow::Result<Self> {
        let channel = super::utils::connect(address).await?;
        let client = PublicClientInner::new(channel);
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
                "received chain_info response with invalid beacon_id: expected {beacon_id}, received {}",
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
