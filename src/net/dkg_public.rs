//! This module provides client and server implementations for DkgPublic service.
use super::utils::Address;
use super::utils::ConnectionError;
use super::utils::ToStatus;
use super::utils::URI_SCHEME;

use crate::core::beacon::BeaconCmd;
use crate::core::daemon::Daemon;
use crate::dkg::dkg_handler::Actions;
use crate::transport::ConvertProto;

use crate::protobuf::dkg as protobuf;
use protobuf::dkg_public_client::DkgPublicClient as _DkgPublicClient;
use protobuf::dkg_public_server::DkgPublic;
use protobuf::DkgPacket;
use protobuf::EmptyDkgResponse;
use protobuf::GossipPacket;

use tonic::transport::Channel;
use tonic::Request;
use tonic::Response;
use tonic::Status;

use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;

use http::Uri;
use tokio::sync::oneshot;
use tracing::debug;

/// Implementor for [`DkgPublic`] trait for use with DkgPublicServer
pub struct DkgPublicHandler(Arc<Daemon>);

impl DkgPublicHandler {
    pub(super) fn new(daemon: Arc<Daemon>) -> Self {
        Self(daemon)
    }
}

#[tonic::async_trait]
impl DkgPublic for DkgPublicHandler {
    async fn packet(
        &self,
        request: Request<GossipPacket>,
    ) -> Result<Response<EmptyDkgResponse>, Status> {
        let packet = request.into_inner().validate()?;
        debug!("received gossip packet: {}", packet.data);
        let id = packet.metadata.beacon_id.clone();

        let (tx, rx) = oneshot::channel();
        self.beacons()
            .cmd(
                BeaconCmd::DkgActions(Actions::Gossip(packet, tx.into())),
                &id,
            )
            .await
            .map_err(|err| err.to_status(&id))?;
        rx.await
            .map_err(|err| err.to_status(&id))?
            .map_err(|err| err.to_status(&id))?;

        Ok(Response::new(EmptyDkgResponse {}))
    }

    async fn broadcast_dkg(
        &self,
        request: Request<DkgPacket>,
    ) -> Result<Response<EmptyDkgResponse>, Status> {
        let packet = request.into_inner().validate()?.dkg;
        debug!("received broadcast: {}", packet.bundle);
        let id = packet.metadata.beacon_id.as_str();
        let (tx, rx) = oneshot::channel();
        self.beacons()
            .cmd(
                BeaconCmd::DkgActions(Actions::Broadcast(packet.bundle, tx.into())),
                id,
            )
            .await
            .map_err(|err| err.to_status(id))?;
        rx.await
            .map_err(|err| err.to_status(id))?
            .map_err(|err| err.to_status(id))?;
        Ok(Response::new(EmptyDkgResponse {}))
    }
}

#[allow(dead_code)]
pub struct DkgPublicClient {
    client: _DkgPublicClient<Channel>,
}

impl DkgPublicClient {
    pub async fn new(address: &Address) -> anyhow::Result<Self> {
        let address = format!("{URI_SCHEME}://{}", address.as_str());
        let uri = Uri::from_str(&address)?;
        let channel = Channel::builder(uri)
            .connect()
            .await
            .map_err(|error| ConnectionError { address, error })?;
        let client = _DkgPublicClient::new(channel);

        Ok(Self { client })
    }
}

impl Deref for DkgPublicHandler {
    type Target = Daemon;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
