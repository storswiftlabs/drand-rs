//! Client and server implementations [`DkgPublic`] service.

use super::utils::Address;
use super::utils::Callback;
use super::utils::ToStatus;
use crate::core::beacon::Actions;
use crate::core::beacon::BeaconCmd;
use crate::core::daemon::Daemon;

use crate::protobuf::dkg as protobuf;
use crate::protobuf::dkg::dkg_public_client::DkgPublicClient as _DkgPublicClient;
use crate::transport::ConvertProto;
use protobuf::dkg_public_server::DkgPublic;
use protobuf::DkgPacket;
use protobuf::EmptyDkgResponse;
use protobuf::GossipPacket;
use tonic::transport::Channel;

use std::ops::Deref;
use std::sync::Arc;
use tonic::Request;
use tonic::Response;
use tonic::Status;

/// Implementor for [`DkgPublic`] trait for use with `DkgPublicServer`.
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
        let id = packet.metadata.beacon_id.clone();

        let (tx, rx) = Callback::new();
        self.beacons()
            .cmd(BeaconCmd::DkgActions(Actions::Gossip(packet, tx)), &id)
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
        let packet = request.into_inner();
        let id = &packet.get_id()?;
        let (tx, rx) = Callback::new();
        self.beacons()
            .cmd(BeaconCmd::DkgActions(Actions::Broadcast(packet, tx)), id)
            .await
            .map_err(|err| err.to_status(id))?;
        rx.await
            .map_err(|err| err.to_status(id))?
            .map_err(|err| err.to_status(id))?;
        Ok(Response::new(EmptyDkgResponse {}))
    }
}

pub struct DkgPublicClient {
    client: _DkgPublicClient<Channel>,
}

impl DkgPublicClient {
    pub async fn new(address: &Address) -> anyhow::Result<Self> {
        let channel = super::utils::connect(address).await?;
        let client = _DkgPublicClient::new(channel);
        Ok(Self { client })
    }

    pub async fn broadcast_dkg(&mut self, packet: DkgPacket) -> anyhow::Result<()> {
        let _ = self.client.broadcast_dkg(packet).await?;
        Ok(())
    }
}

impl Deref for DkgPublicHandler {
    type Target = Daemon;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
