//! Client and server implementations for RPC [`DkgPublic`] service.
use super::utils::{Address, Callback, ToStatus};
use crate::{
    core::{
        beacon::{Actions, BeaconCmd},
        daemon::Daemon,
    },
    protobuf::dkg::{
        dkg_public_client::DkgPublicClient as DkgPublicClientInner, dkg_public_server::DkgPublic,
        DkgPacket, EmptyDkgResponse, GossipPacket,
    },
    transport::ConvertProto,
};
use std::{ops::Deref, sync::Arc};
use tonic::{transport::Channel, Request, Response, Status};

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
    client: DkgPublicClientInner<Channel>,
}

impl DkgPublicClient {
    pub async fn new(address: &Address) -> anyhow::Result<Self> {
        let channel = super::utils::connect(address).await?;
        let client = DkgPublicClientInner::new(channel);
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
