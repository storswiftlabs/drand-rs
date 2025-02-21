//! This module provides server implementations for DkgControl and DkgPublic.

use crate::core::daemon::Daemon;
use crate::protobuf::dkg as protobuf;

use protobuf::dkg_control_server::DkgControl;
use protobuf::dkg_public_server::DkgPublic;
use protobuf::DkgCommand;
use protobuf::DkgPacket;
use protobuf::DkgStatusRequest;
use protobuf::DkgStatusResponse;
use protobuf::EmptyDkgResponse;
use protobuf::GossipPacket;

use std::ops::Deref;
use std::sync::Arc;
use tonic::Request;
use tonic::Response;
use tonic::Status;

/// Implementor for [`DkgControl`] trait for use with DkgControlServer
pub struct DkgControlHandler(Arc<Daemon>);

impl DkgControlHandler {
    pub(super) fn new(daemon: Arc<Daemon>) -> Self {
        Self(daemon)
    }
}

#[tonic::async_trait]
impl DkgControl for DkgControlHandler {
    async fn command(
        &self,
        _request: Request<DkgCommand>,
    ) -> Result<Response<EmptyDkgResponse>, Status> {
        Err(Status::unimplemented("command: DkgCommand"))
    }

    async fn dkg_status(
        &self,
        _request: Request<DkgStatusRequest>,
    ) -> Result<Response<DkgStatusResponse>, tonic::Status> {
        Err(Status::unimplemented("dkg_status: DkgStatusRequest"))
    }
}

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
        _request: Request<GossipPacket>,
    ) -> Result<Response<EmptyDkgResponse>, Status> {
        Err(Status::unimplemented("packet: GossipPacket"))
    }

    async fn broadcast_dkg(
        &self,
        _request: Request<DkgPacket>,
    ) -> Result<Response<EmptyDkgResponse>, Status> {
        Err(Status::unimplemented("broadcast_dkg: DkgPacket"))
    }
}

impl Deref for DkgControlHandler {
    type Target = Daemon;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Deref for DkgPublicHandler {
    type Target = Daemon;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
