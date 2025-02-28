//! This module provides client and server implementations for DkgControl service.

use super::utils::ToStatus;
use crate::core::beacon::BeaconCmd;
use crate::core::daemon::Daemon;
use crate::dkg::dkg_handler::DkgActions;

use crate::protobuf::dkg as protobuf;
use protobuf::dkg_control_client::DkgControlClient as _DkgControlClient;
use protobuf::dkg_control_server::DkgControl;
use protobuf::DkgCommand;
use protobuf::DkgStatusRequest;
use protobuf::DkgStatusResponse;
use protobuf::EmptyDkgResponse;

use tonic::transport::Channel;
use tonic::Request;
use tonic::Response;
use tonic::Status;

use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::oneshot;

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
        request: Request<DkgStatusRequest>,
    ) -> Result<Response<DkgStatusResponse>, tonic::Status> {
        let id = request.get_ref().beacon_id.as_str();
        let (tx, rx) = oneshot::channel();
        self.beacons()
            .cmd(BeaconCmd::DkgActions(DkgActions::Status(tx)), id)
            .await
            .map_err(|err| err.to_status(id))?;
        let responce = rx
            .await
            .map_err(|err| err.to_status(id))?
            .map_err(|err| err.to_status(id))?;
        Ok(Response::new(responce))
    }
}

pub struct DkgControlClient {
    _client: _DkgControlClient<Channel>,
}

impl Deref for DkgControlHandler {
    type Target = Daemon;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
