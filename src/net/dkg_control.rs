//! This module provides client and server implementations for DkgControl service.

use super::utils::ToStatus;
use crate::core::beacon::BeaconCmd;
use crate::core::daemon::Daemon;
use crate::dkg::dkg_handler::DkgActions;
use crate::net::control::CONTROL_HOST;
use crate::net::utils::ConnectionError;

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
use std::str::FromStr;
use std::sync::Arc;

use http::Uri;
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
    client: _DkgControlClient<Channel>,
}

impl DkgControlClient {
    pub async fn new(port: &str) -> anyhow::Result<Self> {
        let address = format!("grpc://{CONTROL_HOST}:{port}");
        let uri = Uri::from_str(&address)?;
        let channel = Channel::builder(uri)
            .connect()
            .await
            .map_err(|error| ConnectionError { address, error })?;
        let client = _DkgControlClient::new(channel);

        Ok(Self { client })
    }

    pub async fn dkg_status(&mut self, beacon_id: &str) -> anyhow::Result<DkgStatusResponse> {
        let request = DkgStatusRequest {
            beacon_id: beacon_id.to_owned(),
        };
        let response = self.client.dkg_status(request).await?;
        let inner = response.into_inner();

        Ok(inner)
    }
}

impl Deref for DkgControlHandler {
    type Target = Daemon;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
