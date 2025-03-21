//! This module provides client and server implementations for DkgControl service.

use super::utils::ToStatus;
use crate::core::beacon::BeaconCmd;
use crate::core::daemon::Daemon;
use crate::dkg::process::Actions;
use crate::net::control::CONTROL_HOST;
use crate::net::utils::ConnectionError;

use crate::protobuf::dkg as protobuf;
use crate::transport::ConvertProto;
use protobuf::dkg_control_client::DkgControlClient as _DkgControlClient;
use protobuf::dkg_control_server::DkgControl;
use protobuf::CommandMetadata;
use protobuf::DkgCommand;
use protobuf::DkgStatusRequest;
use protobuf::DkgStatusResponse;
use protobuf::EmptyDkgResponse;
use protobuf::JoinOptions;

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
        request: Request<DkgCommand>,
    ) -> Result<Response<EmptyDkgResponse>, Status> {
        let inner = request.into_inner().validate()?;
        let id = inner.metadata.beacon_id.as_str();
        let (tx, rx) = oneshot::channel();
        let cmd = Actions::Command(inner.command, tx.into());

        self.beacons()
            .cmd(BeaconCmd::DkgActions(cmd), id)
            .await
            .map_err(|err| err.to_status(id))?;

        rx.await
            .map_err(|err| err.to_status(id))?
            .map_err(|err| err.to_status(id))?;

        Ok(Response::new(EmptyDkgResponse {}))
    }

    async fn dkg_status(
        &self,
        request: Request<DkgStatusRequest>,
    ) -> Result<Response<DkgStatusResponse>, tonic::Status> {
        let id = request.get_ref().beacon_id.as_str();
        let (tx, rx) = oneshot::channel();

        self.beacons()
            .cmd(BeaconCmd::DkgActions(Actions::Status(tx.into())), id)
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

    pub async fn dkg_join(&mut self, beacon_id: &str) -> anyhow::Result<()> {
        let request = DkgCommand {
            metadata: Some(CommandMetadata {
                beacon_id: beacon_id.to_owned(),
            }),
            command: Some(protobuf::dkg_command::Command::Join(JoinOptions {
                group_file: vec![],
            })),
        };
        let _ = self.client.command(request).await?;

        Ok(())
    }
}

impl Deref for DkgControlHandler {
    type Target = Daemon;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
