// Copyright 2023-2025 StorSwift Inc.
// SPDX-License-Identifier: Apache-2.0

//! Client and server implementations for RPC [`Control`] service.
use super::{
    dkg_control::DkgControlHandler,
    utils::{Callback, NewTcpListener, ToStatus, ERR_METADATA_IS_MISSING},
};
use crate::{
    cli::SyncConfig,
    core::{beacon::BeaconCmd, daemon::Daemon},
    protobuf::{
        dkg::dkg_control_server::DkgControlServer,
        drand::{
            control_client::ControlClient as ControlClientInner,
            control_server::{Control, ControlServer},
            ChainInfoPacket, ChainInfoRequest, LoadBeaconRequest, LoadBeaconResponse, Metadata,
            ShutdownRequest, ShutdownResponse, StartSyncRequest, StatusRequest, StatusResponse,
            SyncProgress,
        },
    },
};
use anyhow::anyhow;
use std::{ops::Deref, pin::Pin, sync::Arc};
use tokio::time::Instant;
use tokio_stream::{
    wrappers::{ReceiverStream, TcpListenerStream},
    Stream,
};
use tonic::{
    transport::{Channel, Server},
    Request, Response, Status,
};

pub const DEFAULT_CONTROL_PORT: &str = "8888";
pub const CONTROL_HOST: &str = "127.0.0.1";

/// Control server streaming response reporting sync progress to the control client.
type ResponseStream = Pin<Box<dyn Stream<Item = SyncProgressResponse> + Send>>;

/// Result type yielded by the sync progress response stream.
pub type SyncProgressResponse = Result<SyncProgress, tonic::Status>;

/// Implementor for [`Control`] trait for use with `ControlServer`.
pub struct ControlHandler(Arc<Daemon>);

#[tonic::async_trait]
impl Control for ControlHandler {
    /// Server streaming response type for the `start_check_chain` method.
    type StartCheckChainStream = ResponseStream;

    /// Server streaming response type for the `start_follow_chain` method.
    type StartFollowChainStream = ResponseStream;

    /// Status responds with the actual status of drand process.
    async fn status(
        &self,
        request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        // Borrow id from metadata.
        let id = request.get_ref().metadata.as_ref().map_or_else(
            || Err(Status::data_loss(ERR_METADATA_IS_MISSING)),
            |meta| Ok(meta.beacon_id.as_str()),
        )?;
        let (tx, rx) = Callback::new();
        self.beacons()
            .cmd(BeaconCmd::Status(tx), id)
            .await
            .map_err(|err| err.to_status(id))?;

        // Await response from callback
        let status = rx
            .await
            .map_err(|recv_err| recv_err.to_status(id))?
            .map_err(|err| err.to_status(id))?;

        Ok(Response::new(status))
    }

    /// ChainInfo returns the chain info for the chain hash or beacon id requested in the metadata.
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

    async fn shutdown(
        &self,
        request: Request<ShutdownRequest>,
    ) -> Result<Response<ShutdownResponse>, Status> {
        let metadata = if let Some(m) = request.into_inner().metadata {
            if self.is_last_id_to_shutdown(&m.beacon_id) {
                self.stop_daemon()
                    .await
                    .map_err(|err| err.to_status(&m.beacon_id))?;
                None
            } else {
                self.stop_id(&m.beacon_id)
                    .await
                    .map_err(|err| err.to_status(&m.beacon_id))?;
                Some(Metadata::with_id(m.beacon_id))
            }
        } else {
            self.stop_daemon()
                .await
                .map_err(|err| Status::unknown(err.to_string()))?;
            None
        };

        Ok(Response::new(ShutdownResponse { metadata }))
    }

    async fn load_beacon(
        &self,
        request: Request<LoadBeaconRequest>,
    ) -> Result<Response<LoadBeaconResponse>, Status> {
        let id = request.get_ref().metadata.as_ref().map_or_else(
            || Err(Status::data_loss(ERR_METADATA_IS_MISSING)),
            |meta| Ok(meta.beacon_id.as_str()),
        )?;

        self.load_id(id).map_err(|err| err.to_status(id))?;
        let response = LoadBeaconResponse {
            metadata: Some(Metadata::with_id(id.to_string())),
        };

        Ok(Response::new(response))
    }

    async fn start_follow_chain(
        &self,
        request: Request<StartSyncRequest>,
    ) -> Result<Response<Self::StartFollowChainStream>, Status> {
        let request = request.into_inner();
        let id = request.metadata.as_ref().map_or_else(
            || Err(Status::data_loss(ERR_METADATA_IS_MISSING)),
            |meta| Ok(meta.beacon_id.clone()),
        )?;
        let (tx, rx) = Callback::new();

        self.beacons()
            .cmd(BeaconCmd::Follow(request, tx), &id)
            .await
            .map_err(|err| Status::unknown(err.to_string()))?;

        let stream_rx = rx
            .await
            .map_err(|err| Status::unknown(err.to_string()))?
            .map_err(|err| Status::unknown(err.to_string()))?;
        Ok(Response::new(Box::pin(ReceiverStream::new(stream_rx))))
    }

    // TODO: this method is required.
    async fn start_check_chain(
        &self,
        _request: Request<StartSyncRequest>,
    ) -> Result<Response<Self::StartCheckChainStream>, Status> {
        Err(Status::unimplemented("start_check_chain: StartSyncRequest"))
    }
}

pub async fn start<N: NewTcpListener>(
    daemon: Arc<Daemon>,
    control: N::Config,
) -> anyhow::Result<()> {
    let listener = N::bind(control)
        .await
        .map_err(|err| anyhow!("failed to start control server: {err}"))?;
    let token = daemon.cancellation_token();

    Server::builder()
        .add_service(ControlServer::new(ControlHandler(daemon.clone())))
        .add_service(DkgControlServer::new(DkgControlHandler::new(
            daemon.clone(),
        )))
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
            let () = token.cancelled().await;
        })
        .await
        .map_err(|err| anyhow!("control server: {err}"))?;

    Ok(())
}

/// Control client capable of issuing proto commands to a running daemon.
pub struct ControlClient {
    client: ControlClientInner<Channel>,
}

impl ControlClient {
    pub async fn new(port: &str) -> anyhow::Result<Self> {
        let address = format!("http://{CONTROL_HOST}:{port}");
        let channel = Channel::from_shared(address)?.connect().await?;
        let client = ControlClientInner::new(channel);

        Ok(Self { client })
    }

    pub async fn status(&mut self, beacon_id: String) -> anyhow::Result<StatusResponse> {
        let request = StatusRequest {
            metadata: Some(Metadata::with_id(beacon_id)),
        };
        let responce = self.client.status(request).await?;
        Ok(responce.into_inner())
    }

    pub async fn load_beacon(&mut self, beacon_id: String) -> anyhow::Result<()> {
        let request = LoadBeaconRequest {
            metadata: Some(Metadata::with_id(beacon_id)),
        };
        let _ = self.client.load_beacon(request).await?;

        Ok(())
    }

    pub async fn shutdown(
        &mut self,
        beacon_id: Option<String>,
    ) -> anyhow::Result<Option<Metadata>> {
        let metadata = beacon_id.map(Metadata::with_id);
        let request = ShutdownRequest { metadata };
        let responce = self.client.shutdown(request).await?;

        Ok(responce.into_inner().metadata)
    }

    pub async fn sync(&mut self, c: SyncConfig) -> anyhow::Result<()> {
        use std::io::Write;
        let metadata = Metadata::with_chain_hash(&c.id, &c.chain_hash)?;
        let request = StartSyncRequest {
            nodes: c.sync_nodes,
            up_to: if c.follow { 0 } else { c.up_to },
            metadata: Some(metadata),
        };
        println!(
            "Launching a follow request: nodes: {:?}, upTo: {}, hash {}, beaconID: {}",
            request.nodes, request.up_to, c.chain_hash, c.id
        );

        let mut responce = self.client.start_follow_chain(request).await?.into_inner();
        let mut spinner = ['/', '—', '\\'].iter().cycle();
        let start = Instant::now();

        while let Ok(Some(progress)) = responce.message().await {
            if progress.current == progress.target || progress.current % 300 == 0 {
                #[allow(clippy::cast_precision_loss)]
                let percent = (progress.current as f64 / progress.target as f64) * 100.0;
                let symbol = spinner.next().expect("infallible");
                print!(
                    "\r{}  synced round up to {} - current target {}     --> {:.2} % ",
                    symbol, progress.current, progress.target, percent,
                );
                std::io::stdout().flush()?;
            }
        }
        println!("time total {}s", start.elapsed().as_secs());

        Ok(())
    }

    pub async fn chain_info(&mut self, beacon_id: String) -> anyhow::Result<ChainInfoPacket> {
        let request = ChainInfoRequest {
            metadata: Some(Metadata::with_id(beacon_id)),
        };
        let info = self.client.chain_info(request).await?.into_inner();

        Ok(info)
    }
}

impl Deref for ControlHandler {
    type Target = Daemon;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
