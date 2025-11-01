//! Client and server implementations for RPC [`Control`] service.

use super::dkg_control::DkgControlHandler;
use super::utils::Callback;
use super::utils::NewTcpListener;
use super::utils::StartServerError;
use super::utils::ToStatus;
use super::utils::ERR_METADATA_IS_MISSING;

use crate::cli::SyncConfig;
use crate::core::beacon::BeaconCmd;
use crate::core::daemon::Daemon;
use crate::protobuf::dkg::dkg_control_server::DkgControlServer;
use crate::protobuf::drand as protobuf;

use protobuf::control_client::ControlClient as _ControlClient;
use protobuf::control_server::Control;
use protobuf::control_server::ControlServer;
use protobuf::BackupDbRequest;
use protobuf::BackupDbResponse;
use protobuf::ChainInfoPacket;
use protobuf::ChainInfoRequest;
use protobuf::GroupPacket;
use protobuf::GroupRequest;
use protobuf::ListSchemesRequest;
use protobuf::ListSchemesResponse;
use protobuf::LoadBeaconRequest;
use protobuf::LoadBeaconResponse;
use protobuf::Metadata;
use protobuf::Ping;
use protobuf::Pong;
use protobuf::PublicKeyRequest;
use protobuf::PublicKeyResponse;
use protobuf::RemoteStatusRequest;
use protobuf::RemoteStatusResponse;
use protobuf::ShutdownRequest;
use protobuf::ShutdownResponse;
use protobuf::StartSyncRequest;
use protobuf::StatusRequest;
use protobuf::StatusResponse;
use protobuf::SyncProgress;

use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::transport::Server;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tracing::debug;
use tracing::error;

use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::Stream;

pub const DEFAULT_CONTROL_PORT: &str = "8888";
pub const CONTROL_HOST: &str = "127.0.0.1";

/// Control server streaming response reporting sync progress to the control client.
type ResponseStream = Pin<Box<dyn Stream<Item = SyncProgressResponse> + Send>>;

/// Result type yielded by the sync progress response stream.
pub type SyncProgressResponse = Result<SyncProgress, tonic::Status>;

/// Implementor for [`Control`] trait for use with `ControlServer`
pub struct ControlHandler(Arc<Daemon>);

#[tonic::async_trait]
impl Control for ControlHandler {
    /// Server streaming response type for the `start_check_chain` method
    type StartCheckChainStream = ResponseStream;

    /// Server streaming response type for the `start_follow_chain` method
    type StartFollowChainStream = ResponseStream;

    /// PingPong simply responds with an empty packet,
    /// proving that this drand node is up and alive.
    async fn ping_pong(&self, _request: Request<Ping>) -> Result<Response<Pong>, Status> {
        debug!("control listener: received ping_pong request");
        let metadata = Some(Metadata::with_default());

        Ok(Response::new(Pong { metadata }))
    }

    /// Status responds with the actual status of drand process
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
        let chain_info = rx
            .await
            .map_err(|recv_err| recv_err.to_status(id))?
            .map_err(|chain_info_err| chain_info_err.to_status(id))?;

        Ok(Response::new(chain_info))
    }

    /// ListSchemes responds with the list of ids for the available schemes
    async fn list_schemes(
        &self,
        _request: Request<ListSchemesRequest>,
    ) -> Result<Response<ListSchemesResponse>, Status> {
        Err(Status::unimplemented("list_schemes: ListSchemesRequest"))
    }

    /// PublicKey returns the longterm public key of the drand node
    async fn public_key(
        &self,
        _request: Request<PublicKeyRequest>,
    ) -> Result<Response<PublicKeyResponse>, Status> {
        Err(Status::unimplemented("public_key: PublicKeyRequest"))
    }

    /// ChainInfo returns the chain info for the chain hash or beacon id requested in the metadata
    async fn chain_info(
        &self,
        request: Request<ChainInfoRequest>,
    ) -> Result<Response<ChainInfoPacket>, Status> {
        // Borrow id from metadata.
        let id = request.get_ref().metadata.as_ref().map_or_else(
            || Err(Status::data_loss(ERR_METADATA_IS_MISSING)),
            |meta| Ok(meta.beacon_id.as_str()),
        )?;

        let (tx, rx) = Callback::new();
        self.beacons()
            .cmd(BeaconCmd::ChainInfo(tx), id)
            .await
            .map_err(|err| err.to_status(id))?;

        // Await response from callback
        let chain_info = rx
            .await
            .map_err(|recv_err| recv_err.to_status(id))?
            .map_err(|chain_info_err| chain_info_err.to_status(id))?;

        Ok(Response::new(chain_info))
    }

    /// GroupFile returns the TOML-encoded group file, containing the group public key and coefficients
    async fn group_file(
        &self,
        _request: Request<GroupRequest>,
    ) -> Result<Response<GroupPacket>, Status> {
        Err(Status::unimplemented("group_file: GroupRequest"))
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

    async fn start_check_chain(
        &self,
        _request: Request<StartSyncRequest>,
    ) -> Result<Response<Self::StartCheckChainStream>, Status> {
        Err(Status::unimplemented("start_check_chain: StartSyncRequest"))
    }

    async fn backup_database(
        &self,
        _request: Request<BackupDbRequest>,
    ) -> Result<Response<BackupDbResponse>, Status> {
        Err(Status::unimplemented("backup_database: BackupDbRequest"))
    }

    async fn remote_status(
        &self,
        _request: Request<RemoteStatusRequest>,
    ) -> Result<Response<RemoteStatusResponse>, Status> {
        Err(Status::unimplemented("remote_status: RemoteStatusRequest"))
    }
}

pub async fn start<N: NewTcpListener>(
    daemon: Arc<Daemon>,
    control: N::Config,
) -> Result<(), StartServerError> {
    let listener = N::bind(control).await.map_err(|err| {
        error!(
            "listener: {}, {err}",
            StartServerError::FailedToStartControl,
        );
        StartServerError::FailedToStartControl
    })?;
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
        .map_err(|err| {
            error!("{}, {err}", StartServerError::FailedToStartControl);
            StartServerError::FailedToStartControl
        })?;

    debug!("control server is shutting down");

    Ok(())
}

/// Control client capable of issuing proto commands to a running daemon.
pub struct ControlClient {
    client: _ControlClient<Channel>,
}

impl ControlClient {
    pub async fn new(port: &str) -> anyhow::Result<Self> {
        let address = format!("http://{CONTROL_HOST}:{port}");
        let channel = Channel::from_shared(address)?.connect().await?;
        let client = _ControlClient::new(channel);

        Ok(Self { client })
    }

    // TODO: add ping_pong.
    #[allow(dead_code)]
    pub async fn ping_pong(&mut self) -> anyhow::Result<()> {
        let request = Ping {
            metadata: Some(Metadata::with_default()),
        };
        let _ = self.client.ping_pong(request).await?;

        Ok(())
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

        tracing::info!(
            "Launching a follow request: nodes: {:?}, upTo: {}, hash {}, beaconID: {}",
            request.nodes,
            request.up_to,
            c.chain_hash,
            c.id
        );

        let mut responce = self.client.start_follow_chain(request).await?.into_inner();
        let mut spinner = ['/', '—', '\\'].iter().cycle();

        while let Ok(Some(progress)) = responce.message().await {
            if progress.current % 300 == 0 {
                #[allow(clippy::cast_precision_loss)]
                let percent = (progress.current as f64 / progress.target as f64) * 100.0;
                let symbol = spinner.next().expect("infallible");
                print!(
                    "\r{}  synced round up to {} - current target {}     --> {:.2} %",
                    symbol, progress.current, progress.target, percent,
                );
                std::io::stdout().flush()?;
            }
        }

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
