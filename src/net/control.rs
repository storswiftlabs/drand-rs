//! This module provides client and server implementations for Control.

use crate::net::protocol::ProtocolClient;
use crate::net::utils::Address;
use crate::net::utils::ConnectionError;
use crate::net::utils::NewTcpListener;
use crate::net::utils::StartServerError;
use crate::net::utils::ToStatus;
use crate::net::utils::ERR_METADATA_IS_MISSING;

use crate::cli::SyncConfig;
use crate::core::beacon::BeaconCmd;
use crate::core::daemon::Daemon;
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

use tonic::transport::Channel;
use tonic::transport::Server;
use tonic::Request;
use tonic::Response;
use tonic::Status;

use tracing::debug;
use tracing::error;
use tracing::info;

use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;

use http::Uri;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::Stream;
use tokio_stream::StreamExt;

type ResponseStream = std::pin::Pin<Box<dyn Stream<Item = Result<SyncProgress, Status>> + Send>>;

pub const DEFAULT_CONTROL_PORT: &str = "8888";
pub const CONTROL_HOST: &str = "127.0.0.1";

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
        debug!("control listener: received ping_pon request");
        let metadata = Metadata::with_default();

        Ok(Response::new(Pong { metadata }))
    }

    /// Status responds with the actual status of drand process
    async fn status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        Err(Status::unimplemented("status: StatusRequest"))
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

        let (tx, rx) = oneshot::channel();
        self.beacons()
            .cmd(BeaconCmd::ChainInfo(tx), id)
            .await
            .map_err(|err| err.to_status(id))?;

        // Await response from callback
        let chain_info = rx
            .await
            .map_err(|recv_err| recv_err.to_status(id))?
            .map_err(Status::not_found)?;

        Ok(Response::new(chain_info))
    }

    /// GroupFile returns the TOML-encoded group file, containing the group public key and coefficients
    async fn group_file(
        &self,
        _request: Request<GroupRequest>,
    ) -> Result<Response<GroupPacket>, Status> {
        Err(Status::unimplemented("group_file: GroupRequest"))
    }

    // Metadata is None: stop the daemon
    // Metadata is Some: stop the beacon_id, if stopped beacon_id was the last - stop the daemon.
    async fn shutdown(
        &self,
        request: Request<ShutdownRequest>,
    ) -> Result<Response<ShutdownResponse>, Status> {
        let metadata = request.get_ref().metadata.as_ref();
        // Callback to indicate if shutdown is graceful
        let (tx_graceful, rx_graceful) = tokio::sync::oneshot::channel::<bool>();

        #[allow(unused_assignments)]
        let mut is_graceful = false;
        let mut is_last_beacon = true;

        // Metadata is Some - request to stop given beacon_id
        if let Some(meta) = metadata {
            is_last_beacon = self
                .stop_id(&meta.beacon_id, tx_graceful)
                .map_err(|err| err.to_status(&meta.beacon_id))?;
            is_graceful = rx_graceful
                .await
                .map_err(|err| err.to_status(&meta.beacon_id))?;
        } else
        // Metadata is None - request to stop the daemon
        {
            self.stop_daemon(tx_graceful);
            is_graceful = rx_graceful
                .await
                .map_err(|_| Status::internal("something is very broken: RecvError"))?;
        }
        if !is_graceful {
            return Err(Status::internal("shutdown is not graceful"));
        }
        // Encode new daemon state, see [`ControlClient::shutdown`]
        let metadata = match is_last_beacon {
            // No more beacons left
            true => None,
            // Daemon is still running
            false => Metadata::with_default(),
        };

        Ok(Response::new(ShutdownResponse { metadata }))
    }

    async fn load_beacon(
        &self,
        request: Request<LoadBeaconRequest>,
    ) -> Result<Response<LoadBeaconResponse>, Status> {
        // Borrow id from metadata.
        let id = request.get_ref().metadata.as_ref().map_or_else(
            || Err(Status::data_loss(ERR_METADATA_IS_MISSING)),
            |meta| Ok(meta.beacon_id.as_str()),
        )?;
        info!(parent: self.log(), "received load id request for {id}");

        // Send request to the daemon
        self.load_id(id).map_err(|err| {
            error!(parent: self.log(), "failed to proceed, {err}: {id}");
            err.to_status(id)
        })?;

        let response = LoadBeaconResponse {
            metadata: Metadata::with_id(id),
        };

        Ok(Response::new(response))
    }

    // Until sync manager is ready.
    async fn start_follow_chain(
        &self,
        request: Request<StartSyncRequest>,
    ) -> Result<Response<Self::StartFollowChainStream>, Status> {
        // Borrow id from metadata.
        let id = request.get_ref().metadata.as_ref().map_or_else(
            || Err(Status::data_loss(ERR_METADATA_IS_MISSING)),
            |meta| Ok(meta.beacon_id.as_str()),
        )?;
        debug!("received follow_chain request for {id}");

        // Get store pointer from beacon process
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.beacons()
            .cmd(BeaconCmd::Sync(tx), id)
            .await
            .map_err(|err| err.to_status(id))?;
        let _store = rx
            .await
            .map_err(|err| err.to_status(id))?
            .map_err(Status::aborted)?;

        // Connect to remote node.
        let inner = request.get_ref();
        let address = inner
            .nodes
            .first()
            .ok_or_else(|| Status::aborted("list of nodes can not be empty"))?;
        let address = Address::precheck(address).map_err(|err| err.to_status(id))?;

        let mut client = ProtocolClient::new(&address)
            .await
            .map_err(|err| Status::from_error(err.into()))?;
        let mut stream = client.sync_chain(inner.up_to, id).await?;

        while let Some(_beacon) = stream.next().await {
            // write to db and stream back the progress
        }

        Err(Status::unimplemented(
            "start_follow_chain: StartSyncRequest",
        ))
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

pub async fn start_server<N: NewTcpListener>(
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
    let cancel = daemon.token.clone();

    Server::builder()
        .add_service(ControlServer::new(ControlHandler(daemon)))
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
            debug!("Control server started");
            let _ = cancel.cancelled().await;
            debug!("Control server: received shutdown request");
        })
        .await
        .map_err(|err| {
            error!("{}, {err}", StartServerError::FailedToStartControl);
            StartServerError::FailedToStartControl
        })?;

    debug!("control server is shutting down");

    Ok(())
}

/// Control client capable of issuing proto commands to a [`DEFAULT_CONTROL_HOST`] running daemon
pub struct ControlClient {
    client: _ControlClient<Channel>,
}

impl ControlClient {
    pub async fn new(port: &str) -> anyhow::Result<Self> {
        let address = format!("grpc://{CONTROL_HOST}:{port}");
        let uri = Uri::from_str(&address)?;
        let channel = Channel::builder(uri)
            .connect()
            .await
            .map_err(|error| ConnectionError { address, error })?;
        let client = _ControlClient::new(channel);

        Ok(Self { client })
    }

    pub async fn ping_pong(&mut self) -> anyhow::Result<()> {
        let request = Ping {
            metadata: Metadata::with_default(),
        };
        let _ = self.client.ping_pong(request).await?;

        Ok(())
    }

    pub async fn load_beacon(&mut self, beacon_id: &str) -> anyhow::Result<()> {
        let request = LoadBeaconRequest {
            metadata: Metadata::with_id(beacon_id),
        };
        let _ = self.client.load_beacon(request).await?;

        Ok(())
    }

    pub async fn shutdown(&mut self, beacon_id: Option<&str>) -> anyhow::Result<bool> {
        let metadata = match beacon_id {
            Some(id) => Metadata::with_id(id),
            None => None,
        };
        let request = ShutdownRequest { metadata };
        let responce = self.client.shutdown(request).await?;
        let is_daemon_running = responce.get_ref().metadata.is_some();
        Ok(is_daemon_running)
    }

    pub async fn sync(&mut self, config: SyncConfig) -> anyhow::Result<()> {
        let metadata = Metadata::with_chain_hash(&config.id, &config.chain_hash)?;
        let request = StartSyncRequest {
            nodes: config.sync_nodes,
            up_to: config.up_to,
            metadata: Some(metadata),
        };

        let mut responce = self.client.start_follow_chain(request).await?.into_inner();

        let mut count: u64 = 0;
        while let Some(item) = responce.next().await {
            count += 1;
            if count % 300 == 0 {
                debug!("received:\n {count}, target: {}", item?.target);
            }
        }

        Ok(())
    }
}

impl Deref for ControlHandler {
    type Target = Daemon;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
