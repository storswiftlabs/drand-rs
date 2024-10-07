use super::protocol::ProtocolHandler;
use super::public::PublicHandler;
use super::utils;

use super::utils::INTERNAL_ERR;
use crate::core::beacon::BeaconCmd;
use crate::core::beacon::BeaconID;
use crate::core::daemon::Daemon;
use crate::core::schemes_init::Schemes;

use crate::log::Logger;
use crate::protobuf::common::Metadata;
use crate::protobuf::drand::control_client::ControlClient;
use crate::protobuf::drand::control_server::Control;
use crate::protobuf::drand::control_server::ControlServer;
use crate::protobuf::drand::protocol_server::ProtocolServer;
use crate::protobuf::drand::public_server::PublicServer;
use crate::protobuf::drand::InitDkgPacket;
use crate::protobuf::drand::InitDkgResponse;
use crate::protobuf::drand::ListSchemesRequest;
use crate::protobuf::drand::ListSchemesResponse;
use crate::protobuf::drand::LoadBeaconRequest;
use crate::protobuf::drand::LoadBeaconResponse;
use crate::protobuf::drand::PoolInfoRequest;
use crate::protobuf::drand::PoolInfoResponse;
use crate::protobuf::drand::PublicKeyRequest;
use crate::protobuf::drand::PublicKeyResponse;
use crate::protobuf::drand::SetupInfoPacket;
use crate::protobuf::drand::ShutdownRequest;
use crate::protobuf::drand::ShutdownResponse;

use anyhow::bail;
use std::ops::Deref;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use tonic::Request;
use tonic::Response;
use tonic::Status;
use tracing::*;

pub const DEFAULT_CONTROL_PORT: &str = "8888";
pub const CONTROL_HOST: &str = "127.0.0.1";

#[derive(Clone)]
pub struct ControlHandler(Arc<Daemon>);
impl ControlHandler {
    fn new(daemon: Arc<Daemon>) -> Self {
        Self(daemon)
    }
}

#[tonic::async_trait]
// TODO: make whole impl readable, see 'ProtocolHandler'
impl Control for ControlHandler {
    /// InitDKG sends information to daemon to start a fresh DKG protocol
    async fn init_dkg(
        &self,
        req: Request<InitDkgPacket>,
    ) -> Result<Response<InitDkgResponse>, Status> {
        let req = req.into_inner();
        let id: BeaconID = req.metadata.as_ref().into();
        trace!(parent: self.span(), "received DkgInfoPacket, id: {id}");
        if let Some(info) = req.info {
            let (tx, rx) = oneshot::channel();
            if let Err(e) = self.beacons().init_dkg(tx, info.from_proto(), id).await {
                return Err(Status::from_error(e.into()));
            };
            match rx.await.map_err(|e| Status::from_error(e.into()))? {
                Ok(share_path) => Ok(Response::new(InitDkgResponse { share_path })),
                Err(e) => Err(Status::from_error(e.into())),
            }
        } else {
            Err(Status::data_loss("setup_info_packet can't be None"))
        }
    }

    async fn list_schemes(
        &self,
        _req: Request<ListSchemesRequest>,
    ) -> Result<Response<ListSchemesResponse>, Status> {
        let response = ListSchemesResponse {
            ids: Schemes::list_schemes(),
            metadata: None,
        };
        Ok(Response::new(response))
    }

    async fn pool_info(
        &self,
        _req: Request<PoolInfoRequest>,
    ) -> Result<Response<PoolInfoResponse>, Status> {
        trace!(parent: self.span(), "received pool_info request");
        let pool_info = self.pool_status().await?;
        let response = PoolInfoResponse { pool_info };

        Ok(Response::new(response))
    }

    async fn public_key(
        &self,
        req: Request<PublicKeyRequest>,
    ) -> Result<Response<PublicKeyResponse>, Status> {
        let metadata = req.into_inner().metadata;
        let id = utils::get_ref_id(metadata.as_ref());

        let (sender, callback) = oneshot::channel();
        if let Err(e) = self
            .beacons()
            .cmd(BeaconCmd::ShowPublicKey(sender), id)
            .await
        {
            return Err(Status::from_error(e.into()));
        }
        match callback.await {
            Ok(pub_key) => Ok(Response::new(PublicKeyResponse { pub_key, metadata })),
            Err(_) => Err(Status::internal(INTERNAL_ERR)),
        }
    }

    /// Shutdown request:
    ///    - Metadata is None: stop the daemon
    ///    - Metadata is Some: stop the beacon_id, if stopped beacon_id was the last: stop the daemon
    async fn shutdown(
        &self,
        req: Request<ShutdownRequest>,
    ) -> Result<Response<ShutdownResponse>, Status> {
        let mut metadata = req.into_inner().metadata;

        if metadata.is_some() {
            if self
                .stop_id(&metadata.as_ref().into())
                .await
                .map_err(|e| Status::from_error(e.into()))?
            {
                metadata = None
            }
        } else {
            self.stop_daemon()
                .await
                .map_err(|e| Status::from_error(e.into()))?
        }

        Ok(Response::new(ShutdownResponse { metadata }))
    }

    async fn load_beacon(
        &self,
        _req: Request<LoadBeaconRequest>,
    ) -> Result<Response<LoadBeaconResponse>, Status> {
        todo!()
    }
}

async fn start_node(daemon: Arc<Daemon>) -> anyhow::Result<()> {
    let listener = TcpListener::bind(daemon.private_listen())
        .await
        .map_err(|err| anyhow::anyhow!("DBG::start_node::listener err: {}", err))?;
    let mut stop = daemon.stop_daemon.subscribe();

    if let Err(err) = Server::builder()
        .add_service(ProtocolServer::new(ProtocolHandler::new(daemon)))
        .add_service(PublicServer::new(PublicHandler))
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
            let _ = stop.recv().await;
            println!("DBG:: stopped: ProtocolServer, PublicServer")
        })
        .await
    {
        bail!("Node start is failed: {err:?}")
    }
    Ok(())
}

async fn start_control(daemon: Arc<Daemon>, control_port: &str) -> Result<(), anyhow::Error> {
    let listener = TcpListener::bind(control_address_server(control_port)).await?;
    let mut stop = daemon.stop_daemon.subscribe();

    if let Err(err) = Server::builder()
        .add_service(ControlServer::new(ControlHandler::new(daemon)))
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
            let _ = stop.recv().await;
            println!("DBG:: stopped: ControlServer")
        })
        .await
    {
        bail!("Control server start is failed: {err:?}",)
    }

    Ok(())
}

pub async fn start(
    folder: &str,
    id: Option<String>,
    control_port: &str,
    private_listen: String,
) -> anyhow::Result<()> {
    let logger = Logger::register_node(&private_listen);
    info!(parent: &logger.span, "DrandDaemon initializing: private_listen: {private_listen}, control_port: {control_port}, folder: {folder}");
    let daemon = Daemon::new(private_listen, folder, id.as_ref(), logger)?;

    if let Err(err) = tokio::try_join!(
        start_node(Arc::clone(&daemon)),
        start_control(daemon, control_port)
    ) {
        bail!("can't instantiate drand daemon: {err}")
    }

    Ok(())
}

pub async fn stop(control_port: &str, beacon_id: Option<&String>) -> anyhow::Result<()> {
    let mut conn = ControlClient::connect(control_address_client(control_port)).await?;

    let mut metadata = None; // shutdown daemon if beacon_id.is_none()
    if let Some(id) = beacon_id {
        metadata = Some(Metadata {
            beacon_id: id.into(),
            ..Default::default()
        })
    }
    match conn.shutdown(ShutdownRequest { metadata }).await {
        Ok(response) => {
            if let Some(m) = response.get_ref().metadata.as_ref() {
                println!("beacon process [{}] stopped correctly. Bye.\n", m.beacon_id)
            } else {
                println!("drand daemon stopped correctly. Bye.\n")
            }
        }
        Err(err) => {
            if let Some(id) = beacon_id {
                println!(
                    "error stopping beacon process: [{id}], status: {}",
                    err.message()
                )
            } else {
                println!("error stopping drand daemon, status: {}", err.message())
            }
        }
    }

    Ok(())
}

pub async fn public_key_request(
    control_port: &str,
    beacon_id: Option<String>,
) -> anyhow::Result<()> {
    let mut conn = ControlClient::connect(control_address_client(control_port)).await?;

    let mut metadata = None;
    if let Some(beacon_id) = beacon_id {
        metadata = Some(Metadata {
            beacon_id,
            ..Default::default()
        })
    }
    let response = conn.public_key(PublicKeyRequest { metadata }).await?;
    let key: &Vec<u8> = response.get_ref().pub_key.as_ref();
    println!("pubKey: {}", hex::encode(key));
    Ok(())
}

pub async fn list_schemes(control_port: &str) -> anyhow::Result<()> {
    let mut client = ControlClient::connect(control_address_client(control_port)).await?;
    let list = client
        .list_schemes(ListSchemesRequest { metadata: None })
        .await?;
    let msg = list.get_ref().ids.join("\n");
    println!("Drand supports the following list of schemes:\n{msg}\n\nChoose one of them and set it on --scheme flag");

    Ok(())
}

pub async fn pool_info(control_port: &str) -> anyhow::Result<()> {
    let mut client = ControlClient::connect(control_address_client(control_port)).await?;
    let res = client.pool_info(PoolInfoRequest {}).await?;

    println!("Pool status:\n{}", res.get_ref().pool_info);

    Ok(())
}

pub async fn share(
    control_port: &str,
    leader: bool,
    id: Option<String>,
    leader_tls: bool,
    leader_address: String,
) -> anyhow::Result<()> {
    if leader {
        bail!("leader logic is not implemented yet")
    }

    let addr = control_address_client(control_port);
    let secret = crate::core::dkg::load_secret_cmd()?.into_bytes();
    let metadata = Metadata {
        beacon_id: id.unwrap(),
        ..Default::default()
    };
    let req = InitDkgPacket {
        info: Some(SetupInfoPacket {
            leader,
            leader_address,
            secret,
            leader_tls,
            metadata: Some(metadata.clone()),
            ..Default::default()
        }),
        metadata: Some(metadata),
        ..Default::default()
    };
    println!("Participation in DKG");
    let mut conn = ControlClient::connect(addr).await?;

    match conn.init_dkg(req).await {
        Ok(res) => println!("--- distributed share path: {}", res.get_ref().share_path),
        Err(e) => println!("failed to init dkg: {e:?}"),
    }

    Ok(())
}

// TODO: use 'Address' and 'ControlAddress' instead
fn control_address_server(control_port: &str) -> String {
    format!("{CONTROL_HOST}:{control_port}")
}
fn control_address_client(control_port: &str) -> String {
    format!("http://{CONTROL_HOST}:{control_port}")
}

impl Deref for ControlHandler {
    type Target = Daemon;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
