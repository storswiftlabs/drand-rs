use crate::core::beacon;
use crate::core::daemon::Daemon;
use crate::key::keys::Pair;
use crate::key::store::FileStore;
use crate::key::Scheme;
use crate::net::control;
use crate::net::control::ControlClient;
use crate::net::dkg_control::DkgControlClient;
use crate::net::health::HealthClient;
use crate::net::protocol;
use crate::net::protocol::ProtocolClient;
use crate::net::utils::Address;
use crate::net::utils::ControlListener;
use crate::net::utils::NodeListener;

use anyhow::bail;
use anyhow::Result;
use clap::arg;
use clap::command;
use clap::Parser;
use clap::Subcommand;
use energon::drand::schemes::BN254UnchainedOnG1Scheme;
use energon::drand::schemes::DefaultScheme;
use energon::drand::schemes::SigsOnG1Scheme;
use energon::drand::schemes::UnchainedScheme;
use energon::points::KeyPoint;
use energon::traits::Affine;

/// Generate the long-term keypair (drand.private, drand.public) for this node, and load it on the drand daemon if it is up and running
#[derive(Debug, Parser, Clone)]
pub struct KeyGenConfig {
    /// Set the port you want to listen to for control port commands. If not specified, we will use the default value.
    #[arg(long, default_value = control::DEFAULT_CONTROL_PORT)]
    pub control: String,
    /// Folder to keep all drand cryptographic information, with absolute path.
    #[arg(long, default_value_t =  FileStore::drand_home())]
    pub folder: String,
    /// Indicates the id for the randomness generation process which the command applies to.
    #[arg(long, default_value = beacon::DEFAULT_BEACON_ID)]
    pub id: String,
    /// Indicates a set of values drand will use to configure the randomness generation process
    #[arg(long, default_value = DefaultScheme::ID)]
    pub scheme: String,
    /// The address other nodes will be able to contact this node on (specified as 'private-listen' to the daemon)
    pub address: String,
}

/// Start the drand daemon.
#[derive(Debug, Parser, Clone)]
pub struct Config {
    /// Folder to keep all drand cryptographic information, with absolute path.
    #[arg(long, default_value_t = FileStore::drand_home())]
    pub folder: String,
    /// Set the port you want to listen to for control port commands. If not specified, we will use the default value.
    #[arg(long, default_value = control::DEFAULT_CONTROL_PORT)]
    pub control: String,
    /// Set the listening (binding) address of the private API. Useful if you have some kind of proxy.
    #[arg(long)]
    pub private_listen: String,
    /// Launch a metrics server at the specified host:port.
    #[arg(long, default_value = None)]
    pub metrics: Option<String>,
    /// Indicates the id for the randomness generation process which will be started
    #[arg(long, default_value = None)]
    pub id: Option<String>,
}

/// Sync your local randomness chain with other nodes and validate your local beacon chain. To follow a remote node, it requires the use of the 'follow' flag.
#[derive(Debug, Parser, Clone)]
pub struct SyncConfig {
    /// Set the port you want to listen to for control port commands. If not specified, we will use the default value.
    #[arg(long, default_value = control::DEFAULT_CONTROL_PORT)]
    pub control: String,
    /// The hash of the chain info.
    #[arg(long)]
    pub chain_hash: String,
    /// <ADDRESS:PORT>,<...> of (multiple) reachable drand daemon(s). When checking our local database, using our local daemon address will result in a dry run.
    #[arg(long)]
    pub sync_nodes: Vec<String>,
    /// Specify a round at which the drand daemon will stop syncing the chain, typically used to bootstrap a new node.
    /// Note: The `up_to` value is ignored when the '--follow' flag is used.
    #[arg(long, default_value = "0")]
    pub up_to: u64,
    /// Indicates the id for the randomness generation process which will be started
    #[arg(long)]
    pub id: String,
    /// Indicates whether we want to follow another daemon up to latest chain height.
    #[arg(long)]
    pub follow: bool,
}

/// Commands for interacting with the DKG
#[derive(Subcommand, Clone, Debug)]
pub enum Dkg {
    Join {
        /// Set the port you want to listen to for control port commands. If not specified, we will use the default value.
        #[arg(long, default_value = control::DEFAULT_CONTROL_PORT)]
        control: String,
        /// Indicates the id for the randomness generation process which will be started
        #[arg(long)]
        id: String,
        /// Absolute path to the group file of previous epoch
        #[arg(long, default_value = None)]
        group: Option<String>,
    },
    Accept {
        /// Set the port you want to listen to for control port commands. If not specified, we will use the default value.
        #[arg(long, default_value = control::DEFAULT_CONTROL_PORT)]
        control: String,
        /// Indicates the id for the randomness generation process which will be started
        #[arg(long)]
        id: String,
    },
    Reject {
        /// Set the port you want to listen to for control port commands. If not specified, we will use the default value.
        #[arg(long, default_value = control::DEFAULT_CONTROL_PORT)]
        control: String,
        /// Indicates the id for the randomness generation process which will be started
        #[arg(long)]
        id: String,
    },
    Status {
        /// Set the port you want to listen to for control port commands. If not specified, we will use the default value.
        #[arg(long, default_value = control::DEFAULT_CONTROL_PORT)]
        control: String,
        /// Indicates the id for the randomness generation process which will be started
        #[arg(long, default_value = beacon::DEFAULT_BEACON_ID)]
        id: String,
    },
}

/// Local information retrieval about the node's cryptographic material and current state.
#[derive(Subcommand, Clone, Debug)]
pub enum Show {
    ChainInfo {
        /// Set the port you want to listen to for control port commands. If not specified, we will use the default value.
        #[arg(long, default_value = control::DEFAULT_CONTROL_PORT)]
        control: String,
        /// Indicates the id for the randomness generation process which will be started
        #[arg(long)]
        id: String,
    },
    Status {
        /// Set the port you want to listen to for control port commands. If not specified, we will use the default value.
        #[arg(long, default_value = control::DEFAULT_CONTROL_PORT)]
        control: String,
        /// Indicates the id for the randomness generation process which will be started
        #[arg(long)]
        id: String,
    },
}

/// Multiple commands of utility functions, such as reseting a state, checking the connection of a peer...
#[derive(Subcommand, Clone, Debug)]
pub enum Util {
    /// Check node at the given `ADDRESS` (you can put multiple ones) over the gRPC communication.
    Check {
        /// Indicates the id for the randomness generation process which will be started.
        #[arg(long, default_value = None)]
        id: Option<String>,
        addresses: Vec<String>,
    },
}

#[derive(Debug, Parser, Clone)]
#[command(
    name = "drand rust implementation (BETA)", 
    version = env!("CARGO_PKG_VERSION"), 
    about = "distributed randomness service"
)]
pub struct Cli {
    #[arg(long, global = true)]
    pub verbose: bool,
    #[command(subcommand)]
    pub commands: Cmd,
}

#[derive(Debug, Parser, Clone)]
pub enum Cmd {
    GenerateKeypair(KeyGenConfig),
    Start(Config),
    /// Stop the drand daemon.
    Stop {
        /// Set the port you want to listen to for control port commands. If not specified, we will use the default value
        #[arg(long, default_value = control::DEFAULT_CONTROL_PORT)]
        control: String,
        /// Indicates the id to be stopped, if not provided - stops all processes and shutdowns the daemon
        #[arg(long, default_value = None)]
        id: Option<String>,
    },
    /// Load a stopped beacon from the filesystem
    Load {
        /// Set the port you want to listen to for control port commands. If not specified, we will use the default value.
        #[arg(long, default_value = control::DEFAULT_CONTROL_PORT)]
        control: String,
        /// Indicates the id for the randomness generation process which will be started
        #[arg(long)]
        id: String,
    },
    Sync(SyncConfig),
    #[command(subcommand)]
    Dkg(Dkg),
    #[command(subcommand)]
    Show(Show),
    #[command(subcommand)]
    Util(Util),
}

impl Cli {
    pub async fn run(self) -> Result<()> {
        crate::log::setup_tracing(self.verbose)?;

        match self.commands {
            Cmd::GenerateKeypair(config) => keygen_cmd(config).await?,
            Cmd::Start(config) => start_cmd(config).await?,
            Cmd::Load { control, id } => load_beacon_cmd(&control, id).await?,
            Cmd::Stop { control, id } => stop_cmd(&control, id).await?,
            Cmd::Sync(config) => sync_cmd(config).await?,
            Cmd::Dkg(dkg) => match dkg {
                Dkg::Join { control, id, group } => {
                    dkg_join_cmd(&control, id, group.as_deref()).await?;
                }
                Dkg::Accept { control, id } => dkg_accept_cmd(&control, id).await?,
                Dkg::Reject { control, id } => dkg_reject_cmd(&control, id).await?,
                Dkg::Status { control, id } => dkg_status_cmd(&control, id).await?,
            },
            Cmd::Show(show) => match show {
                Show::ChainInfo { control, id } => chain_info_cmd(&control, id).await?,
                Show::Status { control, id } => status_cmd(&control, id).await?,
            },
            Cmd::Util(util) => match util {
                Util::Check { id, addresses } => util_check_cmd(id.as_deref(), addresses).await?,
            },
        }

        Ok(())
    }
}

async fn keygen_cmd(config: KeyGenConfig) -> Result<()> {
    println!("Generating private / public key pair");
    match config.scheme.as_str() {
        DefaultScheme::ID => keygen::<DefaultScheme>(&config)?,
        UnchainedScheme::ID => keygen::<UnchainedScheme>(&config)?,
        SigsOnG1Scheme::ID => keygen::<SigsOnG1Scheme>(&config)?,
        BN254UnchainedOnG1Scheme::ID => keygen::<BN254UnchainedOnG1Scheme>(&config)?,
        _ => bail!("keygen: unknown scheme: {}", config.scheme),
    }

    // If keys were generated successfully, daemon needs to load them.
    match control::ControlClient::new(&config.control).await {
        Ok(mut client) => {
            client.load_beacon(config.id).await?;
        }
        Err(_) => eprintln!("Keys couldn't be loaded on drand daemon. If it is not running, these new keys will be loaded on startup"),
    }

    Ok(())
}

/// Generic helper for [`keygen_cmd`]
fn keygen<S: Scheme>(config: &KeyGenConfig) -> Result<()> {
    let address = Address::precheck(&config.address)?;
    let pair = Pair::<S>::generate(address)?;
    let store = FileStore::new_checked(&config.folder, &config.id)?;
    store.save_key_pair(&pair)?;

    Ok(())
}

async fn start_cmd(config: Config) -> Result<()> {
    let private_listen = Address::precheck(&config.private_listen)?;
    let control_port = config.control.clone();

    // Start metrics server
    if let Some(ref address) = config.metrics {
        crate::net::metrics::setup_metrics(address)?;
    }

    let daemon = Daemon::new(config)?;
    // Start control server
    let control = daemon.tracker.spawn({
        let daemon = daemon.clone();
        control::start_server::<ControlListener>(daemon, control_port)
    });
    // Start node server
    let node = daemon.tracker.spawn({
        let daemon = daemon.clone();
        protocol::start_server::<NodeListener>(daemon, private_listen)
    });

    assert!(
        tokio::try_join!(control, node).is_ok(),
        "can not start node"
    );

    Ok(())
}

async fn load_beacon_cmd(control_port: &str, beacon_id: String) -> Result<()> {
    let mut client = ControlClient::new(control_port).await?;
    client.load_beacon(beacon_id).await?;

    Ok(())
}

async fn stop_cmd(control_port: &str, beacon_id: Option<String>) -> anyhow::Result<()> {
    let mut conn = ControlClient::new(control_port).await?;

    match conn.shutdown(beacon_id.clone()).await {
        Ok(is_daemon_running) => {
            if is_daemon_running {
                println!("beacon process [{beacon_id:?}] stopped correctly. Bye.");
            } else {
                println!("drand daemon stopped correctly. Bye.");
            }
        }
        Err(err) => {
            if let Some(id) = beacon_id {
                println!("error stopping beacon process: [{id}], status: {err}");
            } else {
                println!("error stopping drand daemon, status: {err}");
            }
        }
    }

    Ok(())
}

async fn sync_cmd(config: SyncConfig) -> Result<()> {
    let mut client = ControlClient::new(&config.control).await?;
    client.sync(config).await?;

    Ok(())
}

async fn dkg_join_cmd(
    control_port: &str,
    beacon_id: String,
    groupfile_path: Option<&str>,
) -> Result<()> {
    let mut client = DkgControlClient::new(control_port).await?;
    client.dkg_join(beacon_id, groupfile_path).await?;
    println!("Joined the DKG successfully!");

    Ok(())
}

async fn dkg_accept_cmd(control_port: &str, beacon_id: String) -> Result<()> {
    let mut client = DkgControlClient::new(control_port).await?;
    client.dkg_accept(beacon_id).await?;

    Ok(())
}

async fn dkg_reject_cmd(control_port: &str, beacon_id: String) -> Result<()> {
    let mut client = DkgControlClient::new(control_port).await?;
    client.dkg_reject(beacon_id).await?;

    Ok(())
}

async fn dkg_status_cmd(control_port: &str, beacon_id: String) -> Result<()> {
    let mut client = DkgControlClient::new(control_port).await?;
    let response: crate::transport::dkg::DkgStatusResponse =
        client.dkg_status(beacon_id).await?.try_into()?;

    crate::dkg::utils::print_dkg_status(response);

    Ok(())
}

async fn chain_info_cmd(control_port: &str, beacon_id: String) -> Result<()> {
    let mut client = ControlClient::new(control_port).await?;
    let info = client.chain_info(beacon_id).await?;
    println!("{info}");

    Ok(())
}

async fn status_cmd(control_port: &str, beacon_id: String) -> Result<()> {
    let mut client = ControlClient::new(control_port).await?;
    let status = client.status(beacon_id.clone()).await?;
    println!(
        "Beacon ID: {beacon_id}\nLatest stored round: {}",
        status.latest_stored_round
    );

    Ok(())
}

async fn util_check_cmd(beacon_id: Option<&str>, addresses: Vec<String>) -> Result<()> {
    let peers = addresses
        .iter()
        .map(|addr| Address::precheck(addr.as_str()))
        .collect::<Result<Vec<_>, _>>()?;

    let mut invalid_ids: Vec<&Address> = Vec::with_capacity(peers.len());
    for peer in &peers {
        if let Err(err) = {
            match beacon_id {
                Some(id) => check_identity_address(peer, id.to_string()).await,
                None => HealthClient::check(peer).await,
            }
        } {
            if tracing::enabled!(tracing::Level::DEBUG) {
                println!("drand: error checking id {peer}: {}", err.root_cause());
            } else {
                println!("drand: error checking id {peer}");
            }

            invalid_ids.push(peer);
            continue;
        }
        println!("drand: id {peer} answers correctly");
    }
    if !invalid_ids.is_empty() {
        println!("following nodes don't answer: {invalid_ids:?}");
    }

    Ok(())
}

async fn check_identity_address(peer: &Address, beacon_id: String) -> Result<()> {
    let mut client = ProtocolClient::new(peer).await?;
    let resp = client.get_identity(beacon_id).await?;

    if resp.address != *peer {
        bail!(
            "mismatch of address: contact {peer} reply with {}",
            resp.address
        )
    }
    if match resp.scheme_name.as_str() {
        DefaultScheme::ID => KeyPoint::<DefaultScheme>::deserialize(&resp.key).is_err(),
        SigsOnG1Scheme::ID => KeyPoint::<SigsOnG1Scheme>::deserialize(&resp.key).is_err(),
        UnchainedScheme::ID => KeyPoint::<UnchainedScheme>::deserialize(&resp.key).is_err(),
        BN254UnchainedOnG1Scheme::ID => {
            KeyPoint::<BN254UnchainedOnG1Scheme>::deserialize(&resp.key).is_err()
        }
        _ => bail!(
            "received an invalid / unsupported SchemeName in identity response: {}",
            resp.scheme_name
        ),
    } {
        bail!("could not unmarshal public key");
    };

    Ok(())
}
