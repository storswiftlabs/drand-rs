use crate::core::beacon;
use crate::core::daemon::Daemon;

use crate::key::keys::Pair;
use crate::key::store::FileStore;
use crate::key::Scheme;

use crate::net::control;
use crate::net::control::ControlClient;
use crate::net::dkg_control::DkgControlClient;
use crate::net::protocol;
use crate::net::utils::Address;
use crate::net::utils::ControlListener;
use crate::net::utils::NodeListener;

use clap::arg;
use clap::command;
use clap::Parser;
use clap::Subcommand;

use anyhow::bail;
use anyhow::Result;
use energon::drand::schemes::*;

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
    /// Indicates the id for the randomness generation process which will be started
    #[arg(long, default_value = None)]
    pub id: Option<String>,
}

/// Sync your local randomness chain with other nodes and validate your local beacon chain. To follow a remote node, it requires the use of the 'follow' flag.
#[derive(Debug, Parser, Clone)]
pub struct SyncConfig {
    /// Folder to keep all drand cryptographic information, with absolute path.
    #[arg(long, default_value_t = FileStore::drand_home())]
    pub folder: String,
    /// Set the port you want to listen to for control port commands. If not specified, we will use the default value.
    #[arg(long, default_value = control::DEFAULT_CONTROL_PORT)]
    pub control: String,
    /// The hash of the chain info.
    #[arg(long)]
    pub chain_hash: String,
    /// <ADDRESS:PORT>,<...> of (multiple) reachable drand daemon(s). When checking our local database, using our local daemon address will result in a dry run.
    #[arg(long)]
    pub sync_nodes: Vec<String>,
    /// Specify a round at which the drand daemon will stop syncing the chain, typically used to bootstrap a new node in chained mode
    #[arg(long, default_value = "0")]
    pub up_to: u64,
    /// Indicates the id for the randomness generation process which will be started
    #[arg(long)]
    pub id: String,
    /// Indicates whether we want to follow another daemon, if not we perform a check of our local DB. Requires to specify the chain-hash using the 'chain-hash' flag.
    #[arg(long)]
    pub follow: bool,
}

#[derive(Subcommand, Clone, Debug)]
/// Commands for interacting with the DKG
pub enum Dkg {
    Join {
        /// Set the port you want to listen to for control port commands. If not specified, we will use the default value.
        #[arg(long, default_value = control::DEFAULT_CONTROL_PORT)]
        control: String,
        /// Indicates the id for the randomness generation process which will be started
        #[arg(long)]
        id: String,
    },
}

#[derive(Debug, Parser, Clone)]
#[command(name = "git")]
#[command(about = "", long_about = None)]
pub struct CLI {
    #[arg(long, global = true)]
    pub verbose: bool,
    #[command(subcommand)]
    pub commands: Cmd,
}

#[derive(Debug, Parser, Clone)] //TODO: mv Clone to tests
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
}

impl CLI {
    pub async fn run(self) -> Result<()> {
        // Logs are disabled in tests by default.
        #[cfg(not(test))]
        crate::log::init_log(self.verbose)?;

        match self.commands {
            Cmd::GenerateKeypair(config) => keygen_cmd(&config).await?,
            Cmd::Start(config) => start_cmd(config).await?,
            Cmd::Load { control, id } => load_cmd(&control, &id).await?,
            Cmd::Stop { control, id } => stop_cmd(&control, id.as_deref()).await?,
            Cmd::Sync(config) => sync_cmd(config).await?,
            Cmd::Dkg(dkg) => match dkg {
                Dkg::Join { control, id } => join_cmd(&control, &id).await?,
            },
        }

        Ok(())
    }
}

async fn keygen_cmd(config: &KeyGenConfig) -> Result<()> {
    println!("Generating private / public key pair");
    match config.scheme.as_str() {
        DefaultScheme::ID => keygen::<DefaultScheme>(config).await?,
        UnchainedScheme::ID => keygen::<UnchainedScheme>(config).await?,
        SigsOnG1Scheme::ID => keygen::<SigsOnG1Scheme>(config).await?,
        BN254UnchainedOnG1Scheme::ID => keygen::<BN254UnchainedOnG1Scheme>(config).await?,
        _ => bail!("keygen: unknown scheme: {}", config.scheme),
    }

    // Note: Loading keys into the daemon at `keygen_cmd` is disabled in tests.
    //       Testing [`ControlClient::load_beacon`] is done outside this function.
    #[cfg(not(test))]
    // If keys were generated successfully, daemon needs to load them.
    match control::ControlClient::new(&config.control).await {
        Ok(mut client) => {
            client.load_beacon(&config.id).await?;
        }
        Err(_) => {
            // Just print, exit code 0
            eprintln!("Keys couldn't be loaded on drand daemon. If it is not running, these new keys will be loaded on startup")
        }
    }

    Ok(())
}

/// Generic helper for [`keygen_cmd`]
async fn keygen<S: Scheme>(config: &KeyGenConfig) -> Result<()> {
    let address = Address::precheck(&config.address)?;
    let pair = Pair::<S>::generate(address)?;
    let store = FileStore::new_checked(&config.folder, &config.id)?;
    store.save_key_pair(&pair)?;

    Ok(())
}

async fn start_cmd(config: Config) -> Result<()> {
    let node_address = Address::precheck(&config.private_listen)?;
    let daemon = Daemon::new(&config).await?;
    // Start control server
    let control = daemon.tracker.spawn({
        let daemon = daemon.clone();
        control::start_server::<ControlListener>(daemon, config.control)
    });
    // Start node server
    let node = daemon.tracker.spawn({
        let daemon = daemon.clone();
        protocol::start_server::<NodeListener>(daemon, node_address)
    });

    if tokio::try_join!(control, node).is_err() {
        panic!("can not start node");
    };

    Ok(())
}

/// Load beacon id into drand node
async fn load_cmd(control_port: &str, beacon_id: &str) -> Result<()> {
    let mut client = ControlClient::new(control_port).await?;
    client.load_beacon(beacon_id).await?;

    Ok(())
}

async fn stop_cmd(control_port: &str, beacon_id: Option<&str>) -> anyhow::Result<()> {
    let mut conn = ControlClient::new(control_port).await?;

    match conn.shutdown(beacon_id).await {
        Ok(is_daemon_running) => {
            if is_daemon_running {
                println!("beacon process [{:?}] stopped correctly. Bye.\n", beacon_id)
            } else {
                println!("drand daemon stopped correctly. Bye.\n")
            }
        }
        Err(err) => {
            if let Some(id) = beacon_id {
                println!("error stopping beacon process: [{id}], status: {err}")
            } else {
                println!("error stopping drand daemon, status: {err}")
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

async fn join_cmd(control_port: &str, beacon_id: &str) -> Result<()> {
    let mut client = DkgControlClient::new(control_port).await?;
    client.dkg_join(beacon_id).await?;

    Ok(())
}
