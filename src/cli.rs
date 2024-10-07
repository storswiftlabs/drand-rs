use crate::core::beacon;
use crate::key::store::FileStore;
use crate::log::init_log;
use crate::net::control;

use clap::arg;
use clap::command;
use clap::Parser;
use clap::Subcommand;
use energon::drand::scheme;

#[derive(Debug, Parser)]
#[command(name = "git")]
#[command(about = "", long_about = None)]
pub struct Cli {
    #[arg(long, global = true)]
    verbose: bool,
    #[command(subcommand)]
    commands: Commands,
}

// NOTE: Goal is to keep CLI in sync with golang implementation (currently aligned to v1.5.8)

#[derive(Debug, Parser)]
pub enum Commands {
    /// Generate the long-term keypair (drand.private, drand.public) for this node, and load it on the drand daemon if it is up and running
    GenerateKeypair {
        /// Folder to keep all drand cryptographic information, with absolute path.
        #[arg(long, default_value_t = FileStore::drand_home())]
        folder: String,
        /// Disable TLS for all communications (not recommended). (default: false)
        #[arg(long)]
        tls_disable: bool,
        /// Indicates the id for the randomness generation process which will be started
        #[arg(long, default_value = beacon::DEFAULT_BEACON_ID)]
        id: String,
        /// Indicates a set of values drand will use to configure the randomness generation process
        #[arg(long, default_value = scheme::DEFAULT_SCHEME)]
        scheme: String,
        /// The address other nodes will be able to contact this node on (specified as 'private-listen' to the daemon)
        address: String,
    },
    #[command(subcommand)]
    Util(Util),
    #[command(subcommand)]
    Show(Show),
    /// Start the drand daemon.
    Start {
        /// Folder to keep all drand cryptographic information, with absolute path.
        #[arg(long, default_value_t = FileStore::drand_home())]
        folder: String,
        /// Indicates the id for the randomness generation process which will be started
        #[arg(long, default_value = None)]
        id: Option<String>,
        /// The address other nodes will be able to contact this node on (specified as 'private-listen' to the daemon)
        #[arg(long)]
        private_listen: String,
        /// Set the port you want to listen to for control port commands. If not specified, we will use the default value
        #[arg(long, default_value = control::DEFAULT_CONTROL_PORT)]
        control: String,
    },
    /// Stop the drand daemon.
    Stop {
        /// Set the port you want to listen to for control port commands. If not specified, we will use the default value
        #[arg(long, default_value = control::DEFAULT_CONTROL_PORT)]
        control: String,
        /// Indicates the id to be stopped, if not provided - stops all processes and shutdowns the daemon
        #[arg(long, default_value = None)]
        id: Option<String>,
    },
    /// Launch a sharing protocol.
    Share {
        /// Set the port you want to listen to for control port commands. If not specified, we will use the default value
        #[arg(long, default_value = control::DEFAULT_CONTROL_PORT)]
        control: String,
        /// Specify if this node should act as the leader for setting up the group. (default: false)
        #[arg(long)]
        leader: bool,
        /// Indicates the id for the randomness generation process which will be started
        #[arg(long, default_value = beacon::DEFAULT_BEACON_ID)]
        id: Option<String>,
        /// Disable TLS for all communications (not recommended). (default: false)
        #[arg(long)]
        tls_disable: bool,
        /// Address of the coordinator that will assemble the public keys and start the DKG
        #[arg(long)]
        connect: String,
    },
}

#[derive(Subcommand, Debug)]
/// Drand show - local information retrieval about the node's cryptographic material. Show prints the information about the collective public key (drand.cokey), the group details (group.toml), the long-term private key (drand.private), the long-term public key (drand.public), or the private key share (drand.share), respectively.
pub enum Show {
    /// shows the long-term public key of a node.
    Public {
        /// Set the port you want to listen to for control port commands. If not specified, we will use the default value
        #[arg(long, default_value = control::DEFAULT_CONTROL_PORT)]
        control: String,
        /// Indicates the id for the randomness generation process which will be started
        #[arg(long, default_value = beacon::DEFAULT_BEACON_ID)]
        id: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
pub enum Util {
    /// Check node at the given `ADDRESS` (you can put multiple ones) in the group for accessibility over the gRPC communication. If the node is not running behind TLS, you need to pass the tls-disable flag.
    Check {
        /// Disable TLS for all communications (not recommended). (default: false)
        #[arg(long)]
        tls_disable: bool,
        /// Indicates the id for the randomness generation process which will be started
        #[arg(long, default_value = None)]
        id: Option<String>,
        addresses: Vec<String>,
    },
    /// List all scheme ids available to use
    ListSchemes {
        /// Set the port you want to listen to for control port commands. If not specified, we will use the default value.
        #[arg(long, default_value = control::DEFAULT_CONTROL_PORT)]
        control: String,
    },

    PoolInfo {
        /// Set the port you want to listen to for control port commands. If not specified, we will use the default value.
        #[arg(long, default_value = control::DEFAULT_CONTROL_PORT)]
        control: String,
    },
}

impl Cli {
    pub async fn run(self) -> anyhow::Result<()> {
        init_log(self.verbose)?;
        match self.commands {
            Commands::GenerateKeypair {
                folder,
                id,
                scheme,
                address,
                tls_disable,
            } => {
                //  - Default boolean value is true, omitting '--tls_disable' leads to tls_disable:true
                //  - We want to keep CLI in sync with golang implementation
                //  - From here tls:true means tls enabled, this can probably be done more elegantly
                let tls = !tls_disable;
                crate::core::schemes_init::gen_keypair(scheme, &folder, tls, id, address)?
            }
            Commands::Start {
                folder,
                id,
                private_listen,
                control,
            } => control::start(&folder, id, &control, private_listen).await?,

            Commands::Stop { control, id } => control::stop(&control, id.as_ref()).await?,

            Commands::Show(show) => match show {
                Show::Public { control, id } => control::public_key_request(&control, id).await?,
            },
            Commands::Util(util) => match util {
                Util::Check {
                    tls_disable,
                    id,
                    addresses,
                } => todo!(),
                Util::ListSchemes { control } => control::list_schemes(&control).await?,
                Util::PoolInfo { control } => control::pool_info(&control).await?,
            },
            Commands::Share {
                control,
                leader,
                id,
                tls_disable,
                connect,
            } => {
                //  - Default boolean value is true, omitting '--tls_disable' leads to tls_disable:true
                //  - We want to keep CLI in sync with golang implementation
                //  - From here tls:true means tls enabled, this can probably be done more elegantly
                let tls = !tls_disable;
                control::share(&control, leader, id, tls, connect).await?
            }
        }
        Ok(())
    }
}
