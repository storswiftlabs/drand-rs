use crate::core::beacon;
use crate::key::keys::Pair;
use crate::key::store::FileStore;
use crate::key::Scheme;
use crate::net::control;
use crate::net::utils::Address;

use anyhow::bail;
use anyhow::Result;
use clap::arg;
use clap::command;
use clap::Parser;
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

#[derive(Debug, Parser)]
#[command(name = "git")]
#[command(about = "", long_about = None)]
pub struct CLI {
    #[arg(long, global = true)]
    verbose: bool,
    #[command(subcommand)]
    commands: Cmd,
}

#[derive(Debug, Parser)]
pub enum Cmd {
    GenerateKeypair(KeyGenConfig),
}

impl CLI {
    pub async fn run(self) -> Result<()> {
        // Logs are disabled in tests by default.
        #[cfg(not(test))]
        crate::log::init_log(self.verbose)?;

        match self.commands {
            Cmd::GenerateKeypair(config) => keygen_cmd(&config).await?,
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
    //       Testing [`ControlClient::load_beacon`] should be done outside this function.
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
