#![warn(clippy::pedantic)]

use clap::Parser;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    drand::cli::Cli::parse().run().await
}
