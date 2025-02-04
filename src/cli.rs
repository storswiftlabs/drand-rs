use clap::arg;
use clap::command;
use clap::Parser;

/// Top-level error for CLI commands
#[derive(thiserror::Error, Debug)]
pub enum CliError {}

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
pub enum Cmd {}

impl CLI {
    pub async fn run(self) -> Result<(), CliError> {
        Ok(())
    }
}
