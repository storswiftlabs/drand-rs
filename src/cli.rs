use clap::arg;
use clap::command;
use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "git")]
#[command(about = "", long_about = None)]
pub struct Cli {
    #[arg(long, global = true)]
    verbose: bool,
    #[command(subcommand)]
    commands: Cmd,
}

#[derive(Debug, Parser)]
pub enum Cmd {}

impl Cli {
    pub async fn run(self) -> anyhow::Result<()> {
        Ok(())
    }
}
