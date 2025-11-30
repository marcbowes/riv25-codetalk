mod cli;
mod credentials;
mod db;
mod lambda;
mod setup;
mod stress;
mod tests;

use anyhow::Result;
use clap::Parser;

#[tokio::main(flavor = "multi_thread", worker_threads = 64)]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = cli::Args::parse();

    match args.command {
        cli::Command::TestChapter { chapter } => {
            tests::run_test(chapter).await?;
        }
        cli::Command::Setup { accounts } => {
            setup::setup_schema(accounts).await?;
        }
        cli::Command::SetupCh04 => {
            setup::setup_chapter4().await?;
        }
        cli::Command::SustainedLoad { parallel, accounts } => {
            stress::run_sustained_load(parallel, accounts).await?;
        }
    }

    Ok(())
}
