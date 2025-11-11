mod cli;
mod db;
mod lambda;
mod stress;
mod tests;

use anyhow::Result;
use clap::Parser;

#[tokio::main]
async fn main() -> Result<()> {
    let args = cli::Args::parse();

    match args.command {
        cli::Command::TestChapter { chapter } => {
            tests::run_test(chapter).await?;
        }
        cli::Command::Setup { accounts } => {
            tests::setup_schema(accounts).await?;
        }
        cli::Command::SetupCh04 => {
            tests::setup_chapter4().await?;
        }
    }

    Ok(())
}
