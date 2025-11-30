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

    // Create the credential cache once (shared between Lambda and DB)
    let credential_cache = credentials::CredentialCache::new().await?;

    // Create the Lambda client using the shared credentials
    let client = lambda::client(&credential_cache).await?;

    match args.command {
        cli::Command::TestChapter { chapter } => {
            tests::run_test(&client, &credential_cache, chapter).await?;
        }
        cli::Command::Setup { accounts } => {
            setup::setup_schema(&credential_cache, accounts).await?;
        }
        cli::Command::SetupCh04 => {
            setup::setup_chapter4(&credential_cache).await?;
        }
        cli::Command::SustainedLoad { invocations_per_sec, accounts } => {
            stress::run_sustained_load(&client, invocations_per_sec, accounts).await?;
        }
    }

    Ok(())
}
