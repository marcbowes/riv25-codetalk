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

    match args.command {
        cli::Command::TestChapter { chapter } => {
            let client_pool = lambda::client_pool(&credential_cache, 1).await?;
            tests::run_test(&client_pool, &credential_cache, chapter).await?;
        }
        cli::Command::Setup { accounts } => {
            setup::setup_schema(&credential_cache, accounts).await?;
        }
        cli::Command::SetupCh04 => {
            setup::setup_chapter4(&credential_cache).await?;
        }
        cli::Command::SustainedLoad { invocations_per_sec, accounts } => {
            // Use 16 clients to distribute load across multiple HTTP connections
            let client_pool = lambda::client_pool(&credential_cache, 16).await?;
            stress::run_sustained_load(&client_pool, invocations_per_sec, accounts).await?;
        }
    }

    Ok(())
}
