use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "helper")]
#[command(about = "Test helper for Aurora DSQL demo")]
pub struct Args {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Test a specific chapter
    TestChapter {
        #[arg(short, long)]
        chapter: u32,
    },
    /// Setup database schema
    Setup {
        #[arg(long, default_value = "1000")]
        accounts: u32,
    },
    /// Setup Chapter 4 (1M accounts)
    SetupCh04,
    /// Run sustained load until Ctrl-C
    SustainedLoad {
        /// Number of parallel requests to maintain
        #[arg(short, long, default_value = "100")]
        parallel: usize,
        /// Number of accounts to use for random transfers
        #[arg(short, long, default_value = "1000")]
        accounts: u32,
    },
}
