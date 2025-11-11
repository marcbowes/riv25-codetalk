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
}
