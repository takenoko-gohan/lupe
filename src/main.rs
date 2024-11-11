mod cmd;
mod pb;
mod util;

use crate::cmd::client::Operation;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug, Clone)]
enum Commands {
    /// Start Server
    Server,
    Client {
        #[arg(value_enum)]
        operation: Operation,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // You can check for the existence of subcommands, and if found use their
    // matches just as you would the top level cmd
    if let Err(e) = match &cli.command {
        Commands::Server => cmd::server::run().await,
        Commands::Client { operation } => cmd::client::run(operation).await,
    } {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
