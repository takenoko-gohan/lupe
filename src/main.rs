mod cmd;
mod proto;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start Server
    Server,
    Client,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // You can check for the existence of subcommands, and if found use their
    // matches just as you would the top level cmd
    if let Err(e) = match &cli.command {
        Commands::Server => cmd::server::run().await,
        Commands::Client => cmd::client::run().await,
    } {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
