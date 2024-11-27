mod cmd;
mod pb;
mod repo;
mod util;

use crate::cmd::load::TableType;
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
    /// Load logs into DuckDB
    Load {
        #[arg(long, value_enum)]
        data_type: TableType,
        #[arg(long)]
        s3_prefix: String,
        #[arg(long)]
        table_name: Option<String>,
    },
    /// Clear all logs from DuckDB
    Clear,
    /// Execute Raw Query
    Query { query: String },
    /// Start Server
    #[command(hide = true)]
    Server,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // You can check for the existence of subcommands, and if found use their
    // matches just as you would the top level cmd
    if let Err(e) = match &cli.command {
        Commands::Load {
            data_type,
            table_name,
            s3_prefix,
        } => cmd::load::exec(data_type.clone(), table_name.clone(), s3_prefix.to_string()).await,
        Commands::Clear => cmd::clear::exec().await,
        Commands::Query { query } => cmd::query::exec(query.clone()).await,
        Commands::Server => cmd::server::exec().await,
    } {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
