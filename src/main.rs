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
        table_type: TableType,
        /// e.g. s3://bucket-name/path/to/**/*.log.gz
        #[arg(long)]
        s3_uri: String,
        /// [default table name: alb: alb_logs, s3: s3_logs]
        #[arg(long)]
        table_name: Option<String>,
    },
    /// Clean up all tables
    Clean,
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
            table_type,
            table_name,
            s3_uri,
        } => cmd::load::exec(table_type.clone(), table_name.clone(), s3_uri.to_string()).await,
        Commands::Clean => cmd::clean::exec().await,
        Commands::Query { query } => cmd::query::exec(query.clone()).await,
        Commands::Server => cmd::server::exec().await,
    } {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
