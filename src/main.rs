mod cmd;
mod pb;
mod repo;
mod util;

use crate::cmd::load::TableType;
use clap::{Parser, Subcommand};
use tracing::error;

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    #[arg(long)]
    debug: bool,
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

    if cli.debug {
        tracing_subscriber::fmt()
            .with_env_filter("info,lupe=debug")
            .with_target(false)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter("info")
            .with_target(false)
            .init();
    }

    // You can check for the existence of subcommands, and if found use their
    // matches just as you would the top level cmd
    if let Err(e) = match &cli.command {
        Commands::Load {
            table_type,
            table_name,
            s3_uri,
        } => cmd::load::run(table_type.clone(), table_name.clone(), s3_uri.to_string()).await,
        Commands::Clean => cmd::clean::run().await,
        Commands::Query { query } => cmd::query::run(query.clone()).await,
        Commands::Server => cmd::server::run().await,
    } {
        error!("{}", e);
        std::process::exit(1);
    }
}
