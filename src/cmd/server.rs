use crate::pb::db::management_server::ManagementServer;
use crate::pb::db::operation_server::OperationServer;
use crate::pb::{ManagementImpl, OperationImpl};
use crate::util::uds::get_sock_path;
use duckdb::Connection;
use tokio::net::UnixListener;
use tokio::signal;
use tokio::sync::{mpsc, Mutex};
use tonic::codegen::tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;
use tracing::{debug, info};

async fn shutdown_signal(mut shutdown_rx: mpsc::Receiver<()>) {
    let ctrl_c_fut = async {
        signal::ctrl_c()
            .await
            .expect("failed to install SIGINT handler");
    };

    let term_fut = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    let shutdown_fut = async {
        shutdown_rx.recv().await;
    };

    tokio::select! {
        _ = ctrl_c_fut => {},
        _ = term_fut => {},
        _ = shutdown_fut => {},
    }
    debug!("shutdown signal received");

    debug!("removing socket file...");
    std::fs::remove_file(get_sock_path()).expect("failed to remove socket file");
}

pub(crate) async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let sock_path = get_sock_path();
    info!("listening on {:?}", sock_path);

    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
    let db_conn = Mutex::new(Connection::open_in_memory()?);

    let mgmt = ManagementImpl::builder().shutdown_tx(shutdown_tx).build();
    let ope = OperationImpl::builder().db_conn(db_conn).build();

    let uds = UnixListener::bind(sock_path)?;
    let uds_stream = UnixListenerStream::new(uds);
    Server::builder()
        .add_service(ManagementServer::new(mgmt))
        .add_service(OperationServer::new(ope))
        .serve_with_incoming_shutdown(uds_stream, shutdown_signal(shutdown_rx))
        .await?;

    Ok(())
}
