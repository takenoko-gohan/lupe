use crate::pb::db::management_server::ManagementServer;
use crate::pb::db::operation_server::OperationServer;
use crate::pb::{ManagementImpl, OperationImpl};
#[cfg(windows)]
use crate::util::named_pipe::get_named_pipe_server_stream;
#[cfg(unix)]
use crate::util::uds::get_sock_path;
use duckdb::Connection;
#[cfg(unix)]
use tokio::net::UnixListener;
use tokio::signal;
use tokio::sync::{mpsc, Mutex};
#[cfg(unix)]
use tonic::codegen::tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;
use tracing::{debug, info};

async fn shutdown_signal(mut shutdown_rx: mpsc::Receiver<()>) {
    let ctrl_c_fut = async {
        signal::ctrl_c()
            .await
            .expect("failed to install SIGINT handler");
    };

    #[cfg(unix)]
    let term_fut = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    let shutdown_fut = async {
        shutdown_rx.recv().await;
    };

    #[cfg(unix)]
    tokio::select! {
        _ = ctrl_c_fut => {},
        _ = term_fut => {},
        _ = shutdown_fut => {},
    }

    #[cfg(windows)]
    tokio::select! {
        _ = ctrl_c_fut => {},
        _ = shutdown_fut => {},
    }

    debug!("shutdown signal received");

    #[cfg(unix)]
    {
        debug!("removing socket file...");
        std::fs::remove_file(get_sock_path()).expect("failed to remove socket file");
    }
}

pub(crate) async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
    let db_conn = Mutex::new(Connection::open_in_memory()?);

    let mgmt = ManagementImpl::builder().shutdown_tx(shutdown_tx).build();
    let ope = OperationImpl::builder().db_conn(db_conn).build();

    #[cfg(unix)]
    let uds_stream = {
        let sock_path = get_sock_path();
        info!("listening on {:?}", sock_path);

        let uds = UnixListener::bind(sock_path)?;
        UnixListenerStream::new(uds)
    };

    let builder = Server::builder()
        .add_service(ManagementServer::new(mgmt))
        .add_service(OperationServer::new(ope));

    #[cfg(unix)]
    builder
        .serve_with_incoming_shutdown(uds_stream, shutdown_signal(shutdown_rx))
        .await?;

    #[cfg(windows)]
    builder
        .serve_with_incoming_shutdown(get_named_pipe_server_stream(), shutdown_signal(shutdown_rx))
        .await?;

    Ok(())
}
