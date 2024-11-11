use crate::pb::duckdb::management_server::ManagementServer;
use crate::pb::ManagementImpl;
use crate::util::uds::get_sock_path;
use tokio::net::UnixListener;
use tokio::signal;
use tokio::sync::mpsc;
use tonic::codegen::tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

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
    println!("shutdown signal received");

    println!("remove socket file");
    std::fs::remove_file(get_sock_path()).expect("failed to remove socket file");
}

pub(crate) async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting server...");

    let sock_path = get_sock_path();
    println!("Listening on {:?}", sock_path);

    let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

    let mgmt = ManagementImpl::builder().shutdown_tx(shutdown_tx).build();

    let uds = UnixListener::bind(sock_path)?;
    let uds_stream = UnixListenerStream::new(uds);
    Server::builder()
        .add_service(ManagementServer::new(mgmt))
        .serve_with_incoming_shutdown(uds_stream, shutdown_signal(shutdown_rx))
        .await?;

    Ok(())
}
