use crate::proto::hello_world::greeter_server::GreeterServer;
use crate::proto::GreeterImpl;
use std::path::Path;
use tokio::net::UnixListener;
use tonic::codegen::tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

pub(crate) async fn run() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting server...");

    let sock_path = Path::new("/tmp/alb-logview.sock");

    let greeter = GreeterImpl::default();

    let uds = UnixListener::bind(sock_path)?;
    let uds_stream = UnixListenerStream::new(uds);

    Server::builder()
        .add_service(GreeterServer::new(greeter))
        .serve_with_incoming(uds_stream)
        .await?;

    Ok(())
}
