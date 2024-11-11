use crate::pb::duckdb::management_client::ManagementClient;
use crate::util::uds::get_sock_path;
use clap::ValueEnum;
use hyper_util::rt::TokioIo;
use tokio::net::UnixStream;
use tonic::transport::{Endpoint, Uri};
use tower::service_fn;

#[derive(ValueEnum, Debug, Clone)]
pub(crate) enum Operation {
    HealthCheck,
    Shutdown,
}

pub(crate) async fn run(operation: &Operation) -> Result<(), Box<dyn std::error::Error>> {
    // We will ignore this uri because uds do not use it
    // if your connector does use the uri it will be provided
    // as the request to the `MakeConnection`.
    let channel = Endpoint::try_from("http://[::]:0")?
        .connect_with_connector(service_fn(|_: Uri| async {
            let sock_path = get_sock_path();
            println!("Connecting to {:?}", sock_path);

            // Connect to a Uds socket
            Ok::<_, std::io::Error>(TokioIo::new(UnixStream::connect(sock_path).await?))
        }))
        .await?;

    let mut client = ManagementClient::new(channel);

    match operation {
        Operation::HealthCheck => {
            let request = tonic::Request::new(crate::pb::duckdb::HealthCheckRequest::default());
            let response = client.health_check(request).await?;
            println!("RESPONSE={:?}", response);
            Ok(())
        }
        Operation::Shutdown => {
            let request = tonic::Request::new(crate::pb::duckdb::ShutdownRequest::default());
            let response = client.shutdown(request).await?;
            println!("RESPONSE={:?}", response);
            Ok(())
        }
    }
}
