use crate::pb::db::management_client::ManagementClient;
use crate::pb::db::ShutdownRequest;
use crate::util::uds::{create_channel, get_sock_path};
use tonic::Request;
use tracing::info;

pub(crate) async fn run() -> Result<(), Box<dyn std::error::Error>> {
    if get_sock_path().exists() {
        let channel = create_channel().await?;
        let mut mgmt_client = ManagementClient::new(channel.clone());

        let req = Request::new(ShutdownRequest::default());

        info!("shutting down server...");
        mgmt_client
            .shutdown(req)
            .await
            .map_err(|e| e.message().to_string())?;
        info!("shutdown successfully");
    } else {
        info!("server is not running");
    }

    Ok(())
}
