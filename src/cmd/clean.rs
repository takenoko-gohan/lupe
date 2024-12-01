use crate::pb::db::management_client::ManagementClient;
use crate::pb::db::ShutdownRequest;
#[cfg(windows)]
use crate::util::named_pipe;
#[cfg(unix)]
use crate::util::uds;
use tonic::Request;

pub(crate) async fn exec() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(unix)]
    if uds::get_sock_path().exists() {
        let channel = uds::create_channel().await?;
        let mut mgmt_client = ManagementClient::new(channel.clone());

        let req = Request::new(ShutdownRequest::default());

        println!("Shutting down server...");
        mgmt_client.shutdown(req).await?;
    } else {
        println!("Already shutdown");
    }

    #[cfg(windows)]
    {
        let channel = named_pipe::create_channel().await?;
        let mut mgmt_client = ManagementClient::new(channel.clone());

        let req = Request::new(ShutdownRequest::default());

        println!("Shutting down server...");
        mgmt_client.shutdown(req).await?;
    }

    Ok(())
}
