use hyper_util::rt::TokioIo;
use std::path::PathBuf;
use tokio::net::UnixStream;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;
use tracing::{debug, warn};

pub(crate) fn get_sock_path() -> PathBuf {
    std::env::temp_dir().join("lupe.sock")
}

pub(crate) async fn create_channel() -> Result<Channel, Box<dyn std::error::Error>> {
    for i in 0..3 {
        match connect().await {
            Ok(channel) => {
                return Ok(channel);
            }
            Err(e) => {
                warn!("failed to connect to unix domain socket: {:?}", e);
                let wait_times = if i == 0 { 1 } else { i * 2 };
                warn!("retry in {} seconds", wait_times);
                tokio::time::sleep(std::time::Duration::from_secs(wait_times)).await;
            }
        }
    }

    Err("failed to connect to unix domain socket".into())
}

async fn connect() -> Result<Channel, Box<dyn std::error::Error>> {
    let channel = Endpoint::try_from("http://[::]:0")?
        .connect_with_connector(service_fn(|_: Uri| async {
            let sock_path = get_sock_path();

            debug!("connecting to {:?}", sock_path);
            Ok::<_, std::io::Error>(TokioIo::new(UnixStream::connect(sock_path).await?))
        }))
        .await?;

    Ok(channel)
}
