use async_stream::stream;
use futures_core::stream::Stream;
use hyper_util::rt::TokioIo;
use std::pin::Pin;
use tokio::net::windows::named_pipe::ClientOptions;
use tokio::{
    io::{self, AsyncRead, AsyncWrite},
    net::windows::named_pipe::{NamedPipeServer, ServerOptions},
};
use tonic::transport::server::Connected;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;
use windows::Win32::Foundation::ERROR_PIPE_BUSY;

pub(crate) const PIPE_NAME: &str = r"\\.\pipe\lupe";

pub(crate) async fn create_channel() -> Result<Channel, Box<dyn std::error::Error>> {
    for i in 0..3 {
        match connect().await {
            Ok(channel) => {
                return Ok(channel);
            }
            Err(e) if e.raw_os_error() == Some(ERROR_PIPE_BUSY.0 as i32) => {
                println!("Warning: failed to connect to named pipe: {:?}", e);
                let wait_times = if i == 0 { 1 } else { i * 2 };
                println!("Retry in {} seconds", wait_times);
                tokio::time::sleep(std::time::Duration::from_secs(wait_times)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }

    Err("Failed to connect to named pipe".into())
}

async fn connect() -> Result<Channel, io::Error> {
    let channel = Endpoint::try_from("http://[::]:0")
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
        .connect_with_connector(service_fn(|_: Uri| async {
            let client = ClientOptions::new().open(PIPE_NAME)?;

            Ok::<_, io::Error>(TokioIo::new(client))
        }))
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    Ok(channel)
}

pub struct TonicNamedPipeServer {
    inner: NamedPipeServer,
}

impl TonicNamedPipeServer {
    pub fn new(inner: NamedPipeServer) -> Self {
        Self { inner }
    }
}

impl Connected for TonicNamedPipeServer {
    type ConnectInfo = ();

    fn connect_info(&self) -> Self::ConnectInfo {
        ()
    }
}

impl AsyncRead for TonicNamedPipeServer {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut io::ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for TonicNamedPipeServer {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), io::Error>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

pub fn get_named_pipe_server_stream() -> impl Stream<Item = io::Result<TonicNamedPipeServer>> {
    stream! {
        let mut server = ServerOptions::new()
            .first_pipe_instance(true)
            .create(PIPE_NAME)?;

        loop {
            server.connect().await?;

            let client = TonicNamedPipeServer::new(server);

            yield Ok(client);

            server = ServerOptions::new().create(PIPE_NAME)?;
        }
    }
}
