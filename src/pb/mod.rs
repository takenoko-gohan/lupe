use crate::pb::duckdb::{HealthCheckReply, HealthCheckRequest, ShutdownReply, ShutdownRequest};
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};
use typed_builder::TypedBuilder;

pub(crate) mod duckdb {
    tonic::include_proto!("duckdb");
}

#[derive(Debug, TypedBuilder)]
pub(crate) struct ManagementImpl {
    shutdown_tx: mpsc::Sender<()>,
}

#[tonic::async_trait]
impl duckdb::management_server::Management for ManagementImpl {
    async fn health_check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckReply>, Status> {
        let reply = HealthCheckReply {
            message: "OK".into(),
        };
        Ok(Response::new(reply))
    }

    async fn shutdown(
        &self,
        _request: Request<ShutdownRequest>,
    ) -> Result<Response<ShutdownReply>, Status> {
        self.shutdown_tx
            .send(())
            .await
            .map_err(|_| Status::internal("failed to send shutdown signal"))?;

        Ok(Response::new(ShutdownReply {
            message: "OK".into(),
        }))
    }
}
