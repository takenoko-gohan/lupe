use crate::pb::db::management_server::Management;
use crate::pb::db::operation_server::Operation;
use crate::pb::db::{
    CreateTableReply, CreateTableRequest, HealthCheckReply, HealthCheckRequest, RawQueryReply,
    RawQueryRequest, ShutdownReply, ShutdownRequest,
};
use crate::repo;
use crate::repo::{alb, s3, Client};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};
use tonic::{Request, Response, Status};
use typed_builder::TypedBuilder;

pub(crate) mod db {
    tonic::include_proto!("db");
}

#[derive(Debug, TypedBuilder)]
pub(crate) struct ManagementImpl {
    shutdown_tx: mpsc::Sender<()>,
}

#[tonic::async_trait]
impl Management for ManagementImpl {
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

#[derive(Debug, TypedBuilder)]
pub(crate) struct OperationImpl {
    db_conn: Mutex<duckdb::Connection>,
    #[builder(default)]
    init: RwLock<bool>,
}

impl OperationImpl {
    async fn get_connection(&self) -> Result<duckdb::Connection, Box<dyn std::error::Error>> {
        match self.db_conn.lock().await.try_clone() {
            Ok(conn) => Ok(conn),
            Err(e) => Err(Box::new(e)),
        }
    }
}

#[tonic::async_trait]
impl Operation for OperationImpl {
    async fn create_table(
        &self,
        request: Request<CreateTableRequest>,
    ) -> Result<Response<CreateTableReply>, Status> {
        let req = request.into_inner();

        let conn = self
            .get_connection()
            .await
            .map_err(|e| Status::internal(format!("failed to get connection: {}", e)))?;

        if !*self.init.read().await {
            repo::init(&conn)
                .map_err(|e| Status::internal(format!("failed to initialize: {}", e)))?;

            let mut init = self.init.write().await;
            *init = true;
        }

        let client: Arc<dyn Client> = match req.table_type {
            0 => Arc::new(alb::ClientImpl::builder().conn(conn).build()),
            1 => Arc::new(s3::ClientImpl::builder().conn(conn).build()),
            _ => return Err(Status::invalid_argument("invalid table type")),
        };

        match client.create_table(&req.table_name, &req.s3_uri) {
            Ok(_) => Ok(Response::new(CreateTableReply {
                message: "OK".into(),
            })),
            Err(e) => Err(Status::internal(format!("failed to create table: {}", e))),
        }
    }

    async fn raw_query(
        &self,
        request: Request<RawQueryRequest>,
    ) -> Result<Response<RawQueryReply>, Status> {
        let req = request.into_inner();

        let conn = self
            .get_connection()
            .await
            .map_err(|e| Status::internal(format!("failed to get connection: {}", e)))?;

        let result =
            repo::raw_query(&conn, &req.query).map_err(|e| Status::internal(format!("{}", e)))?;

        Ok(Response::new(result.into()))
    }
}
