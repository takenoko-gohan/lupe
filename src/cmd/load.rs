use crate::pb::db::management_client::ManagementClient;
use crate::pb::db::operation_client::OperationClient;
use crate::pb::db::{CreateTableRequest, HealthCheckRequest};
use crate::util::uds::{create_channel, get_sock_path};
use clap::ValueEnum;
use tokio::process::Command;
use tonic::Request;
use tracing::{debug, info};

#[derive(ValueEnum, Debug, Clone)]
pub(crate) enum TableType {
    Alb,
    S3,
}

impl From<TableType> for i32 {
    fn from(table_type: TableType) -> i32 {
        match table_type {
            TableType::Alb => 0,
            TableType::S3 => 1,
        }
    }
}

pub(crate) async fn run(
    table_type: TableType,
    table_name: Option<String>,
    s3_uri: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let exe_path = std::env::current_exe()?;

    if !get_sock_path().exists() {
        info!("starting server...");
        let _child = Command::new(exe_path).arg("server").spawn()?;
        info!("server start successfully");
    }

    let channel = create_channel().await?;
    let mut mgmt_client = ManagementClient::new(channel.clone());

    let health_req = Request::new(HealthCheckRequest::default());
    match mgmt_client.health_check(health_req).await {
        Ok(resp) => {
            debug!("response={:?}", resp);
            debug!("server is up and running");
        }
        Err(e) => {
            return Err(e.message().into());
        }
    }

    let table_name = table_name.unwrap_or_else(|| match table_type {
        TableType::Alb => "alb_logs".to_string(),
        TableType::S3 => "s3_logs".to_string(),
    });

    let mut ope_client = OperationClient::new(channel);
    let create_table_req = Request::new(CreateTableRequest {
        table_type: table_type.into(),
        table_name,
        s3_uri,
    });
    ope_client
        .create_table(create_table_req)
        .await
        .map_err(|e| e.message().to_string())?;

    Ok(())
}
