use crate::pb;
use crate::pb::db::management_client::ManagementClient;
use crate::pb::db::operation_client::OperationClient;
use crate::pb::db::{CreateTableRequest, HealthCheckRequest};
use crate::util::uds::{create_channel, get_sock_path};
use clap::ValueEnum;
use tokio::process::Command;
use tonic::Request;

#[derive(ValueEnum, Debug, Clone)]
pub(crate) enum DataType {
    Alb,
}

impl From<pb::db::create_table_request::DataType> for DataType {
    fn from(data_type: pb::db::create_table_request::DataType) -> Self {
        match data_type {
            pb::db::create_table_request::DataType::Alb => DataType::Alb,
        }
    }
}

pub(crate) async fn exec(
    data_type: DataType,
    table_name: Option<String>,
    s3_prefix: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let exe_path = std::env::current_exe()?;

    if !get_sock_path().exists() {
        let _child = Command::new(exe_path).arg("server").spawn()?;
    }

    let channel = create_channel().await?;
    let mut mgmt_client = ManagementClient::new(channel.clone());

    let health_req = Request::new(HealthCheckRequest::default());
    match mgmt_client.health_check(health_req).await {
        Ok(resp) => {
            println!("Debug: response={:?}", resp);
            println!("Debug: Server is up and running");
        }
        Err(e) => {
            eprintln!("Error: {:?}", e);
        }
    }

    let table_name = table_name.unwrap_or_else(|| match data_type {
        DataType::Alb => "alb_logs".to_string(),
    });

    let mut ope_client = OperationClient::new(channel);
    let create_table_req = Request::new(CreateTableRequest {
        table_name,
        s3_prefix,
    });
    ope_client.create_table(create_table_req).await?;

    Ok(())
}
