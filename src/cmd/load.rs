use crate::pb::db::management_client::ManagementClient;
use crate::pb::db::operation_client::OperationClient;
use crate::pb::db::{CreateTableRequest, HealthCheckRequest};
#[cfg(windows)]
use crate::util::named_pipe;
#[cfg(unix)]
use crate::util::uds;
use clap::ValueEnum;
use tokio::process::Command;
use tonic::Request;

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

pub(crate) async fn exec(
    table_type: TableType,
    table_name: Option<String>,
    s3_uri: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let exe_path = std::env::current_exe()?;

    #[cfg(unix)]
    let (channel, mut mgmt_client) = {
        if !uds::get_sock_path().exists() {
            let _child = Command::new(exe_path).arg("server").spawn()?;
        }

        let channel = uds::create_channel().await?;
        let mgmt_client = ManagementClient::new(channel.clone());

        (channel, mgmt_client)
    };

    #[cfg(windows)]
    let (channel, mut mgmt_client) = {
        let channel = match named_pipe::create_channel().await {
            Ok(channel) => channel,
            Err(_) => {
                let _child = Command::new(exe_path).arg("server").spawn()?;
                named_pipe::create_channel().await?
            }
        };
        let mgmt_client = ManagementClient::new(channel.clone());

        (channel, mgmt_client)
    };

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
    ope_client.create_table(create_table_req).await?;

    Ok(())
}
