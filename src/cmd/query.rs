use crate::pb::db::operation_client::OperationClient;
use crate::pb::db::RawQueryRequest;
#[cfg(windows)]
use crate::util::named_pipe;
#[cfg(unix)]
use crate::util::uds;
use comfy_table::Table;
use tonic::Request;

pub(crate) async fn exec(query: String) -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(unix)]
    let channel = uds::create_channel().await?;
    #[cfg(windows)]
    let channel = named_pipe::create_channel().await?;
    let mut client = OperationClient::new(channel);

    let req = Request::new(RawQueryRequest { query });
    let resp = client.raw_query(req).await?.into_inner();

    let mut table = Table::new();
    table.set_header(resp.columns);
    for row in resp.rows {
        table.add_row(row.values);
    }

    println!("{}", table);

    Ok(())
}
