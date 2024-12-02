use crate::pb::db::operation_client::OperationClient;
use crate::pb::db::RawQueryRequest;
use crate::util::uds::create_channel;
use comfy_table::Table;
use tonic::Request;

pub(crate) async fn run(query: String) -> Result<(), Box<dyn std::error::Error>> {
    let channel = create_channel().await?;
    let mut client = OperationClient::new(channel);

    let req = Request::new(RawQueryRequest { query });
    let resp = client
        .raw_query(req)
        .await
        .map_err(|e| e.message().to_string())?
        .into_inner();

    let mut table = Table::new();
    table.set_header(resp.columns);
    for row in resp.rows {
        table.add_row(row.values);
    }

    println!("{}", table);

    Ok(())
}
