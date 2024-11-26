use crate::pb::db::management_server::Management;
use crate::pb::db::operation_server::Operation;
use crate::pb::db::{
    CreateTableReply, CreateTableRequest, HealthCheckReply, HealthCheckRequest, RawQueryReply,
    RawQueryRequest, Row, ShutdownReply, ShutdownRequest,
};
use crate::util::db_schema::AlbLogsRow;
use tokio::sync::{mpsc, Mutex};
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

        conn.execute_batch(
            "INSTALL httpfs;
            LOAD httpfs;
            CREATE SECRET (
                TYPE S3,
                PROVIDER CREDENTIAL_CHAIN,
                CHAIN 'config;sts;sso;env'
            );",
        )
        .map_err(|e| Status::internal(format!("failed to initialize: {}", e)))?;

        let result = conn.execute(
            format!(
                r#"CREATE TABLE {} AS
                SELECT * FROM read_csv(
                    '{}',
                    columns={{
                        'type': 'VARCHAR',
                        'time': 'TIMESTAMP',
                        'elb': 'VARCHAR',
                        'client_port': 'VARCHAR',
                        'target_port': 'VARCHAR',
                        'request_processing_time': 'DOUBLE',
                        'target_processing_time': 'DOUBLE',
                        'response_processing_time': 'DOUBLE',
                        'elb_status_code': 'INTEGER',
                        'target_status_code': 'VARCHAR',
                        'received_bytes': 'BIGINT',
                        'sent_bytes': 'BIGINT',
                        'request': 'VARCHAR',
                        'user_agent': 'VARCHAR',
                        'ssl_cipher': 'VARCHAR',
                        'ssl_protocol': 'VARCHAR',
                        'target_group_arn': 'VARCHAR',
                        'trace_id': 'VARCHAR',
                        'domain_name': 'VARCHAR',
                        'chosen_cert_arn': 'VARCHAR',
                        'matched_rule_priority': 'VARCHAR',
                        'request_creation_time': 'TIMESTAMP',
                        'actions_executed': 'VARCHAR',
                        'redirect_url': 'VARCHAR',
                        'error_reason': 'VARCHAR',
                        'target_port_list': 'VARCHAR',
                        'target_status_code_list': 'VARCHAR',
                        'classification': 'VARCHAR',
                        'classification_reason': 'VARCHAR',
                        'conn_trace_id': 'VARCHAR'
                    }},
                    delim=' ',
                    quote='"',
                    escape='"',
                    header=False,
                    auto_detect=False
                );"#,
                req.table_name, req.s3_prefix
            )
            .as_str(),
            [],
        );

        match result {
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

        let mut stmt = conn
            .prepare(&req.query)
            .map_err(|e| Status::internal(format!("failed to prepare query: {}", e)))?;

        let rows_stmt = stmt
            .query_map([], |row| {
                let time = row.get::<_, i64>("time")?;
                let request_creation_time = row.get::<_, i64>("request_creation_time")?;
                let row = AlbLogsRow::builder()
                    .conn_type(row.get("type").ok())
                    .time(chrono::DateTime::from_timestamp_micros(time))
                    .elb(row.get("elb").ok())
                    .client_port(row.get("client_port").ok())
                    .target_port(row.get("target_port").ok())
                    .request_processing_time(row.get("request_processing_time").ok())
                    .target_processing_time(row.get("target_processing_time").ok())
                    .response_processing_time(row.get("response_processing_time").ok())
                    .elb_status_code(row.get("elb_status_code").ok())
                    .target_status_code(row.get("target_status_code").ok())
                    .received_bytes(row.get("received_bytes").ok())
                    .sent_bytes(row.get("sent_bytes").ok())
                    .request(row.get("request").ok())
                    .user_agent(row.get("user_agent").ok())
                    .ssl_cipher(row.get("ssl_cipher").ok())
                    .ssl_protocol(row.get("ssl_protocol").ok())
                    .target_group_arn(row.get("target_group_arn").ok())
                    .trace_id(row.get("trace_id").ok())
                    .domain_name(row.get("domain_name").ok())
                    .chosen_cert_arn(row.get("chosen_cert_arn").ok())
                    .matched_rule_priority(row.get("matched_rule_priority").ok())
                    .request_creation_time(chrono::DateTime::from_timestamp_micros(
                        request_creation_time,
                    ))
                    .actions_executed(row.get("actions_executed").ok())
                    .redirect_url(row.get("redirect_url").ok())
                    .error_reason(row.get("error_reason").ok())
                    .target_port_list(row.get("target_port_list").ok())
                    .target_status_code_list(row.get("target_status_code_list").ok())
                    .classification(row.get("classification").ok())
                    .classification_reason(row.get("classification_reason").ok())
                    .conn_trace_id(row.get("conn_trace_id").ok())
                    .build();

                Ok(row)
            })
            .map_err(|e| Status::internal(format!("failed to execute query: {}", e)))?;

        let mut alb_logs_rows: Vec<AlbLogsRow> = Vec::new();
        for row in rows_stmt {
            alb_logs_rows
                .push(row.map_err(|e| Status::internal(format!("failed to get row: {}", e)))?);
        }

        let columns = stmt.column_names();

        let mut rows: Vec<Row> = Vec::new();
        for alb_logs_row in alb_logs_rows {
            let mut values: Vec<String> = Vec::new();
            for column in &columns {
                let value = alb_logs_row.get_field(column).unwrap_or("NULL".to_string());
                values.push(value);
            }
            rows.push(Row { values });
        }

        Ok(Response::new(RawQueryReply { columns, rows }))
    }
}
