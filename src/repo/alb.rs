use crate::repo::Client;
use duckdb::Connection;
use typed_builder::TypedBuilder;

#[derive(TypedBuilder)]
pub(crate) struct ClientImpl {
    conn: Connection,
}

impl Client for ClientImpl {
    fn create_table(&self, table_name: &str, s3_uri: &str) -> duckdb::Result<usize> {
        self.conn.execute(
            format!(
                r#"CREATE TABLE {} AS
                WITH csv_logs AS (
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
                    )
                )
                SELECT
                    csv_logs.* exclude (target_status_code),
                    try_cast(csv_logs.target_status_code AS INTEGER) AS target_status_code
                FROM csv_logs;"#,
                table_name, s3_uri
            )
            .as_str(),
            [],
        )
    }
}
