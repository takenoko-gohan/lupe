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
                            'conn_trace_id': 'VARCHAR',
                            'transformed_host': 'VARCHAR',
                            'transformed_uri': 'VARCHAR',
                            'request_transform_status': 'VARCHAR'
                        }},
                        delim=' ',
                        quote='"',
                        escape='"',
                        header=False,
                        auto_detect=False,
                        null_padding=True
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::test_fixture;

    #[test]
    fn create_table_loads_alb_fixture() {
        let conn = Connection::open_in_memory().unwrap();
        let client = ClientImpl::builder()
            .conn(conn.try_clone().unwrap())
            .build();

        client
            .create_table("alb_logs", &test_fixture("alb.log"))
            .unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM alb_logs", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 3);

        let (kind, status, target_status, request, conn_trace_id): (
            String,
            i32,
            Option<i32>,
            String,
            String,
        ) = conn
            .query_row(
                "SELECT type, elb_status_code, target_status_code, request, conn_trace_id
                 FROM alb_logs
                 WHERE type = 'http' AND elb_status_code = 200",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(kind, "http");
        assert_eq!(status, 200);
        assert_eq!(target_status, Some(200));
        assert!(request.contains("GET http://www.example.com:80/ HTTP/1.1"));
        assert_eq!(conn_trace_id, "TID_1234abcd5678ef90");

        let (cipher, domain, transform_status, transformed_host): (String, String, String, String) =
            conn.query_row(
                "SELECT ssl_cipher, domain_name, request_transform_status, transformed_host
                     FROM alb_logs
                     WHERE type = 'https'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(cipher, "ECDHE-RSA-AES128-GCM-SHA256");
        assert_eq!(domain, "www.example.com");
        assert_eq!(transform_status, "TransformSuccess");
        assert_eq!(transformed_host, "m.example.com");

        let (target_status, error_reason): (Option<i32>, String) = conn
            .query_row(
                "SELECT target_status_code, error_reason FROM alb_logs WHERE elb_status_code = 502",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(target_status, None);
        assert_eq!(error_reason, "LambdaInvalidResponse");
    }
}
