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
                WITH parsed_logs AS (
                    SELECT
                        regexp_extract(col1, '^([0-9a-zA-Z]+)\s+([a-z0-9.\-]+)\s+\[([0-9/A-Za-z: +]+)\] ([^ ]+) ([^ ]+) ([^ ]+) ([^ ]+) ([^ ]+) ("[^"]*"|-) ([^ ]+) ([^ ]+) (\d+|-) (\d+|-) (\d+|-) (\d+|-) ("[^"]*"|-) ("[^"]*"|-) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+)(.*)$',
                        [
                            'bucket_owner',
                            'bucket',
                            'time',
                            'remote_ip',
                            'requester',
                            'request_id',
                            'operation',
                            'key',
                            'request_uri',
                            'http_status',
                            'error_code',
                            'bytes_sent',
                            'object_size',
                            'total_time',
                            'turn_around_time',
                            'referrer',
                            'user_agent',
                            'version_id',
                            'host_id',
                            'signature_version',
                            'cipher_suite',
                            'authentication_type',
                            'host_header',
                            'tls_version',
                            'access_point_arn',
                            'acl_required',
                            'source_region'
                        ]) AS log_struct
                    FROM read_csv(
                        '{}',
                        columns={{
                            'col1': 'VARCHAR'
                        }},
                        delim='\t',
                        quote='"',
                        escape='"',
                        header=False,
                        auto_detect=False
                    )
                )
                SELECT
                    log_struct.* exclude (time, bytes_sent, object_size, total_time, turn_around_time),
                    strptime(log_struct.time, '%d/%b/%Y:%H:%M:%S %z') AS time,
                    try_cast(log_struct.bytes_sent AS INTEGER) AS bytes_sent,
                    try_cast(log_struct.object_size AS INTEGER) AS object_size,
                    try_cast(log_struct.total_time AS INTEGER) AS total_time,
                    try_cast(log_struct.turn_around_time AS INTEGER) AS turn_around_time,
                FROM parsed_logs;"#,
                table_name, s3_uri
            ).as_str(),
            []
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repo::test_fixture;

    #[test]
    fn create_table_loads_s3_fixture() {
        let conn = Connection::open_in_memory().unwrap();
        let client = ClientImpl::builder()
            .conn(conn.try_clone().unwrap())
            .build();

        client
            .create_table("s3_logs", &test_fixture("s3.log"))
            .unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM s3_logs", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 2);

        let (bucket, status, bytes, object_size, region, access_point): (
            String,
            String,
            Option<i32>,
            Option<i32>,
            String,
            String,
        ) = conn
            .query_row(
                "SELECT bucket, http_status, bytes_sent, object_size, source_region, access_point_arn
                 FROM s3_logs
                 WHERE operation = 'REST.GET.VERSIONING'",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(bucket, "amzn-s3-demo-bucket1");
        assert_eq!(status, "200");
        assert_eq!(bytes, Some(113));
        assert_eq!(object_size, None);
        assert_eq!(region, "us-east-1");
        assert!(access_point.contains("accesspoint/example-AP"));

        let (key, bytes_sent, object_size): (String, Option<i32>, Option<i32>) = conn
            .query_row(
                "SELECT key, bytes_sent, object_size FROM s3_logs WHERE operation = 'REST.PUT.OBJECT'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(key, "s3-dg.pdf");
        assert_eq!(bytes_sent, None);
        assert_eq!(object_size, Some(4406583));
    }
}
