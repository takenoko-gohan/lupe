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
                        regexp_extract(col1, '^([0-9a-zA-Z]+)\s+([a-z0-9.\-]+)\s+\[([0-9/A-Za-z: +]+)\] ([^ ]+) ([^ ]+) ([^ ]+) ([^ ]+) ([^ ]+) ("[^"]*"|-) ([^ ]+) ([^ ]+) (\d+|-) (\d+|-) (\d+|-) (\d+|-) ("[^"]*"|-) ("[^"]*"|-) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+)(.*)$',
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
                            'acl_required'
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
                    log_struct.* exclude (bytes_sent, object_size, total_time, turn_around_time),
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
