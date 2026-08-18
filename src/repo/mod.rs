use crate::pb::db::{RawQueryReply, Row};
use chrono::NaiveTime;
use duckdb::arrow::array::{Array, AsArray, PrimitiveArray, RecordBatch};
use duckdb::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Date32Type, Date64Type, DurationMicrosecondType,
    DurationMillisecondType, DurationNanosecondType, DurationSecondType, Float16Type, Float32Type,
    Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, IntervalDayTimeType,
    IntervalMonthDayNanoType, IntervalUnit, IntervalYearMonthType, Time32MillisecondType,
    Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimeUnit,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use duckdb::Connection;
use std::collections::HashMap;
use std::ops::{Add, Index};
use std::sync::Arc;
use typed_builder::TypedBuilder;

pub(crate) mod alb;
pub(crate) mod s3;

pub(crate) fn init(conn: &Connection) -> duckdb::Result<()> {
    conn.execute_batch(
        "INSTALL httpfs;
        LOAD httpfs;
        CREATE SECRET (
            TYPE S3,
            PROVIDER CREDENTIAL_CHAIN,
            CHAIN 'config;sts;sso;env'
        );",
    )
}

pub(crate) fn raw_query(
    conn: &Connection,
    query: &str,
) -> Result<QueryResult, Box<dyn std::error::Error>> {
    let mut stmt = conn.prepare(query)?;
    let columns: Columns = stmt
        .query_arrow([])
        .map_err(|e| format!("failed to query: {:?}", e))?
        .collect::<Vec<RecordBatch>>()
        .try_into()?;

    if columns.len() == 0 {
        Ok(QueryResult::builder()
            .header(stmt.column_names())
            .rows(Vec::new())
            .build())
    } else {
        QueryResult::try_from(columns)
    }
}

pub(crate) trait Client {
    fn create_table(&self, table_name: &str, s3_uri: &str) -> duckdb::Result<usize>;
}

#[derive(Debug, PartialEq, TypedBuilder)]
pub(crate) struct QueryResult {
    header: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl From<QueryResult> for RawQueryReply {
    fn from(value: QueryResult) -> Self {
        Self {
            columns: value.header,
            rows: value
                .rows
                .iter()
                .map(|row| Row {
                    values: row.clone(),
                })
                .collect(),
        }
    }
}

impl TryFrom<Columns> for QueryResult {
    type Error = Box<dyn std::error::Error>;

    fn try_from(value: Columns) -> Result<Self, Self::Error> {
        let mut header = Vec::new();
        let mut rows = Vec::new();

        for i in 0..value.len() {
            let Some(column) = value.get(i) else {
                return Err(format!("failed to get column: {:?}", i).into());
            };
            header.push(column.name.clone());

            for j in 0..column.values.len() {
                if rows.len() <= j {
                    rows.push(Vec::new());
                }
                rows[j].push(column.values[j].clone());
            }
        }

        Ok(Self::builder().header(header).rows(rows).build())
    }
}

struct Columns(HashMap<usize, Column>);

impl Columns {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn get(&self, index: usize) -> Option<&Column> {
        self.0.get(&index)
    }
}

impl From<HashMap<usize, Column>> for Columns {
    fn from(value: HashMap<usize, Column>) -> Self {
        Self(value)
    }
}

#[derive(TypedBuilder)]
struct Column {
    name: String,
    values: Values,
}

impl TryInto<Columns> for Vec<RecordBatch> {
    type Error = Box<dyn std::error::Error>;

    fn try_into(self) -> Result<Columns, Self::Error> {
        let mut columns: HashMap<usize, Column> = HashMap::new();
        for record in self.iter() {
            let fields = record.schema().fields().to_owned();

            for i in 0..record.num_columns() {
                let field_name = fields[i].name().clone();
                let values = Values::try_from(record.column(i))?;

                match columns.get_mut(&i) {
                    Some(column) => {
                        column.values.append(values);
                    }
                    None => {
                        columns
                            .insert(i, Column::builder().name(field_name).values(values).build());
                    }
                }
            }
        }

        Ok(columns.into())
    }
}

struct Values(Vec<String>);

impl Values {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn append(&mut self, other: Self) {
        self.0.extend(other.0);
    }
}

fn format_temporal<T, D>(
    array: &PrimitiveArray<T>,
    mut convert: impl FnMut(&PrimitiveArray<T>, usize) -> Option<D>,
) -> Values
where
    T: ArrowPrimitiveType,
    D: ToString,
{
    (0..array.len())
        .map(|i| {
            if array.is_null(i) {
                "NULL".to_string()
            } else {
                convert(array, i)
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "NULL".to_string())
            }
        })
        .collect()
}

impl From<Vec<String>> for Values {
    fn from(value: Vec<String>) -> Self {
        Self(value)
    }
}

impl FromIterator<String> for Values {
    fn from_iter<T: IntoIterator<Item = String>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl Index<usize> for Values {
    type Output = String;

    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl TryFrom<&Arc<dyn Array>> for Values {
    type Error = Box<dyn std::error::Error>;

    fn try_from(value: &Arc<dyn Array>) -> Result<Self, Self::Error> {
        match value.data_type() {
            DataType::Null => Ok(vec!["NULL".to_string(); value.len()].into()),
            DataType::Boolean => {
                let array = value.as_boolean();
                Ok(array
                    .iter()
                    .map(|v| match v {
                        Some(v) => v.to_string(),
                        None => "NULL".to_string(),
                    })
                    .collect::<Values>())
            }
            DataType::Int8 => {
                let array = value.as_primitive::<Int8Type>();
                Ok(array
                    .iter()
                    .map(|v| match v {
                        Some(v) => v.to_string(),
                        None => "NULL".to_string(),
                    })
                    .collect::<Values>())
            }
            DataType::Int16 => {
                let array = value.as_primitive::<Int16Type>();
                Ok(array
                    .iter()
                    .map(|v| match v {
                        Some(v) => v.to_string(),
                        None => "NULL".to_string(),
                    })
                    .collect::<Values>())
            }
            DataType::Int32 => {
                let array = value.as_primitive::<Int32Type>();
                Ok(array
                    .iter()
                    .map(|v| match v {
                        Some(v) => v.to_string(),
                        None => "NULL".to_string(),
                    })
                    .collect::<Values>())
            }
            DataType::Int64 => {
                let array = value.as_primitive::<Int64Type>();
                Ok(array
                    .iter()
                    .map(|v| match v {
                        Some(v) => v.to_string(),
                        None => "NULL".to_string(),
                    })
                    .collect::<Values>())
            }
            DataType::UInt8 => {
                let array = value.as_primitive::<UInt8Type>();
                Ok(array
                    .iter()
                    .map(|v| match v {
                        Some(v) => v.to_string(),
                        None => "NULL".to_string(),
                    })
                    .collect::<Values>())
            }
            DataType::UInt16 => {
                let array = value.as_primitive::<UInt16Type>();
                Ok(array
                    .iter()
                    .map(|v| match v {
                        Some(v) => v.to_string(),
                        None => "NULL".to_string(),
                    })
                    .collect::<Values>())
            }
            DataType::UInt32 => {
                let array = value.as_primitive::<UInt32Type>();
                Ok(array
                    .iter()
                    .map(|v| match v {
                        Some(v) => v.to_string(),
                        None => "NULL".to_string(),
                    })
                    .collect::<Values>())
            }
            DataType::UInt64 => {
                let array = value.as_primitive::<UInt64Type>();
                Ok(array
                    .iter()
                    .map(|v| match v {
                        Some(v) => v.to_string(),
                        None => "NULL".to_string(),
                    })
                    .collect::<Values>())
            }
            DataType::Float16 => {
                let array = value.as_primitive::<Float16Type>();
                Ok(array
                    .iter()
                    .map(|v| match v {
                        Some(v) => v.to_string(),
                        None => "NULL".to_string(),
                    })
                    .collect::<Values>())
            }
            DataType::Float32 => {
                let array = value.as_primitive::<Float32Type>();
                Ok(array
                    .iter()
                    .map(|v| match v {
                        Some(v) => v.to_string(),
                        None => "NULL".to_string(),
                    })
                    .collect::<Values>())
            }
            DataType::Float64 => {
                let array = value.as_primitive::<Float64Type>();
                Ok(array
                    .iter()
                    .map(|v| match v {
                        Some(v) => v.to_string(),
                        None => "NULL".to_string(),
                    })
                    .collect::<Values>())
            }
            DataType::Timestamp(unit, _) => match unit {
                TimeUnit::Second => Ok(format_temporal(
                    value.as_primitive::<TimestampSecondType>(),
                    PrimitiveArray::value_as_datetime,
                )),
                TimeUnit::Millisecond => Ok(format_temporal(
                    value.as_primitive::<TimestampMillisecondType>(),
                    PrimitiveArray::value_as_datetime,
                )),
                TimeUnit::Microsecond => Ok(format_temporal(
                    value.as_primitive::<TimestampMicrosecondType>(),
                    PrimitiveArray::value_as_datetime,
                )),
                TimeUnit::Nanosecond => Ok(format_temporal(
                    value.as_primitive::<TimestampNanosecondType>(),
                    PrimitiveArray::value_as_datetime,
                )),
            },
            DataType::Date32 => Ok(format_temporal(
                value.as_primitive::<Date32Type>(),
                PrimitiveArray::value_as_date,
            )),
            DataType::Date64 => Ok(format_temporal(
                value.as_primitive::<Date64Type>(),
                PrimitiveArray::value_as_date,
            )),
            DataType::Time32(unit) => match unit {
                TimeUnit::Second => Ok(format_temporal(
                    value.as_primitive::<Time32SecondType>(),
                    PrimitiveArray::value_as_time,
                )),
                TimeUnit::Millisecond => Ok(format_temporal(
                    value.as_primitive::<Time32MillisecondType>(),
                    PrimitiveArray::value_as_time,
                )),
                _ => Err(format!("unsupported time unit: {:?}", unit).into()),
            },
            DataType::Time64(unit) => match unit {
                TimeUnit::Microsecond => Ok(format_temporal(
                    value.as_primitive::<Time64MicrosecondType>(),
                    PrimitiveArray::value_as_time,
                )),
                TimeUnit::Nanosecond => Ok(format_temporal(
                    value.as_primitive::<Time64NanosecondType>(),
                    PrimitiveArray::value_as_time,
                )),
                _ => Err(format!("unsupported time unit: {:?}", unit).into()),
            },
            DataType::Duration(unit) => match unit {
                TimeUnit::Second => Ok(format_temporal(
                    value.as_primitive::<DurationSecondType>(),
                    PrimitiveArray::value_as_duration,
                )),
                TimeUnit::Millisecond => Ok(format_temporal(
                    value.as_primitive::<DurationMillisecondType>(),
                    PrimitiveArray::value_as_duration,
                )),
                TimeUnit::Microsecond => Ok(format_temporal(
                    value.as_primitive::<DurationMicrosecondType>(),
                    PrimitiveArray::value_as_duration,
                )),
                TimeUnit::Nanosecond => Ok(format_temporal(
                    value.as_primitive::<DurationNanosecondType>(),
                    PrimitiveArray::value_as_duration,
                )),
            },
            DataType::Interval(unit) => match unit {
                IntervalUnit::YearMonth => {
                    let array = value.as_primitive::<IntervalYearMonthType>();
                    Ok(array
                        .iter()
                        .map(|v| match v {
                            Some(v) => {
                                let year = v / 12;
                                let month = v % 12;
                                format!("{} year {} month", year, month)
                            }
                            None => "NULL".to_string(),
                        })
                        .collect())
                }
                IntervalUnit::DayTime => {
                    let array = value.as_primitive::<IntervalDayTimeType>();
                    Ok(array
                        .iter()
                        .map(|v| match v {
                            Some(v) => {
                                let day = v.days;
                                let time = NaiveTime::default()
                                    .add(chrono::Duration::milliseconds(v.milliseconds as i64))
                                    .to_string();

                                let mut value = "".to_string();
                                if day != 0 {
                                    value.push_str(&format!("{} days", day));
                                }
                                if time != "00:00:00" {
                                    value.push_str(&format!(" {}", time));
                                }

                                value
                            }
                            None => "NULL".to_string(),
                        })
                        .collect())
                }
                IntervalUnit::MonthDayNano => {
                    let array = value.as_primitive::<IntervalMonthDayNanoType>();
                    Ok(array
                        .iter()
                        .map(|v| match v {
                            Some(v) => {
                                let month = v.months;
                                let day = v.days;
                                let time = NaiveTime::default()
                                    .add(chrono::Duration::nanoseconds(v.nanoseconds))
                                    .to_string();
                                let mut value = "".to_string();
                                if month != 0 {
                                    value.push_str(&format!("{} months", month));
                                }
                                if day != 0 {
                                    value.push_str(&format!(" {} days", day));
                                }
                                if time != "00:00:00" {
                                    value.push_str(&format!(" {}", time));
                                }

                                value
                            }
                            None => "NULL".to_string(),
                        })
                        .collect::<Values>())
                }
            },
            DataType::Binary => {
                let array = value.as_binary::<i32>();
                Ok(array
                    .iter()
                    .map(|v| match v {
                        Some(v) => String::from_utf8(v.to_vec())
                            .unwrap_or_else(|e| format!("failed to convert to string: {:?}", e)),
                        None => "NULL".to_string(),
                    })
                    .collect())
            }
            DataType::FixedSizeBinary(_) => {
                let array = value.as_fixed_size_binary();
                Ok(array
                    .iter()
                    .map(|v| match v {
                        Some(v) => String::from_utf8(v.to_vec())
                            .unwrap_or_else(|e| format!("failed to convert to string: {:?}", e)),
                        None => "NULL".to_string(),
                    })
                    .collect())
            }
            DataType::LargeBinary => {
                let array = value.as_binary::<i64>();
                Ok(array
                    .iter()
                    .map(|v| match v {
                        Some(v) => String::from_utf8(v.to_vec())
                            .unwrap_or_else(|e| format!("failed to convert to string: {:?}", e)),
                        None => "NULL".to_string(),
                    })
                    .collect())
            }
            DataType::BinaryView => {
                let array = value.as_binary_view();
                Ok(array
                    .iter()
                    .map(|v| match v {
                        Some(v) => String::from_utf8(v.to_vec())
                            .unwrap_or_else(|e| format!("failed to convert to string: {:?}", e)),
                        None => "NULL".to_string(),
                    })
                    .collect())
            }
            DataType::Utf8 => {
                let array = value.as_string::<i32>();
                array
                    .iter()
                    .map(|v| match v {
                        Some(v) => Ok(v.to_string()),
                        None => Ok("NULL".to_string()),
                    })
                    .collect()
            }
            DataType::LargeUtf8 => {
                let array = value.as_string::<i64>();
                array
                    .iter()
                    .map(|v| match v {
                        Some(v) => Ok(v.to_string()),
                        None => Ok("NULL".to_string()),
                    })
                    .collect()
            }
            DataType::Utf8View => {
                let array = value.as_string_view();
                array
                    .iter()
                    .map(|v| match v {
                        Some(v) => Ok(v.to_string()),
                        None => Ok("NULL".to_string()),
                    })
                    .collect()
            }
            _ => Err(format!("unsupported data type: {:?}", value).into()),
        }
    }
}

#[cfg(test)]
pub(crate) fn test_fixture(name: &str) -> String {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
        .to_string_lossy()
        .into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use duckdb::arrow::array::{Int32Array, StringArray};
    use duckdb::arrow::datatypes::{Field, Schema};

    fn record_batch(ids: Vec<Option<i32>>, names: Vec<Option<&str>>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, true),
                Field::new("name", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn query_result_concatenates_record_batches() {
        let columns: Columns = vec![
            record_batch(vec![Some(1), Some(2)], vec![Some("a"), Some("b")]),
            record_batch(vec![Some(3), None], vec![Some("c"), None]),
        ]
        .try_into()
        .unwrap();
        let result = QueryResult::try_from(columns).unwrap();
        assert_eq!(result.header, vec!["id", "name"]);
        assert_eq!(
            result.rows,
            vec![
                vec!["1".to_string(), "a".to_string()],
                vec!["2".to_string(), "b".to_string()],
                vec!["3".to_string(), "c".to_string()],
                vec!["NULL".to_string(), "NULL".to_string()],
            ]
        );
    }

    #[test]
    fn raw_query_formats_common_types() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE t (
                flag BOOLEAN,
                n INTEGER,
                f DOUBLE,
                s VARCHAR,
                ts TIMESTAMP
            );
            INSERT INTO t VALUES
                (true, 7, 1.5, 'ok', TIMESTAMP '2024-01-02 03:04:05'),
                (NULL, NULL, NULL, NULL, NULL);",
        )
        .unwrap();

        let result = raw_query(&conn, "SELECT * FROM t ORDER BY n NULLS LAST").unwrap();
        assert_eq!(result.header, vec!["flag", "n", "f", "s", "ts"]);
        assert_eq!(result.rows.len(), 2);
        assert_eq!(
            result.rows[0],
            vec!["true", "7", "1.5", "ok", "2024-01-02 03:04:05"]
        );
        assert_eq!(result.rows[1], vec!["NULL", "NULL", "NULL", "NULL", "NULL"]);
    }

    #[test]
    fn raw_query_empty_result_keeps_headers() {
        let conn = Connection::open_in_memory().unwrap();
        let result = raw_query(&conn, "SELECT 1 AS a WHERE false").unwrap();
        assert_eq!(result.header, vec!["a"]);
        assert!(result.rows.is_empty());
    }

    #[test]
    fn raw_query_rejects_invalid_sql() {
        let conn = Connection::open_in_memory().unwrap();
        let err = raw_query(&conn, "SELECT FROM").unwrap_err();
        assert!(!err.to_string().is_empty());
    }
}
