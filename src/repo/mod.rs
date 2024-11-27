use crate::pb::db::{RawQueryReply, Row};
use chrono::DateTime;
use duckdb::arrow::array::{
    Array, BooleanArray, Float16Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, LargeStringArray, RecordBatch, StringArray, StringViewArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use duckdb::arrow::datatypes::{DataType, TimeUnit};
use duckdb::Connection;
use std::collections::HashMap;
use std::ops::Index;
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
        .map_err(|e| format!("Failed to query: {:?}", e))?
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

#[derive(TypedBuilder)]
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
                return Err(format!("Failed to get column: {:?}", i).into());
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
                        column.values = values;
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
                let Some(array) = value.as_any().downcast_ref::<BooleanArray>() else {
                    return Err(format!("Failed to downcast to BooleanArray: {:?}", value).into());
                };
                array
                    .iter()
                    .map(|v| match v {
                        Some(v) => Ok(v.to_string()),
                        None => Ok("NULL".to_string()),
                    })
                    .collect()
            }
            DataType::Int8 => {
                let Some(array) = value.as_any().downcast_ref::<Int8Array>() else {
                    return Err(format!("Failed to downcast to Int8Array: {:?}", value).into());
                };
                array
                    .iter()
                    .map(|v| match v {
                        Some(v) => Ok(v.to_string()),
                        None => Ok("NULL".to_string()),
                    })
                    .collect()
            }
            DataType::Int16 => {
                let Some(array) = value.as_any().downcast_ref::<Int16Array>() else {
                    return Err(format!("Failed to downcast to Int16Array: {:?}", value).into());
                };
                array
                    .iter()
                    .map(|v| match v {
                        Some(v) => Ok(v.to_string()),
                        None => Ok("NULL".to_string()),
                    })
                    .collect()
            }
            DataType::Int32 => {
                let Some(array) = value.as_any().downcast_ref::<Int32Array>() else {
                    return Err(format!("Failed to downcast to Int32Array: {:?}", value).into());
                };
                array
                    .iter()
                    .map(|v| match v {
                        Some(v) => Ok(v.to_string()),
                        None => Ok("NULL".to_string()),
                    })
                    .collect()
            }
            DataType::Int64 => {
                let Some(array) = value.as_any().downcast_ref::<Int64Array>() else {
                    return Err(format!("Failed to downcast to Int64Array: {:?}", value).into());
                };
                array
                    .iter()
                    .map(|v| match v {
                        Some(v) => Ok(v.to_string()),
                        None => Ok("NULL".to_string()),
                    })
                    .collect()
            }
            DataType::UInt8 => {
                let Some(array) = value.as_any().downcast_ref::<UInt8Array>() else {
                    return Err(format!("Failed to downcast to UInt8Array: {:?}", value).into());
                };
                array
                    .iter()
                    .map(|v| match v {
                        Some(v) => Ok(v.to_string()),
                        None => Ok("NULL".to_string()),
                    })
                    .collect()
            }
            DataType::UInt16 => {
                let Some(array) = value.as_any().downcast_ref::<UInt16Array>() else {
                    return Err(format!("Failed to downcast to UInt16Array: {:?}", value).into());
                };
                array
                    .iter()
                    .map(|v| match v {
                        Some(v) => Ok(v.to_string()),
                        None => Ok("NULL".to_string()),
                    })
                    .collect()
            }
            DataType::UInt32 => {
                let Some(array) = value.as_any().downcast_ref::<UInt32Array>() else {
                    return Err(format!("Failed to downcast to UInt32Array: {:?}", value).into());
                };
                array
                    .iter()
                    .map(|v| match v {
                        Some(v) => Ok(v.to_string()),
                        None => Ok("NULL".to_string()),
                    })
                    .collect()
            }
            DataType::UInt64 => {
                let Some(array) = value.as_any().downcast_ref::<UInt64Array>() else {
                    return Err(format!("Failed to downcast to UInt64Array: {:?}", value).into());
                };
                array
                    .iter()
                    .map(|v| match v {
                        Some(v) => Ok(v.to_string()),
                        None => Ok("NULL".to_string()),
                    })
                    .collect()
            }
            DataType::Float16 => {
                let Some(array) = value.as_any().downcast_ref::<Float16Array>() else {
                    return Err(format!("Failed to downcast to Float16Array: {:?}", value).into());
                };
                array
                    .iter()
                    .map(|v| match v {
                        Some(v) => Ok(v.to_string()),
                        None => Ok("NULL".to_string()),
                    })
                    .collect()
            }
            DataType::Float32 => {
                let Some(array) = value.as_any().downcast_ref::<Float32Array>() else {
                    return Err(format!("Failed to downcast to Float32Array: {:?}", value).into());
                };
                array
                    .iter()
                    .map(|v| match v {
                        Some(v) => Ok(v.to_string()),
                        None => Ok("NULL".to_string()),
                    })
                    .collect()
            }
            DataType::Float64 => {
                let Some(array) = value.as_any().downcast_ref::<Float64Array>() else {
                    return Err(format!("Failed to downcast to Float64Array: {:?}", value).into());
                };
                array
                    .iter()
                    .map(|v| match v {
                        Some(v) => Ok(v.to_string()),
                        None => Ok("NULL".to_string()),
                    })
                    .collect()
            }
            DataType::Timestamp(time_unit, _) => match time_unit {
                TimeUnit::Second => {
                    let Some(array) = value.as_any().downcast_ref::<TimestampSecondArray>() else {
                        return Err(format!("Failed to downcast to Int64Array: {:?}", value).into());
                    };
                    array
                        .iter()
                        .map(|v| match v {
                            Some(v) => {
                                let Some(dt) = DateTime::from_timestamp(v, 0) else {
                                    return Err(
                                        format!("Failed to convert to DateTime: {:?}", v).into()
                                    );
                                };
                                Ok(dt.to_rfc3339())
                            }
                            None => Ok("NULL".to_string()),
                        })
                        .collect()
                }
                TimeUnit::Millisecond => {
                    let Some(array) = value.as_any().downcast_ref::<TimestampMillisecondArray>()
                    else {
                        return Err(format!("Failed to downcast to Int64Array: {:?}", value).into());
                    };
                    array
                        .iter()
                        .map(|v| match v {
                            Some(v) => {
                                let Some(dt) = DateTime::from_timestamp_millis(v) else {
                                    return Err(
                                        format!("Failed to convert to DateTime: {:?}", v).into()
                                    );
                                };
                                Ok(dt.to_rfc3339())
                            }
                            None => Ok("NULL".to_string()),
                        })
                        .collect()
                }
                TimeUnit::Microsecond => {
                    let Some(array) = value.as_any().downcast_ref::<TimestampMicrosecondArray>()
                    else {
                        return Err(format!("Failed to downcast to Int64Array: {:?}", value).into());
                    };
                    array
                        .iter()
                        .map(|v| match v {
                            Some(v) => {
                                let Some(dt) = DateTime::from_timestamp_micros(v) else {
                                    return Err(
                                        format!("Failed to convert to DateTime: {:?}", v).into()
                                    );
                                };
                                Ok(dt.to_rfc3339())
                            }
                            None => Ok("NULL".to_string()),
                        })
                        .collect()
                }
                TimeUnit::Nanosecond => {
                    let Some(array) = value.as_any().downcast_ref::<TimestampNanosecondArray>()
                    else {
                        return Err(format!("Failed to downcast to Int64Array: {:?}", value).into());
                    };
                    array
                        .iter()
                        .map(|v| match v {
                            Some(v) => {
                                let dt = DateTime::from_timestamp_nanos(v);
                                Ok(dt.to_rfc3339())
                            }
                            None => Ok("NULL".to_string()),
                        })
                        .collect()
                }
            },
            DataType::Utf8 => {
                let Some(array) = value.as_any().downcast_ref::<StringArray>() else {
                    return Err(format!("Failed to downcast to StringArray: {:?}", value).into());
                };
                array
                    .iter()
                    .map(|v| match v {
                        Some(v) => Ok(v.to_string()),
                        None => Ok("NULL".to_string()),
                    })
                    .collect()
            }
            DataType::LargeUtf8 => {
                let Some(array) = value.as_any().downcast_ref::<LargeStringArray>() else {
                    return Err(format!("Failed to downcast to StringArray: {:?}", value).into());
                };
                array
                    .iter()
                    .map(|v| match v {
                        Some(v) => Ok(v.to_string()),
                        None => Ok("NULL".to_string()),
                    })
                    .collect()
            }
            DataType::Utf8View => {
                let Some(array) = value.as_any().downcast_ref::<StringViewArray>() else {
                    return Err(format!("Failed to downcast to StringArray: {:?}", value).into());
                };
                array
                    .iter()
                    .map(|v| match v {
                        Some(v) => Ok(v.to_string()),
                        None => Ok("NULL".to_string()),
                    })
                    .collect()
            }
            _ => Err(format!("Unsupported data type: {:?}", value).into()),
        }
    }
}
