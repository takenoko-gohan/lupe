use crate::pb::db::{RawQueryReply, Row};
use duckdb::arrow::array::{
    Array, AsArray, LargeStringArray, RecordBatch, StringArray, StringViewArray,
};
use duckdb::arrow::datatypes::{
    DataType, Date32Type, Date64Type, DurationMicrosecondType, DurationMillisecondType,
    DurationNanosecondType, DurationSecondType, Float16Type, Float32Type, Float64Type, Int16Type,
    Int32Type, Int64Type, Int8Type, IntervalDayTimeType, IntervalUnit, IntervalYearMonthType,
    Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimeUnit,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
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
                TimeUnit::Second => {
                    let array = value.as_primitive::<TimestampSecondType>();
                    let mut values = Vec::new();
                    for i in 0..array.len() {
                        let v = array.value_as_datetime(i);
                        match v {
                            Some(v) => values.push(v.to_string()),
                            None => values.push("NULL".to_string()),
                        }
                    }
                    Ok(values.into())
                }
                TimeUnit::Millisecond => {
                    let array = value.as_primitive::<TimestampMillisecondType>();
                    let mut values = Vec::new();
                    for i in 0..array.len() {
                        let v = array.value_as_datetime(i);
                        match v {
                            Some(v) => values.push(v.to_string()),
                            None => values.push("NULL".to_string()),
                        }
                    }
                    Ok(values.into())
                }
                TimeUnit::Microsecond => {
                    let array = value.as_primitive::<TimestampMicrosecondType>();
                    let mut values = Vec::new();
                    for i in 0..array.len() {
                        let v = array.value_as_datetime(i);
                        match v {
                            Some(v) => values.push(v.to_string()),
                            None => values.push("NULL".to_string()),
                        }
                    }
                    Ok(values.into())
                }
                TimeUnit::Nanosecond => {
                    let array = value.as_primitive::<TimestampNanosecondType>();
                    let mut values = Vec::new();
                    for i in 0..array.len() {
                        let v = array.value_as_datetime(i);
                        match v {
                            Some(v) => values.push(v.to_string()),
                            None => values.push("NULL".to_string()),
                        }
                    }
                    Ok(values.into())
                }
            },
            DataType::Date32 => {
                let array = value.as_primitive::<Date32Type>();
                let mut values = Vec::new();
                for i in 0..array.len() {
                    let v = array.value_as_date(i);
                    match v {
                        Some(v) => values.push(v.to_string()),
                        None => values.push("NULL".to_string()),
                    }
                }
                Ok(values.into())
            }
            DataType::Date64 => {
                let array = value.as_primitive::<Date64Type>();
                let mut values = Vec::new();
                for i in 0..array.len() {
                    let v = array.value_as_date(i);
                    match v {
                        Some(v) => values.push(v.to_string()),
                        None => values.push("NULL".to_string()),
                    }
                }
                Ok(values.into())
            }
            DataType::Time32(unit) => match unit {
                TimeUnit::Second => {
                    let array = value.as_primitive::<Time32SecondType>();
                    let mut values = Vec::new();
                    for i in 0..array.len() {
                        let v = array.value_as_time(i);
                        match v {
                            Some(v) => values.push(v.to_string()),
                            None => values.push("NULL".to_string()),
                        }
                    }
                    Ok(values.into())
                }
                TimeUnit::Millisecond => {
                    let array = value.as_primitive::<Time32MillisecondType>();
                    let mut values = Vec::new();
                    for i in 0..array.len() {
                        let v = array.value_as_time(i);
                        match v {
                            Some(v) => values.push(v.to_string()),
                            None => values.push("NULL".to_string()),
                        }
                    }
                    Ok(values.into())
                }
                _ => Err(format!("Unsupported time unit: {:?}", unit).into()),
            },
            DataType::Time64(unit) => match unit {
                TimeUnit::Microsecond => {
                    let array = value.as_primitive::<Time64MicrosecondType>();
                    let mut values = Vec::new();
                    for i in 0..array.len() {
                        let v = array.value_as_time(i);
                        match v {
                            Some(v) => values.push(v.to_string()),
                            None => values.push("NULL".to_string()),
                        }
                    }
                    Ok(values.into())
                }
                TimeUnit::Nanosecond => {
                    let array = value.as_primitive::<Time64NanosecondType>();
                    let mut values = Vec::new();
                    for i in 0..array.len() {
                        let v = array.value_as_time(i);
                        match v {
                            Some(v) => values.push(v.to_string()),
                            None => values.push("NULL".to_string()),
                        }
                    }
                    Ok(values.into())
                }
                _ => Err(format!("Unsupported time unit: {:?}", unit).into()),
            },
            DataType::Duration(unit) => match unit {
                TimeUnit::Second => {
                    let array = value.as_primitive::<DurationSecondType>();
                    let mut values = Vec::new();
                    for i in 0..array.len() {
                        let v = array.value_as_duration(i);
                        match v {
                            Some(v) => values.push(v.to_string()),
                            None => values.push("NULL".to_string()),
                        }
                    }
                    Ok(values.into())
                }
                TimeUnit::Millisecond => {
                    let array = value.as_primitive::<DurationMillisecondType>();
                    let mut values = Vec::new();
                    for i in 0..array.len() {
                        let v = array.value_as_duration(i);
                        match v {
                            Some(v) => values.push(v.to_string()),
                            None => values.push("NULL".to_string()),
                        }
                    }
                    Ok(values.into())
                }
                TimeUnit::Microsecond => {
                    let array = value.as_primitive::<DurationMicrosecondType>();
                    let mut values = Vec::new();
                    for i in 0..array.len() {
                        let v = array.value_as_duration(i);
                        match v {
                            Some(v) => values.push(v.to_string()),
                            None => values.push("NULL".to_string()),
                        }
                    }
                    Ok(values.into())
                }
                TimeUnit::Nanosecond => {
                    let array = value.as_primitive::<DurationNanosecondType>();
                    let mut values = Vec::new();
                    for i in 0..array.len() {
                        let v = array.value_as_duration(i);
                        match v {
                            Some(v) => values.push(v.to_string()),
                            None => values.push("NULL".to_string()),
                        }
                    }
                    Ok(values.into())
                }
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
                                let time = v.milliseconds;
                                format!("{} day {} millisecond", day, time)
                            }
                            None => "NULL".to_string(),
                        })
                        .collect())
                }
                _ => Err(format!("Unsupported interval unit: {:?}", unit).into()),
            },
            DataType::Binary => {
                let array = value.as_binary::<i32>();
                Ok(array
                    .iter()
                    .map(|v| match v {
                        Some(v) => String::from_utf8(v.to_vec())
                            .unwrap_or_else(|e| format!("Failed to convert to string: {:?}", e)),
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
                            .unwrap_or_else(|e| format!("Failed to convert to string: {:?}", e)),
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
                            .unwrap_or_else(|e| format!("Failed to convert to string: {:?}", e)),
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
                            .unwrap_or_else(|e| format!("Failed to convert to string: {:?}", e)),
                        None => "NULL".to_string(),
                    })
                    .collect())
            }
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
                    return Err(
                        format!("Failed to downcast to LargeStringArray: {:?}", value).into(),
                    );
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
                    return Err(
                        format!("Failed to downcast to StringViewArray: {:?}", value).into(),
                    );
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
