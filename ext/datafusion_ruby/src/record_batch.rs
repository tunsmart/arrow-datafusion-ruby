use datafusion::arrow::{
    array::{Array, Float64Array, Int64Array, MapArray, StringArray, StringViewArray, TimestampMillisecondArray, TimestampNanosecondArray},
    datatypes::{DataType, TimeUnit},
    record_batch::RecordBatch,
};
use magnus::{Error, RHash, Value};
use chrono::{DateTime, TimeZone, Utc};

use crate::errors::DataFusionError;
use std::collections::HashMap;

#[magnus::wrap(class = "Datafusion::RecordBatch")]
pub(crate) struct RbRecordBatch {
    rb: RecordBatch,
}

impl RbRecordBatch {
    pub(crate) fn new(rb: RecordBatch) -> Self {
        Self { rb }
    }

    pub(crate) fn to_hash(&self) -> Result<HashMap<String, Vec<Value>>, Error> {
        let mut columns_by_name: HashMap<String, Vec<Value>> = HashMap::new();
        for (i, field) in self.rb.schema().fields().iter().enumerate() {
            let column = self.rb.column(i);
            let result = match column.data_type() {
                DataType::Int64 => {
                    let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
                    array.values().iter().map(|v| (*v as i64).into()).collect()
                }
                DataType::Float64 => {
                    let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
                    array.values().iter().map(|v| (*v as f64).into()).collect()
                }
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                    if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
                        array.iter().map(|opt_v| opt_v.unwrap_or("").to_string().into()).collect()
                    } else if let Some(array) = column.as_any().downcast_ref::<StringViewArray>() {
                        array.iter().map(|opt_v| opt_v.unwrap_or("").to_string().into()).collect()
                    } else {
                        return Err(DataFusionError::CommonError(format!(
                            "unhandled string array type: {} (array: {:?})",
                            column.data_type(),
                            column.as_ref()
                        ))
                        .into())
                    }
                }
                DataType::Timestamp(unit, _tz) => {
                    match unit {
                        TimeUnit::Millisecond => {
                            let array = column.as_any().downcast_ref::<TimestampMillisecondArray>()
                                .ok_or_else(|| DataFusionError::CommonError(format!(
                                    "failed to downcast timestamp array: {} (array: {:?})",
                                    column.data_type(),
                                    column.as_ref()
                                )))?;
                            
                            array.values().iter().map(|ts_millis| {
                                let secs = ts_millis / 1000;
                                let nanos = ((ts_millis % 1000) * 1_000_000) as u32;
                                let dt: DateTime<Utc> = Utc.timestamp_opt(secs, nanos).unwrap();
                                dt.format("%Y-%m-%dT%H:%M:%S").to_string().into()
                            }).collect()
                        }
                        TimeUnit::Nanosecond => {
                            let array = column.as_any().downcast_ref::<TimestampNanosecondArray>()
                                .ok_or_else(|| DataFusionError::CommonError(format!(
                                    "failed to downcast timestamp array: {} (array: {:?})",
                                    column.data_type(),
                                    column.as_ref()
                                )))?;
                            
                            array.values().iter().map(|ts_nanos| {
                                let secs = ts_nanos / 1_000_000_000;
                                let nanos = (ts_nanos % 1_000_000_000) as u32;
                                let dt: DateTime<Utc> = Utc.timestamp_opt(secs, nanos).unwrap();
                                dt.format("%Y-%m-%dT%H:%M:%S").to_string().into()
                            }).collect()
                        }
                        _ => {
                            return Err(DataFusionError::CommonError(format!(
                                "unsupported timestamp unit: {:?} (array: {:?})",
                                unit,
                                column.as_ref()
                            ))
                            .into())
                        }
                    }
                }
                DataType::Map(field, _) => {
                    let array = column.as_any().downcast_ref::<MapArray>()
                        .ok_or_else(|| DataFusionError::CommonError(format!(
                            "failed to downcast map array: {} (array: {:?})",
                            column.data_type(),
                            column.as_ref()
                        )))?;
                    array.iter().map(|opt_struct| {
                        if let Some(struct_array) = opt_struct {
                            let hash = RHash::new();
                            let keys = struct_array.column(0).as_any().downcast_ref::<StringArray>()
                                .ok_or_else(|| DataFusionError::CommonError("failed to get keys array".to_string()))
                                .unwrap();
                            
                            if let DataType::Struct(fields) = field.data_type() {
                                let value_type = &fields[1].data_type();
                                match value_type {
                                    DataType::Utf8 => {
                                        let values = struct_array.column(1).as_any().downcast_ref::<StringArray>()
                                            .ok_or_else(|| DataFusionError::CommonError("failed to get string values array".to_string()))
                                            .unwrap();
                                        for i in 0..struct_array.len() {
                                            let key = keys.value(i).to_string();
                                            let value = values.value(i).to_string();
                                            hash.aset(key, value).unwrap();
                                        }
                                    }
                                    DataType::Float64 => {
                                        let values = struct_array.column(1).as_any().downcast_ref::<Float64Array>()
                                            .ok_or_else(|| DataFusionError::CommonError("failed to get float values array".to_string()))
                                            .unwrap();
                                        for i in 0..struct_array.len() {
                                            let key = keys.value(i).to_string();
                                            let value = values.value(i);
                                            hash.aset(key, value).unwrap();
                                        }
                                    }
                                    other => {
                                        return Err(DataFusionError::CommonError(format!(
                                            "unsupported value type in map: {:?}",
                                            other
                                        )))
                                        .unwrap();
                                    }
                                }
                            } else {
                                return Err(DataFusionError::CommonError(format!(
                                    "invalid map field type: expected struct, got {:?}",
                                    field.data_type()
                                )))
                                .unwrap();
                            }
                            hash.into()
                        } else {
                            RHash::new().into()
                        }
                    }).collect()
                }
                unknown => {
                    return Err(DataFusionError::CommonError(format!(
                        "unhandled data type: {} (array: {:?})",
                        unknown,
                        column.as_ref()
                    ))
                    .into())
                }
            };
            columns_by_name.insert(field.name().clone(), result);
        }
        Ok(columns_by_name)
    }
}
