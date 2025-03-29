use datafusion::arrow::{
    array::{Array, Float64Array, Int64Array, MapArray, StringArray, StringViewArray, TimestampMillisecondArray},
    datatypes::{DataType, TimeUnit},
    record_batch::RecordBatch,
};
use magnus::{Error, RHash, Value};

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
            println!("Processing column '{}' with data type: {:?}", field.name(), column.data_type());
            println!("Array type: {:?}", column.as_ref());
            
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
                DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                    println!("Attempting to handle timestamp with timezone: {:?}", tz);
                    let array = column.as_any().downcast_ref::<TimestampMillisecondArray>()
                        .ok_or_else(|| DataFusionError::CommonError(format!(
                            "failed to downcast timestamp array: {} (array: {:?})",
                            column.data_type(),
                            column.as_ref()
                        )))?;
                    array.values().iter().map(|ts_millis| (*ts_millis).into()).collect()
                }
                DataType::Map(field, _) => {
                    println!("Processing Map type with field: {:?}", field);
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
                            let values = struct_array.column(1).as_any().downcast_ref::<StringArray>()
                                .ok_or_else(|| DataFusionError::CommonError("failed to get values array".to_string()))
                                .unwrap();
                            for i in 0..struct_array.len() {
                                let key = keys.value(i).to_string();
                                let value = values.value(i).to_string();
                                hash.aset(key, value).unwrap();
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
