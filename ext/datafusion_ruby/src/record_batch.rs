use datafusion::arrow::{
    array::{Float64Array, Int64Array, StringArray},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use magnus::{Error, Value};

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
            println!("Column '{}' has type: {:?}", field.name(), column.data_type());
            println!("Column '{}' has array type: {:?}", field.name(), column.as_ref());
            columns_by_name.insert(
                field.name().clone(),
                match column.data_type() {
                    DataType::Int64 => {
                        let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
                        array.values().iter().map(|v| (*v as i64).into()).collect()
                    }
                    DataType::Float64 => {
                        let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
                        array.values().iter().map(|v| (*v as f64).into()).collect()
                    }
                    DataType::Utf8 | DataType::LargeUtf8 => {
                        if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
                            array.iter().map(|opt_v| opt_v.unwrap_or("").to_string().into()).collect()
                        } else {
                            return Err(DataFusionError::CommonError(format!(
                                "unhandled string array type: {} (array: {:?})",
                                column.data_type(),
                                column.as_ref()
                            ))
                            .into());
                        }
                    }
                    unknown => {
                        return Err(DataFusionError::CommonError(format!(
                            "unhandled data type: {} (array: {:?})",
                            unknown,
                            column.as_ref()
                        ))
                        .into())
                    }
                },
            );
        }
        Ok(columns_by_name)
    }
}
