use core::fmt;

use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError as InnerDataFusionError;
use object_store::Error as ObjectStoreError;
use magnus::Error as MagnusError;

use crate::datafusion_error;

#[derive(Debug)]
pub enum DataFusionError {
    ExecutionError(InnerDataFusionError),
    ArrowError(ArrowError),
    CommonError(String),
    ObjectStoreError(ObjectStoreError),
}

impl fmt::Display for DataFusionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DataFusionError::ExecutionError(e) => write!(f, "Rust DataFusion error: {:?}", e),
            DataFusionError::ArrowError(e) => write!(f, "Rust Arrow error: {:?}", e),
            DataFusionError::CommonError(e) => write!(f, "Ruby DataFusion error: {:?}", e),
            DataFusionError::ObjectStoreError(e) => write!(f, "Object Store error: {:?}", e),
        }
    }
}

impl From<ArrowError> for DataFusionError {
    fn from(err: ArrowError) -> DataFusionError {
        DataFusionError::ArrowError(err)
    }
}

impl From<InnerDataFusionError> for DataFusionError {
    fn from(err: InnerDataFusionError) -> DataFusionError {
        DataFusionError::ExecutionError(err)
    }
}

impl From<ObjectStoreError> for DataFusionError {
    fn from(err: ObjectStoreError) -> DataFusionError {
        DataFusionError::ObjectStoreError(err)
    }
}

impl From<DataFusionError> for MagnusError {
    fn from(err: DataFusionError) -> MagnusError {
        MagnusError::new(datafusion_error(), err.to_string())
    }
}
