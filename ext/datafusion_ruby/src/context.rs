use datafusion::execution::context::SessionContext;
use datafusion::prelude::*;
use magnus::Error;
use object_store::aws::AmazonS3Builder;
use std::env;
use std::sync::Arc;
use url::Url;

use crate::dataframe::RbDataFrame;
use crate::errors::DataFusionError;
use crate::utils::wait_for_future;

/// The following environment variables must be defined:
///
/// - AWS_ACCESS_KEY_ID
/// - AWS_SECRET_ACCESS_KEY
/// - AWS_REGION

#[magnus::wrap(class = "Datafusion::SessionContext")]
pub(crate) struct RbSessionContext {
    ctx: SessionContext,
}

impl RbSessionContext {
    pub(crate) fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
        }
    }

    pub(crate) fn register_csv(&self, name: String, table_path: String) -> Result<(), Error> {
        let result =
            self.ctx
                .register_csv(&name, &table_path, CsvReadOptions::new());
        wait_for_future(result).map_err(DataFusionError::from)?;
        Ok(())
    }

    pub(crate) fn sql(&self, query: String) -> Result<RbDataFrame, Error> {
        let result = self.ctx.sql(&query);
        let df = wait_for_future(result).map_err(DataFusionError::from)?;
        Ok(RbDataFrame::new(Arc::new(df)))
    }

    pub(crate) fn register_object_store(&self, bucket_name: String) -> Result<(), Error> {
        let s3 = AmazonS3Builder::new()
            .with_bucket_name(&bucket_name)
            .with_region(env::var("AWS_REGION").unwrap())
            .with_access_key_id(env::var("AWS_ACCESS_KEY_ID").unwrap())
            .with_secret_access_key(env::var("AWS_SECRET_ACCESS_KEY").unwrap())
            .build()
            .map_err(DataFusionError::from)?;
        let path = format!("s3://{bucket_name}");
        let s3_url = Url::parse(&path).unwrap();
        self.ctx.register_object_store(&s3_url, Arc::new(s3));
        Ok(())
    }
}
