use datafusion::execution::context::SessionContext;
use datafusion::execution::config::SessionConfig;
use datafusion::prelude::*;
use magnus::Error;
use object_store::aws::AmazonS3Builder;
use std::env;
use std::sync::Arc;
use url::Url;

use crate::dataframe::RbDataFrame;
use crate::errors::DataFusionError;
use crate::utils::wait_for_future;

#[magnus::wrap(class = "Datafusion::SessionContext")]
pub(crate) struct RbSessionContext {
    ctx: SessionContext,
}

impl RbSessionContext {
    pub(crate) fn new() -> Self {
        let config = SessionConfig::new().with_information_schema(true);
        Self {
            ctx: SessionContext::new_with_config(config),
        }
    }

    pub(crate) fn register_csv(&self, name: String, table_path: String) -> Result<(), Error> {
        let result =
            self.ctx
                .register_csv(&name, &table_path, CsvReadOptions::new());
        wait_for_future(result).map_err(DataFusionError::from)?;
        Ok(())
    }

    pub(crate) fn create_table(&self, query: String) -> Result<(), Error> {
        let result = self.ctx.sql(&query);
        wait_for_future(result).map_err(DataFusionError::from)?;
        Ok(())
    }

    pub(crate) fn sql(&self, query: String) -> Result<RbDataFrame, Error> {
        let options = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);
        let result = self.ctx.sql_with_options(&query, options);
        let df = wait_for_future(result).map_err(DataFusionError::from)?;
        Ok(RbDataFrame::new(Arc::new(df)))
    }

    pub(crate) fn register_object_store(
        &self,
        bucket_name: String,
        region: Option<String>,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
    ) -> Result<(), Error> {
        let region = region.or_else(|| env::var("AWS_REGION").ok())
            .ok_or_else(|| DataFusionError::CommonError("AWS region not provided and AWS_REGION environment variable not set".to_string()))?;
            
        let access_key_id = access_key_id.or_else(|| env::var("AWS_ACCESS_KEY_ID").ok())
            .ok_or_else(|| DataFusionError::CommonError("AWS access key ID not provided and AWS_ACCESS_KEY_ID environment variable not set".to_string()))?;
            
        let secret_access_key = secret_access_key.or_else(|| env::var("AWS_SECRET_ACCESS_KEY").ok())
            .ok_or_else(|| DataFusionError::CommonError("AWS secret access key not provided and AWS_SECRET_ACCESS_KEY environment variable not set".to_string()))?;

        let s3 = AmazonS3Builder::new()
            .with_bucket_name(&bucket_name)
            .with_region(region)
            .with_access_key_id(access_key_id)
            .with_secret_access_key(secret_access_key)
            .build()
            .map_err(DataFusionError::from)?;

        let path = format!("s3://{bucket_name}");
        let s3_url = Url::parse(&path).unwrap();
        self.ctx.register_object_store(&s3_url, Arc::new(s3));
        Ok(())
    }
}
