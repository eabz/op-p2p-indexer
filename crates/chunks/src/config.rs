//! Shared R2 settings at application boundaries; credentials are never included in errors.

use object_store::aws::AmazonS3Builder;
use op_indexer_chainspec::ChainSpec;

use crate::R2Config;

/// A required R2 setting was absent or empty.
#[derive(Debug, thiserror::Error)]
#[error("{name} is required")]
pub struct R2ConfigError {
    name: &'static str,
}

impl R2Config {
    /// An S3 client of the bucket with the key: R2's endpoint for the account, or the one set.
    pub(crate) fn s3(&self) -> AmazonS3Builder {
        let endpoint = self
            .endpoint
            .clone()
            .unwrap_or_else(|| format!("https://{}.r2.cloudflarestorage.com", self.account_id));
        AmazonS3Builder::new()
            .with_endpoint(endpoint)
            // R2 takes any region; "auto" is its documented one.
            .with_region("auto")
            .with_bucket_name(&self.bucket)
            .with_access_key_id(&self.access_key_id)
            .with_secret_access_key(&self.secret_access_key)
    }

    /// Reads the shared `OP_INDEXER_R2_*` settings from the process environment.
    ///
    /// # Errors
    /// Returns an error naming a missing account id, access key id or secret key.
    pub fn from_env(chain: &ChainSpec) -> Result<Self, R2ConfigError> {
        Self::from_lookup(chain, |name| std::env::var(name).ok())
    }

    /// Reads R2 settings through a lookup, allowing command-line overrides at the binary edge.
    /// Empty values are treated as absent; the bucket defaults to `<chain>-snapshot` and
    /// the prefix to `archive`.
    ///
    /// # Errors
    /// Returns an error naming a missing account id, access key id or secret key.
    pub fn from_lookup(
        chain: &ChainSpec,
        mut lookup: impl FnMut(&str) -> Option<String>,
    ) -> Result<Self, R2ConfigError> {
        let mut lookup = |name| lookup(name).filter(|value| !value.is_empty());
        Ok(Self {
            account_id: lookup("OP_INDEXER_R2_ACCOUNT_ID").ok_or(R2ConfigError {
                name: "OP_INDEXER_R2_ACCOUNT_ID",
            })?,
            bucket: lookup("OP_INDEXER_R2_BUCKET")
                .unwrap_or_else(|| format!("{}-snapshot", chain.name)),
            prefix: lookup("OP_INDEXER_R2_PREFIX").unwrap_or_else(|| "archive".to_owned()),
            access_key_id: lookup("OP_INDEXER_R2_ACCESS_KEY_ID").ok_or(R2ConfigError {
                name: "OP_INDEXER_R2_ACCESS_KEY_ID",
            })?,
            secret_access_key: lookup("OP_INDEXER_R2_SECRET_ACCESS_KEY").ok_or(R2ConfigError {
                name: "OP_INDEXER_R2_SECRET_ACCESS_KEY",
            })?,
            endpoint: lookup("OP_INDEXER_R2_ENDPOINT"),
            public_url: lookup("OP_INDEXER_R2_PUBLIC_URL"),
        })
    }
}
