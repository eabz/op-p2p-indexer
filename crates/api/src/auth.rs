//! API keys (D18 in `docs/serving.md`): a request is served only with one of the configured
//! keys, sent as gRPC metadata `authorization: Bearer <key>`. Covers the stream and Flight
//! services alike. Keys are never logged.

use std::sync::Arc;

use subtle::ConstantTimeEq;
use tonic::metadata::MetadataMap;
use tonic::{Request, Status};

/// Checks requests against a list of keys; with none, every request passes. Use
/// [`ApiKeys::check`] as a tonic interceptor.
#[derive(Clone)]
pub struct ApiKeys(Arc<[String]>);

impl std::fmt::Debug for ApiKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The keys themselves never appear in a log.
        f.debug_struct("ApiKeys")
            .field("keys", &self.0.len())
            .finish()
    }
}

impl ApiKeys {
    /// The keys a request may carry; empty ones are ignored, and with none every request
    /// passes.
    #[must_use]
    pub fn new(keys: &[String]) -> Self {
        Self(keys.iter().filter(|key| !key.is_empty()).cloned().collect())
    }

    /// The interceptor: `UNAUTHENTICATED` without a configured key.
    ///
    /// # Errors
    ///
    /// Returns `UNAUTHENTICATED` if keys are configured and the request carries none of them.
    pub fn check(&self, request: Request<()>) -> Result<Request<()>, Status> {
        self.verify(request.metadata())?;
        Ok(request)
    }

    /// Checks the metadata of a request, for a service whose calls take different keys.
    ///
    /// # Errors
    ///
    /// As [`Self::check`].
    pub fn verify(&self, metadata: &MetadataMap) -> Result<(), Status> {
        if self.0.is_empty() {
            return Ok(());
        }
        let given = metadata
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "));
        match given {
            Some(given)
                if self
                    .0
                    .iter()
                    .any(|key| bool::from(key.as_bytes().ct_eq(given.as_bytes()))) =>
            {
                Ok(())
            }
            _ => Err(Status::unauthenticated("a valid API key is required")),
        }
    }
}
