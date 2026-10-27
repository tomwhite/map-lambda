//! Getting a request to a dispatcher and its body back.
//!
//! Behind a trait so that a test - or a Python fake - can answer with
//! frames it wrote itself.

use crate::Error;
use aws_config::retry::RetryConfig;
use aws_config::{BehaviorVersion, ConfigLoader};
use aws_credential_types::Credentials;
use aws_sdk_lambda::config::Region;
use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::types::{InvocationType, InvokeWithResponseStreamResponseEvent as Event};
use aws_sdk_lambda::Client;
use aws_smithy_types::timeout::TimeoutConfig;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use std::time::Duration;

/// A local `cargo lambda watch` server, started from rust/.
pub const LOCAL_ENDPOINT_URL: &str = "http://localhost:9000";

/// Lambda's maximum function duration: a dispatcher's stream can stay
/// open that long.
const DISPATCHER_READ_TIMEOUT: Duration = Duration::from_secs(900);

/// A dispatcher's response body, as it arrives.
pub type Chunks = BoxStream<'static, Result<Vec<u8>, Error>>;

/// Somewhere to send a dispatcher request.
pub trait Transport: Send + Sync {
    /// Invoke one dispatcher and hand back its response body.
    fn stream<'a>(
        &'a self,
        function: &'a str,
        payload: Vec<u8>,
    ) -> BoxFuture<'a, Result<Chunks, Error>>;

    /// Invoke without waiting for an answer.
    fn invoke<'a>(
        &'a self,
        function: &'a str,
        payload: Vec<u8>,
    ) -> BoxFuture<'a, Result<(), Error>>;
}

/// Real Lambda, or a local `cargo lambda watch` when given an endpoint.
#[derive(Debug, Clone)]
pub struct Lambda {
    client: Client,
    local: bool,
}

impl Lambda {
    /// Credentials and region come from the environment, unless
    /// `endpoint_url` is set: the emulator does not check them, but the
    /// SDK will not sign without them.
    pub async fn connect(endpoint_url: Option<&str>) -> Self {
        // Retries are the dispatcher's, where they are counted; one
        // underneath would be an invocation nothing counted.
        let loader: ConfigLoader =
            aws_config::defaults(BehaviorVersion::latest()).retry_config(RetryConfig::disabled());
        let timeouts = TimeoutConfig::builder().read_timeout(DISPATCHER_READ_TIMEOUT);
        let config = match endpoint_url {
            None => loader.timeout_config(timeouts.build()),
            Some(url) => loader
                .region(Region::new("us-east-1"))
                .credentials_provider(Credentials::new("test", "test", None, None, "local"))
                .endpoint_url(url)
                // A server that is not running fails fast rather than hanging.
                .timeout_config(timeouts.connect_timeout(Duration::from_secs(2)).build()),
        };
        Self {
            client: Client::new(&config.load().await),
            local: endpoint_url.is_some(),
        }
    }

    async fn streamed(&self, function: &str, payload: Vec<u8>) -> Result<Chunks, Error> {
        let output = self
            .client
            .invoke_with_response_stream()
            .function_name(function)
            .payload(Blob::new(payload))
            .send()
            .await
            .map_err(|e| Error::Transport(error_chain(&e)))?;

        let chunks = futures::stream::unfold(Some(output.event_stream), |state| async move {
            let mut events = state?;
            match events.recv().await {
                Ok(None) => None,
                Ok(Some(Event::PayloadChunk(update))) => {
                    let bytes = update.payload.map(Blob::into_inner).unwrap_or_default();
                    Some((Ok(bytes), Some(events)))
                }
                // The HTTP status was 200 before the function had done
                // anything, so a failure can only arrive here.
                Ok(Some(Event::InvokeComplete(complete))) => match complete.error_code {
                    Some(code) => {
                        let detail = complete.error_details.unwrap_or_default();
                        let failed =
                            Error::FailedMidStream(format!("{code} {detail}").trim().to_string());
                        Some((Err(failed), None))
                    }
                    None => Some((Ok(Vec::new()), Some(events))),
                },
                Ok(Some(_)) => Some((Ok(Vec::new()), Some(events))),
                Err(e) => Some((Err(Error::Transport(error_chain(&e))), None)),
            }
        });
        Ok(Box::pin(chunks))
    }

    /// The emulator leaves `:message-type` off the event-stream messages it
    /// frames a streamed response with, which botocore tolerates and the
    /// Rust SDK's decoder refuses. Its buffered endpoint serves the same
    /// frames as one body - so a local run gets them all at the end.
    async fn buffered(&self, function: &str, payload: Vec<u8>) -> Result<Chunks, Error> {
        let output = self
            .client
            .invoke()
            .function_name(function)
            .payload(Blob::new(payload))
            .send()
            .await
            .map_err(|e| Error::Transport(error_chain(&e)))?;
        if let Some(failed) = output.function_error {
            return Err(Error::DispatcherFailed(failed));
        }
        let body = output.payload.map(Blob::into_inner).unwrap_or_default();
        Ok(Box::pin(futures::stream::once(async move { Ok(body) })))
    }
}

impl Transport for Lambda {
    fn stream<'a>(
        &'a self,
        function: &'a str,
        payload: Vec<u8>,
    ) -> BoxFuture<'a, Result<Chunks, Error>> {
        if self.local {
            Box::pin(self.buffered(function, payload))
        } else {
            Box::pin(self.streamed(function, payload))
        }
    }

    fn invoke<'a>(
        &'a self,
        function: &'a str,
        payload: Vec<u8>,
    ) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            self.client
                .invoke()
                .function_name(function)
                .invocation_type(InvocationType::Event)
                .payload(Blob::new(payload))
                .send()
                .await
                .map_err(|e| Error::Transport(error_chain(&e)))?;
            Ok(())
        })
    }
}

/// Every message in an error's source chain, joined by ": ".
///
/// An SDK error's outermost message is `service error` or `dispatch
/// failure`; what went wrong is two or three sources down.
fn error_chain(error: &dyn std::error::Error) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(error) = source {
        message.push_str(": ");
        message.push_str(&error.to_string());
        source = error.source();
    }
    message
}
