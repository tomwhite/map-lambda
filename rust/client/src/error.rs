//! What can go wrong between asking for a run and getting its answers.

use map_lambda_protocol::MAX_REQUEST_BYTES;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("a dispatcher run needs at least one dispatcher")]
    NoDispatchers,

    #[error("concurrency {concurrency} is too small to split {dispatchers} ways")]
    ConcurrencyTooSmall {
        concurrency: usize,
        dispatchers: usize,
    },

    #[error(
        "concurrency {concurrency} over {dispatchers} dispatcher(s) is {widest} in flight \
         each, more than the {carries} {holder} can carry: it would queue the rest and the run \
         would just be slow. Use at least {need} dispatchers, or lower concurrency"
    )]
    TooMuchInFlight {
        concurrency: usize,
        dispatchers: usize,
        widest: usize,
        carries: usize,
        holder: &'static str,
        need: usize,
    },

    #[error(
        "partition {partition} is {:.1} MiB, over Lambda's {} MiB request limit: it would be \
         refused as RequestTooLargeException with nothing run. Use at least {need} dispatchers, \
         or send smaller inputs",
        *bytes as f64 / 1024.0 / 1024.0,
        MAX_REQUEST_BYTES / 1024 / 1024
    )]
    RequestTooLarge {
        partition: usize,
        bytes: usize,
        need: usize,
    },

    #[error("an invocation failed its last attempt, so the call cannot succeed: {0}")]
    InvocationFailed(String),

    #[error(
        "dispatcher {req_id} reported {} failed invocation(s) ({function_errors} function, \
         {transport_errors} transport): {}",
        function_errors + transport_errors,
        first_error.as_deref().unwrap_or("None")
    )]
    InvocationsFailed {
        req_id: String,
        function_errors: usize,
        transport_errors: usize,
        first_error: Option<String>,
    },

    #[error(
        "dispatcher {req_id} completed {succeeded} of {expected} invocation(s) in its partition \
         with no error reported"
    )]
    PartitionShort {
        req_id: String,
        succeeded: usize,
        expected: usize,
    },

    #[error(
        "dispatcher stream ended with no result frame - it stopped partway through its \
         partition, so some of its work was never done"
    )]
    NoResult,

    #[error("dispatcher invocation failed: {0}")]
    DispatcherFailed(String),

    #[error("dispatcher failed mid-stream: {0}")]
    FailedMidStream(String),

    #[error("could not reach Lambda: {0}")]
    Transport(String),

    #[error("{context}: {source}")]
    Json {
        context: &'static str,
        #[source]
        source: serde_json::Error,
    },
}

impl Error {
    pub(crate) fn json(context: &'static str) -> impl FnOnce(serde_json::Error) -> Self {
        move |source| Self::Json { context, source }
    }

    /// Whether the run was refused before anything was invoked, because
    /// of what it was asked to do rather than anything that happened.
    pub fn is_refusal(&self) -> bool {
        matches!(
            self,
            Self::NoDispatchers
                | Self::ConcurrencyTooSmall { .. }
                | Self::TooMuchInFlight { .. }
                | Self::RequestTooLarge { .. }
        )
    }
}
