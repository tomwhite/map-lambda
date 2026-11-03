//! A dispatcher: fans one partition of a call's inputs out to workers from
//! inside Lambda.
//!
//! It is deployed as a binary around [`run`], named `dispatcher` by
//! default:
//!
//! ```no_run
//! #[tokio::main]
//! async fn main() -> Result<(), lambda_runtime::Error> {
//!     map_lambda_dispatcher::run().await
//! }
//! ```
#![warn(missing_docs)]

// A request flows `main` -> `run` -> `handler` -> `dispatch` -> `invoke`:
//
// - `handler` - response streaming, and the entry point.
// - `lambda_clients` - the Lambda clients, which `handler` gets.
// - `dispatch` - the loop that turns inputs into invocations.
// - `invoke` - one worker invocation, with retries.
// - `tally` - what the partition has come to so far, kept by `dispatch`.

use lambda_runtime::{run as lambda_run, service_fn, tracing, Error};
use map_lambda_protocol as protocol;

mod dispatch;
mod handler;
mod invoke;
mod lambda_clients;
mod tally;

/// Serve the dispatcher on the Lambda runtime, until the runtime stops it.
pub async fn run() -> Result<(), Error> {
    tracing::init_default_subscriber();

    lambda_run(service_fn(handler::function_handler)).await
}
