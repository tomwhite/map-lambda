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

// - `clients` - the Lambda clients, and how many there have to be.
// - `invoke` - one worker invocation, with retries.
// - `tally` - what the partition has come to so far.
// - `dispatch` - the loop that turns inputs into invocations.
// - `handler` - response streaming, and the entry point.

use lambda_runtime::{run as lambda_run, service_fn, tracing, Error};
use map_lambda_protocol as protocol;

mod clients;
mod dispatch;
mod handler;
mod invoke;
mod tally;

/// Serve the dispatcher on the Lambda runtime, until the runtime stops it.
pub async fn run() -> Result<(), Error> {
    tracing::init_default_subscriber();

    lambda_run(service_fn(handler::function_handler)).await
}
