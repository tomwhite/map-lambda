//! A dispatcher: fans one partition of a call's inputs out to workers from
//! inside Lambda.
//!
//! A dispatcher is generic over workers, it is told a function name, a payload
//! template and a list of inputs by the client.
//!
//! - `clients` - the Lambda clients, and how many there have to be.
//! - `invoke` - one worker invocation, with retries.
//! - `tally` - what the partition has come to so far.
//! - `dispatch` - the loop that turns inputs into invocations.
//! - `handler` - response streaming, and the entry point.

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
