//! A coordinator: dispatches one partition of a call's inputs from inside
//! Lambda.
//!
//! A coordinator is generic over workers, it is told a function name, a payload
//! template and a list of inputs by `map_lambda/coordinator.py`.
//!
//! - `protocol` - what goes over the wire, both directions.
//! - `clients` - the Lambda clients, and how many there have to be.
//! - `invoke` - one worker invocation, with retries.
//! - `tally` - what the partition has come to so far.
//! - `dispatch` - the loop that turns inputs into invocations.
//! - `handler` - response streaming, and the entry point.

use lambda_runtime::{run as lambda_run, service_fn, tracing, Error};

mod clients;
mod dispatch;
mod handler;
mod invoke;
mod protocol;
mod tally;

/// Serve the coordinator on the Lambda runtime, until the runtime stops it.
pub async fn run() -> Result<(), Error> {
    tracing::init_default_subscriber();

    lambda_run(service_fn(handler::function_handler)).await
}
