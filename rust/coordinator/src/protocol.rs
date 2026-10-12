//! The wire contract with `map_lambda/coordinator.py`: what a
//! coordinator is asked for, and the frames it answers with.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// What the client sends: a function to call, a payload to call it
/// with, and the work to spread across the calls.
#[derive(Deserialize)]
pub(crate) struct Request {
    /// Worker to invoke once per input, e.g. "echo".
    pub(crate) target_function: String,
    /// The worker's payload with everything constant across the partition
    /// filled in.
    pub(crate) template: Map<String, Value>,
    /// The work, one entry per invocation, each whatever the worker
    /// wants under `input_key`.
    pub(crate) inputs: Vec<Value>,
    /// Which key an input goes under in the worker's payload.
    pub(crate) input_key: String,
    /// How many invocations may be in flight from this coordinator. The
    /// run's total in flight is the sum across coordinators.
    pub(crate) concurrency: usize,
    /// Where to send the worker invocations, for a local run against
    /// `cargo lambda watch`. Absent means real Lambda.
    #[serde(default)]
    pub(crate) endpoint_url: Option<String>,
    /// Extra attempts after a *transport* failure, per invocation.
    #[serde(default = "default_retries")]
    pub(crate) retries: usize,
    /// Whether to put one record per invocation on the frames.
    #[serde(default = "default_true")]
    pub(crate) return_stats: bool,
    /// Whether to send the workers' answers back at all.
    #[serde(default = "default_true")]
    pub(crate) return_outputs: bool,
}

fn default_retries() -> usize {
    2
}

fn default_true() -> bool {
    true
}

/// Statistics for a single worker invocation.
#[derive(Serialize)]
pub(crate) struct Stat {
    /// Where this invocation's input sat in the partition, counting from 0.
    pub(crate) index: usize,
    /// The worker invocation's AWS request ID
    pub(crate) req_id: String,
    /// Epoch seconds, to match Python's `time.time()`.
    pub(crate) invoke_start: f64,
    pub(crate) invoke_end: f64,
}

/// One worker's answer.
///
/// `response` is the worker's whole body, verbatim. Never null: a worker
/// that answered nothing gets no `Output` at all.
#[derive(Serialize)]
pub(crate) struct Output {
    /// Where the input this answers sat in the partition, as `Stat::index`.
    pub(crate) index: usize,
    pub(crate) response: Value,
}

/// What finished since the last frame.
#[derive(Serialize)]
pub(crate) struct Completed {
    /// Invocations that came back successfully.
    pub(crate) done: usize,
    /// Empty unless the request asked for outputs - and a worker that
    /// answered nothing has no entry here either way.
    pub(crate) outputs: Vec<Output>,
    /// Empty unless the request asked for stats.
    pub(crate) stats: Vec<Stat>,
    /// The first invocation of this frame to fail every attempt, as its
    /// own message.
    pub(crate) first_error: Option<String>,
}

impl Completed {
    /// Nothing happened, so there is nothing to send.
    pub(crate) fn is_empty(&self) -> bool {
        self.done == 0
            && self.outputs.is_empty()
            && self.stats.is_empty()
            && self.first_error.is_none()
    }
}

#[derive(Serialize)]
pub(crate) struct Response {
    /// This coordinator's own AWS request id, so its run can be found in
    /// CloudWatch alongside the invocations it dispatched.
    pub(crate) req_id: String,
    pub(crate) dispatched: usize,
    pub(crate) succeeded: usize,
    /// Lambda ran the worker and it failed: the error channel
    /// (`X-Amz-Function-Error`), never anything in a body.
    pub(crate) function_errors: usize,
    /// The request never got a usable response: connection, timeout, or
    /// an error status.
    pub(crate) transport_errors: usize,
    /// Attempts beyond the first, across all invocations.
    pub(crate) retried: usize,
    /// Summed wait before those attempts.
    pub(crate) retry_wait_ms: f64,
    /// The first failed invocation's message.
    pub(crate) first_error: Option<String>,
    /// Whether this invocation had to build the Lambda clients, i.e.
    /// whether the execution environment was cold. Null for a local
    /// run, which has no shared set to build.
    pub(crate) cold: Option<bool>,
    /// HTTP/2 connections the invocations were spread over.
    pub(crate) connections: usize,
    pub(crate) elapsed_ms: f64,
}

/// What goes over the wire, one JSON object per line: a completed frame
/// per reporting tick, followed by exactly one result.
#[derive(Serialize)]
pub(crate) enum Frame {
    Completed(Completed),
    Result(Box<Response>),
}
