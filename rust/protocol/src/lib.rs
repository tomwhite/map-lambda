//! The wire contract between a client and a dispatcher: what a
//! dispatcher is asked for, the frames it answers with, and the limits
//! both ends are bound by.
//!
//! Every type derives both directions, because both ends depend on this
//! crate. Callers use the client's re-exports, not this.

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_json::{Map, Value};

/// Lambda's SETTINGS_MAX_CONCURRENT_STREAMS: the hard limit, past which
/// requests on one HTTP/2 connection queue.
pub const MAX_STREAMS_PER_CONNECTION: usize = 128;

/// How many invocations a dispatcher puts through one HTTP/2 connection:
/// `MAX_STREAMS_PER_CONNECTION` less headroom.
pub const STREAMS_PER_CONNECTION: usize = 100;

/// The most invocations one dispatcher will carry at once.
pub const MAX_IN_FLIGHT: usize = 1_600;

/// What a dispatcher against `cargo lambda watch` carries: it builds one
/// throwaway client, so one connection's worth.
pub const MAX_IN_FLIGHT_LOCAL: usize = STREAMS_PER_CONNECTION;

/// Clients a dispatcher builds at cold start, enough to cover
/// `MAX_IN_FLIGHT`.
pub const MAX_HTTP_CLIENTS: usize = MAX_IN_FLIGHT.div_ceil(STREAMS_PER_CONNECTION);

/// Lambda's hard cap on a synchronous request payload, which the whole
/// `Request` has to fit: inputs, one copy of the template, and framing.
pub const MAX_REQUEST_BYTES: usize = 6 * 1024 * 1024;

/// How many connections `concurrency` invocations need.
pub fn connections_for(concurrency: usize) -> usize {
    concurrency
        .div_ceil(STREAMS_PER_CONNECTION)
        .clamp(1, MAX_HTTP_CLIENTS)
}

/// What the client sends: a function to call, a payload to call it
/// with, and the work to spread across the calls.
#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
    /// Worker to invoke once per input, e.g. "echo".
    pub target_function: String,
    /// The worker's payload with everything constant across the partition
    /// filled in.
    pub template: Map<String, Value>,
    /// The work, one entry per invocation, each whatever the worker
    /// wants under `input_key`.
    pub inputs: Vec<Value>,
    /// Which key an input goes under in the worker's payload.
    pub input_key: String,
    /// How many invocations may be in flight from this dispatcher. The
    /// run's total in flight is the sum across dispatchers.
    pub concurrency: usize,
    /// Where to send the worker invocations, for a local run against
    /// `cargo lambda watch`. `None` means real Lambda.
    pub endpoint_url: Option<String>,
    /// Extra attempts after a *transport* failure, per invocation.
    pub retries: usize,
    /// Whether to put one record per invocation on the frames.
    pub return_stats: bool,
    /// Whether to send the workers' answers back at all.
    pub return_outputs: bool,
}

/// Statistics for a single worker invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stat {
    /// Where this invocation's input sat in the partition, counting from 0.
    pub index: usize,
    /// The worker invocation's AWS request ID
    pub req_id: String,
    /// Epoch seconds, to match Python's `time.time()`.
    pub invoke_start: f64,
    pub invoke_end: f64,
}

/// One worker's answer.
///
/// `response` is the worker's whole body, verbatim. Never null: a worker
/// that answered nothing gets no `Output` at all.
#[derive(Debug, Serialize, Deserialize)]
pub struct Output {
    /// Where the input this answers sat in the partition, as `Stat::index`.
    pub index: usize,
    /// As the worker wrote it: checked to be JSON, but never parsed into
    /// a tree, which takes several times the memory of the text.
    pub response: Box<RawValue>,
}

/// What finished since the last frame.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Completed {
    /// Invocations that came back successfully.
    pub done: usize,
    /// Empty unless the request asked for outputs - and a worker that
    /// answered nothing has no entry here either way.
    pub outputs: Vec<Output>,
    /// Empty unless the request asked for stats.
    pub stats: Vec<Stat>,
    /// The first invocation of this frame to fail every attempt, as its
    /// own message.
    pub first_error: Option<String>,
}

impl Completed {
    /// Nothing happened, so there is nothing to send.
    pub fn is_empty(&self) -> bool {
        self.done == 0
            && self.outputs.is_empty()
            && self.stats.is_empty()
            && self.first_error.is_none()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    /// This dispatcher's own AWS request id, so its run can be found in
    /// CloudWatch alongside the invocations it dispatched.
    pub req_id: String,
    pub dispatched: usize,
    pub succeeded: usize,
    /// Lambda ran the worker and it failed: the error channel
    /// (`X-Amz-Function-Error`), never anything in a body.
    pub function_errors: usize,
    /// The request never got a usable response: connection, timeout, or
    /// an error status.
    pub transport_errors: usize,
    /// Attempts beyond the first, across all invocations.
    pub retried: usize,
    /// Summed wait before those attempts.
    pub retry_wait_ms: f64,
    /// The first failed invocation's message.
    pub first_error: Option<String>,
    /// Whether this invocation had to build the Lambda clients, i.e.
    /// whether the execution environment was cold. Null for a local
    /// run, which has no shared set to build.
    pub cold: Option<bool>,
    /// HTTP/2 connections the invocations were spread over.
    pub connections: usize,
    pub elapsed_ms: f64,
}

/// What goes over the wire, one JSON object per line: a completed frame
/// per reporting tick, followed by exactly one result.
#[derive(Debug, Serialize, Deserialize)]
pub enum Frame {
    Completed(Completed),
    Result(Box<Response>),
}

#[cfg(test)]
mod tests {
    use super::{
        connections_for, Completed, Frame, Output, Response, Stat, MAX_HTTP_CLIENTS, MAX_IN_FLIGHT,
        STREAMS_PER_CONNECTION,
    };
    use serde_json::json;

    fn a_response() -> Response {
        Response {
            req_id: "coord-0".to_string(),
            dispatched: 3,
            succeeded: 3,
            function_errors: 0,
            transport_errors: 0,
            retried: 1,
            retry_wait_ms: 2.5,
            first_error: None,
            cold: None,
            connections: 1,
            elapsed_ms: 1.0,
        }
    }

    /// The two ends derive opposite halves of each type, so a field
    /// only one of them names would be dropped rather than caught.
    #[test]
    fn every_frame_a_dispatcher_writes_reads_back_as_the_same_frame() {
        for frame in [
            Frame::Completed(Completed::default()),
            Frame::Completed(Completed {
                done: 2,
                outputs: vec![Output {
                    index: 4,
                    response: serde_json::value::to_raw_value(&json!({"copied": 1}))
                        .expect("serializes"),
                }],
                stats: vec![Stat {
                    index: 4,
                    req_id: "w-1".to_string(),
                    invoke_start: 1.0,
                    invoke_end: 2.0,
                }],
                first_error: Some("no".to_string()),
            }),
            Frame::Result(Box::new(a_response())),
        ] {
            let line = serde_json::to_string(&frame).expect("serializes");
            assert!(!line.contains('\n'), "a frame is one line");
            let read: Frame = serde_json::from_str(&line).expect("round-trips");
            assert_eq!(serde_json::to_string(&read).expect("serializes"), line);
        }
    }

    #[test]
    fn frames_are_tagged_by_kind() {
        let value = serde_json::to_value(Frame::Completed(Completed::default())).expect("ok");
        assert_eq!(
            value,
            json!({"Completed": {"done": 0, "outputs": [], "stats": [], "first_error": null}})
        );
    }

    #[test]
    fn connections_cover_the_concurrency_asked_for() {
        // Too few connections does not error; the run is just slower.
        for concurrency in [1usize, 99, 100, 101, 128, 900, MAX_IN_FLIGHT] {
            let n = connections_for(concurrency);
            assert!(
                n * STREAMS_PER_CONNECTION >= concurrency,
                "{concurrency} in flight over {n} connection(s)"
            );
            assert!((1..=MAX_HTTP_CLIENTS).contains(&n));
        }
    }

    #[test]
    fn the_clients_built_cover_the_in_flight_limit() {
        // `div_ceil` guarantees the arithmetic; this is the clamp.
        assert_eq!(connections_for(MAX_IN_FLIGHT), MAX_HTTP_CLIENTS);
    }

    #[test]
    fn one_connection_is_enough_below_the_stream_limit() {
        assert_eq!(connections_for(1), 1);
        assert_eq!(connections_for(STREAMS_PER_CONNECTION), 1);
        assert_eq!(connections_for(STREAMS_PER_CONNECTION + 1), 2);
    }
}
