//! The wire contract between a client and a dispatcher: what a
//! dispatcher is asked for, the frames it responds with, and the limits
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
pub const MAX_LAMBDA_CLIENTS: usize = MAX_IN_FLIGHT.div_ceil(STREAMS_PER_CONNECTION);

/// Lambda's hard cap on a synchronous request payload, which the whole
/// `Request` has to fit: inputs, one copy of `constant_args`, and framing.
pub const MAX_REQUEST_BYTES: usize = 6 * 1024 * 1024;

/// How many connections `max_in_flight` invocations need.
pub fn connections_for(max_in_flight: usize) -> usize {
    max_in_flight
        .div_ceil(STREAMS_PER_CONNECTION)
        .clamp(1, MAX_LAMBDA_CLIENTS)
}

/// What the client sends: a function to call, a payload to call it
/// with, and the work to spread across the calls.
#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
    /// Worker to invoke once per input, e.g. "echo".
    pub worker_function: String,
    /// The worker's payload with everything constant across the partition
    /// filled in.
    pub constant_args: Map<String, Value>,
    /// The work, one entry per invocation, each whatever the worker
    /// wants under `input_key`.
    pub inputs: Vec<Value>,
    /// Which key an input goes under in the worker's payload.
    pub input_key: String,
    /// How many invocations may be in flight from this dispatcher. The
    /// call's total in flight is the sum across dispatchers.
    pub max_in_flight: usize,
    /// Where to send the worker invocations, for a local run against
    /// `cargo lambda watch`. `None` means real Lambda.
    pub endpoint_url: Option<String>,
    /// Extra attempts after a *transport* failure, per invocation.
    pub retries: usize,
    /// Whether to put one record per invocation on the frames.
    pub return_stats: bool,
    /// Whether to send the workers' outputs back at all.
    pub return_outputs: bool,
}

/// Statistics for a single worker invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationStats {
    /// Where this invocation's input sat in the partition, counting from 0.
    pub index: usize,
    /// The worker invocation's AWS request ID
    pub req_id: String,
    /// Epoch seconds, to match Python's `time.time()`.
    pub invoke_start: f64,
    pub invoke_end: f64,
}

/// One worker's output.
///
/// `value` is the worker's whole body, verbatim. Never null: a worker
/// that returned nothing gets no `Output` at all.
#[derive(Debug, Serialize, Deserialize)]
pub struct Output {
    /// Where the input this came from sat in the partition, as `InvocationStats::index`.
    pub index: usize,
    /// As the worker wrote it: checked to be JSON, but never parsed into
    /// a tree, which takes several times the memory of the text.
    pub value: Box<RawValue>,
}

/// What finished since the last frame.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Completed {
    /// Invocations that came back successfully.
    pub succeeded: usize,
    /// Empty unless the request asked for outputs - and a worker that
    /// returned nothing has no entry here either way.
    pub outputs: Vec<Output>,
    /// Empty unless the request asked for stats.
    pub invocation_stats: Vec<InvocationStats>,
    /// The first invocation of this frame to fail every attempt, as its
    /// own message.
    pub first_error: Option<String>,
}

impl Completed {
    /// Nothing happened, so there is nothing to send.
    pub fn is_empty(&self) -> bool {
        self.succeeded == 0
            && self.outputs.is_empty()
            && self.invocation_stats.is_empty()
            && self.first_error.is_none()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DispatcherStats {
    /// This dispatcher's own AWS request id, so its run can be found in
    /// CloudWatch alongside the invocations it dispatched.
    pub req_id: String,
    pub dispatched: usize,
    pub succeeded: usize,
    /// Attempts beyond the first, across all invocations.
    pub retried: usize,
    /// Summed wait before those attempts.
    pub retry_wait_ms: f64,
    /// Whether this invocation had to build the Lambda clients, i.e.
    /// whether the execution environment was cold. Null for a local
    /// run, which has no shared set to build.
    pub cold: Option<bool>,
    /// HTTP/2 connections the invocations were spread over.
    pub connections: usize,
    pub elapsed_ms: f64,
}

/// What goes over the wire, one JSON object per line: a completed frame
/// per reporting tick, followed by exactly one summary.
#[derive(Debug, Serialize, Deserialize)]
pub enum Frame {
    Completed(Completed),
    Summary(Box<DispatcherStats>),
}

#[cfg(test)]
mod tests {
    use super::{
        connections_for, Completed, DispatcherStats, Frame, InvocationStats, Output, MAX_IN_FLIGHT,
        MAX_LAMBDA_CLIENTS, STREAMS_PER_CONNECTION,
    };
    use serde_json::json;

    fn a_summary() -> DispatcherStats {
        DispatcherStats {
            req_id: "dispatcher-0".to_string(),
            dispatched: 3,
            succeeded: 3,
            retried: 1,
            retry_wait_ms: 2.5,
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
                succeeded: 2,
                outputs: vec![Output {
                    index: 4,
                    value: serde_json::value::to_raw_value(&json!({"copied": 1}))
                        .expect("serializes"),
                }],
                invocation_stats: vec![InvocationStats {
                    index: 4,
                    req_id: "w-1".to_string(),
                    invoke_start: 1.0,
                    invoke_end: 2.0,
                }],
                first_error: Some("no".to_string()),
            }),
            Frame::Summary(Box::new(a_summary())),
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
            json!({"Completed": {"succeeded": 0, "outputs": [], "invocation_stats": [], "first_error": null}})
        );
    }

    #[test]
    fn connections_cover_the_concurrency_asked_for() {
        // Too few connections does not error; the run is just slower.
        for max_in_flight in [1usize, 99, 100, 101, 128, 900, MAX_IN_FLIGHT] {
            let n = connections_for(max_in_flight);
            assert!(
                n * STREAMS_PER_CONNECTION >= max_in_flight,
                "{max_in_flight} in flight over {n} connection(s)"
            );
            assert!((1..=MAX_LAMBDA_CLIENTS).contains(&n));
        }
    }

    #[test]
    fn the_clients_built_cover_the_in_flight_limit() {
        // `div_ceil` guarantees the arithmetic; this is the clamp.
        assert_eq!(connections_for(MAX_IN_FLIGHT), MAX_LAMBDA_CLIENTS);
    }

    #[test]
    fn one_connection_is_enough_below_the_stream_limit() {
        assert_eq!(connections_for(1), 1);
        assert_eq!(connections_for(STREAMS_PER_CONNECTION), 1);
        assert_eq!(connections_for(STREAMS_PER_CONNECTION + 1), 2);
    }
}
