//! The wire contract between a client and a dispatcher: what a
//! dispatcher is asked for, the frames it responds with, and the limits
//! both ends are bound by.
//!
//! Every type derives both directions, because both ends depend on this
//! crate. Every struct refuses fields it does not know, so ends built
//! from different versions fail rather than quietly disagree. Callers use
//! the client's re-exports, not this.

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_json::{Map, Value};

/// How many invocations a dispatcher puts through one HTTP/2 connection:
/// headroom below Lambda's SETTINGS_MAX_CONCURRENT_STREAMS of 128, past
/// which requests on the connection queue.
pub const STREAMS_PER_CONNECTION: usize = 100;

/// The most invocations one dispatcher will carry at once.
pub const MAX_IN_FLIGHT: usize = 1_600;

/// What a dispatcher against `cargo lambda watch` carries: it builds one
/// throwaway client, so one connection's worth.
pub const MAX_IN_FLIGHT_LOCAL: usize = STREAMS_PER_CONNECTION;

/// Clients a dispatcher builds at cold start, enough to cover
/// `MAX_IN_FLIGHT`.
pub const MAX_LAMBDA_CLIENTS: usize = MAX_IN_FLIGHT.div_ceil(STREAMS_PER_CONNECTION);

/// What the client sends: a function to call, a payload to call it
/// with, and the work to spread across the calls.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    /// Extra attempts after a failed invoke, per invocation.
    pub retries: usize,
    /// Whether to put one record per invocation on the frames.
    pub return_stats: bool,
    /// Whether to send the workers' outputs back at all.
    pub return_outputs: bool,
}

/// What goes over the wire, one JSON object per line: completed frames
/// as the partition runs, followed by exactly one summary.
#[derive(Debug, Serialize, Deserialize)]
pub enum Frame {
    /// What finished since the last frame. The first is sent empty, before
    /// any work starts; after that, only a reporting tick with something
    /// new sends one.
    Completed(Completed),
    /// The partition's stats, sent once, last.
    Summary(Box<DispatcherStats>),
}

/// What finished since the last frame.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

/// One worker's output.
///
/// `value` is the worker's whole body, verbatim. Never null: a worker
/// that returned nothing gets no `Output` at all.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Output {
    /// Where the input this came from sat in the partition, as `InvocationStats::index`.
    pub index: usize,
    /// As the worker wrote it: checked to be JSON, but never parsed into
    /// a tree, which takes several times the memory of the text.
    pub value: Box<RawValue>,
}

/// Statistics for a single worker invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvocationStats {
    /// Where this invocation's input sat in the partition, counting from 0.
    pub index: usize,
    /// The worker invocation's AWS request ID
    pub req_id: String,
    /// Epoch seconds, to match Python's `time.time()`.
    pub invoke_start: f64,
    pub invoke_end: f64,
}

/// What one dispatcher reports about its partition, in the summary frame.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DispatcherStats {
    /// This dispatcher's own AWS request id, so its run can be found in
    /// CloudWatch alongside the invocations it dispatched.
    pub req_id: String,
    /// The partition's inputs. After a failed invocation the dispatcher
    /// starts no more, so fewer may have run.
    pub dispatched: usize,
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
    /// From starting the first invocation to sending the last `Completed`
    /// frame. A cold start's client building comes before it.
    pub elapsed_ms: f64,
}

#[cfg(test)]
mod tests {
    use super::{Completed, DispatcherStats, Frame, InvocationStats, Output};
    use serde_json::json;

    fn a_summary() -> DispatcherStats {
        DispatcherStats {
            req_id: "dispatcher-0".to_string(),
            dispatched: 3,
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
    fn a_field_this_side_does_not_know_is_refused() {
        let line = r#"{"Completed":{"succeeded":0,"outputs":[],"invocation_stats":[],"first_error":null,"extra":1}}"#;
        assert!(serde_json::from_str::<Frame>(line).is_err());
    }

    #[test]
    fn frames_are_tagged_by_kind() {
        let value = serde_json::to_value(Frame::Completed(Completed::default())).expect("ok");
        assert_eq!(
            value,
            json!({"Completed": {"succeeded": 0, "outputs": [], "invocation_stats": [], "first_error": null}})
        );
    }
}
