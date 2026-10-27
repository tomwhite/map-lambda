//! One call: its inputs partitioned across dispatchers, their frames
//! merged as they arrive, and every partition checked once all are in.

use crate::frames::Framer;
use crate::plan::{dispatchers_for, split, stride};
use crate::transport::Transport;
use crate::Error;
use async_stream::try_stream;
use futures::stream::{BoxStream, StreamExt};
use map_lambda_protocol::{Frame, Request, Response, Stat, MAX_REQUEST_BYTES};
pub use map_lambda_protocol::{Response as DispatcherStats, Stat as InvocationStats};
use serde::Serialize;
use serde_json::value::RawValue;
use serde_json::{Map, Value};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// What one `map_unordered` or `for_each` is asked to do.
#[derive(Debug, Clone)]
pub(crate) struct Call {
    /// The name the dispatcher is deployed under.
    pub(crate) dispatcher_function: String,
    pub(crate) target_function: String,
    pub(crate) input_key: String,
    /// The worker's payload, less its input.
    pub(crate) template: Map<String, Value>,
    /// `None` for the fewest that can hold `concurrency`.
    pub(crate) dispatchers: Option<usize>,
    /// Worker invocations in flight across the whole call.
    pub(crate) concurrency: usize,
    pub(crate) retries: usize,
    /// A local `cargo lambda watch` for the dispatchers to invoke workers
    /// on; `None` for real Lambda.
    pub(crate) endpoint_url: Option<String>,
    pub(crate) return_outputs: bool,
    pub(crate) return_stats: bool,
    /// Which call of its session this is, stamped on every record.
    pub(crate) call: usize,
}

/// What arrived since the last batch, numbered against the caller's
/// inputs.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct Batch {
    /// Invocations that came back successfully.
    pub done: usize,
    /// `(index into inputs, the worker's answer)`. Empty for a
    /// `for_each`, and a worker that answered null has no entry.
    pub answers: Vec<(usize, Box<RawValue>)>,
    /// One per invocation, if [`Settings::return_stats`](crate::Settings::return_stats).
    pub invocation_stats: Vec<Record<InvocationStats>>,
    /// One per dispatcher, all in the last batch: they arrive before any
    /// is checked, so a call that fails still says what each dispatcher
    /// did.
    pub dispatcher_stats: Vec<Record<DispatcherStats>>,
}

impl Batch {
    fn is_empty(&self) -> bool {
        self.done == 0
            && self.answers.is_empty()
            && self.invocation_stats.is_empty()
            && self.dispatcher_stats.is_empty()
    }
}

/// A record, stamped with the call it belongs to.
#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
pub struct Record<T> {
    /// The record itself.
    #[serde(flatten)]
    pub record: T,
    /// Which call of its session, counting from 0.
    pub call: usize,
    /// When that call began, as epoch seconds.
    pub call_start: f64,
}

/// A call's batches as they arrive. It ends with an error if the call
/// fails; dropping it tears down the dispatchers still running.
pub type Run = BoxStream<'static, Result<Batch, Error>>;

/// Start `inputs` through `call.dispatchers` dispatchers.
///
/// Refuses, with nothing invoked, a call that could not have worked: a
/// split no dispatcher could carry, or a partition over Lambda's
/// request limit.
pub(crate) fn dispatch(
    transport: Arc<dyn Transport>,
    call: Call,
    inputs: Vec<Value>,
) -> Result<Run, Error> {
    let local = call.endpoint_url.is_some();
    let count = call
        .dispatchers
        .unwrap_or_else(|| dispatchers_for(call.concurrency, local));
    let shares = split(call.concurrency, count, local)?;
    let partitions = stride(inputs, count);
    let expected: Vec<usize> = partitions.iter().map(Vec::len).collect();

    let payloads = partitions
        .into_iter()
        .zip(&shares)
        .map(|(inputs, &concurrency)| {
            serde_json::to_vec(&Request {
                target_function: call.target_function.clone(),
                template: call.template.clone(),
                inputs,
                input_key: call.input_key.clone(),
                concurrency,
                endpoint_url: call.endpoint_url.clone(),
                retries: call.retries,
                return_stats: call.return_stats,
                return_outputs: call.return_outputs,
            })
            .map_err(Error::json("building a dispatcher request"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    for (partition, payload) in payloads.iter().enumerate() {
        if payload.len() > MAX_REQUEST_BYTES {
            return Err(Error::RequestTooLarge {
                partition,
                bytes: payload.len(),
                need: (payload.len() * count).div_ceil(MAX_REQUEST_BYTES),
            });
        }
    }

    let stamp = Stamp {
        call: call.call,
        call_start: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the clock is after 1970")
            .as_secs_f64(),
    };
    let mut arriving = futures::stream::select_all(payloads.into_iter().enumerate().map(
        |(partition, payload)| {
            frames(transport.clone(), call.dispatcher_function.clone(), payload)
                .map(move |frame| (partition, frame))
        },
    ));

    Ok(Box::pin(try_stream! {
        let mut results: Vec<Option<Response>> = vec![None; count];
        while let Some((partition, frame)) = arriving.next().await {
            match frame? {
                Frame::Completed(completed) => {
                    // Retries are already spent, so the call cannot succeed
                    // and this frame's answers are moot.
                    if let Some(error) = completed.first_error {
                        Err(Error::InvocationFailed(error))?;
                    }
                    // Partition k holds inputs[k::count].
                    let place = |index: usize| partition + index * count;
                    let batch = Batch {
                        done: completed.done,
                        answers: completed
                            .outputs
                            .into_iter()
                            .map(|output| (place(output.index), output.response))
                            .collect(),
                        invocation_stats: completed
                            .stats
                            .into_iter()
                            .map(|stat| stamp.on(Stat { index: place(stat.index), ..stat }))
                            .collect(),
                        dispatcher_stats: Vec::new(),
                    };
                    if !batch.is_empty() {
                        yield batch;
                    }
                }
                Frame::Result(response) => results[partition] = Some(*response),
            }
        }

        let responses: Vec<Response> = results
            .into_iter()
            .map(|result| result.expect("a partition's stream ends with its result or an error"))
            .collect();
        yield Batch {
            dispatcher_stats: responses.iter().cloned().map(|r| stamp.on(r)).collect(),
            ..Batch::default()
        };
        for (partition, response) in responses.iter().enumerate() {
            check(response, expected[partition])?;
        }
    }))
}

#[derive(Clone, Copy)]
struct Stamp {
    call: usize,
    call_start: f64,
}

impl Stamp {
    fn on<T>(self, record: T) -> Record<T> {
        Record {
            record,
            call: self.call,
            call_start: self.call_start,
        }
    }
}

/// One dispatcher's frames as they arrive, ending in its result.
fn frames(
    transport: Arc<dyn Transport>,
    function: String,
    payload: Vec<u8>,
) -> BoxStream<'static, Result<Frame, Error>> {
    Box::pin(try_stream! {
        let mut chunks = transport.stream(&function, payload).await?;
        let mut framer = Framer::default();
        let mut finished = false;
        while let Some(chunk) = chunks.next().await {
            for frame in framer.push(&chunk?)? {
                finished |= matches!(frame, Frame::Result(_));
                yield frame;
            }
        }
        if !finished {
            Err(Error::NoResult)?;
        }
    })
}

/// Fail unless a dispatcher did all of its partition, and cleanly.
///
/// A short partition is silent - skipped work just never happens - so
/// the count is checked as well as the errors, against `expected`, the
/// number of inputs this side sent.
fn check(response: &Response, expected: usize) -> Result<(), Error> {
    if response.function_errors + response.transport_errors > 0 {
        return Err(Error::InvocationsFailed {
            req_id: response.req_id.clone(),
            function_errors: response.function_errors,
            transport_errors: response.transport_errors,
            first_error: response.first_error.clone(),
        });
    }
    if response.succeeded != expected {
        return Err(Error::PartitionShort {
            req_id: response.req_id.clone(),
            succeeded: response.succeeded,
            expected,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::check;
    use map_lambda_protocol::Response;

    fn response(succeeded: usize) -> Response {
        Response {
            req_id: "coord-0".to_string(),
            dispatched: succeeded,
            succeeded,
            function_errors: 0,
            transport_errors: 0,
            retried: 0,
            retry_wait_ms: 0.0,
            first_error: None,
            cold: Some(false),
            connections: 1,
            elapsed_ms: 1.0,
        }
    }

    fn refusal(response: Response, expected: usize) -> String {
        check(&response, expected).expect_err("refused").to_string()
    }

    #[test]
    fn a_clean_full_partition_passes() {
        check(&response(42), 42).expect("clean");
    }

    #[test]
    fn a_reported_failure_is_refused() {
        let failed = Response {
            function_errors: 1,
            first_error: Some("boom".into()),
            ..response(41)
        };
        assert!(refusal(failed, 42).contains("1 function"));
    }

    /// The dangerous case: work silently skipped.
    #[test]
    fn a_short_partition_with_no_error_is_refused() {
        let short = Response {
            dispatched: 42,
            ..response(41)
        };
        assert!(refusal(short, 42).contains("completed 41 of 42"));
    }
}
