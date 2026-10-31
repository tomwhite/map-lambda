//! One call: its inputs partitioned across dispatchers, their frames
//! merged as they arrive, and every partition checked once all are in.

use crate::frames::Framer;
use crate::invoker::Invoker;
use crate::plan::{dispatchers_for, partitions, shares};
use crate::Error;
use async_stream::try_stream;
use futures::stream::{BoxStream, StreamExt};
pub use map_lambda_protocol::{DispatcherStats, InvocationStats};
use map_lambda_protocol::{Frame, Request, MAX_REQUEST_BYTES};
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
    pub(crate) worker_function: String,
    pub(crate) input_key: String,
    /// The worker's payload, less its input.
    pub(crate) constant_args: Map<String, Value>,
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
    pub(crate) number: usize,
}

/// What arrived since the last batch, numbered against the caller's
/// inputs.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct Batch {
    /// Invocations that came back successfully.
    pub succeeded: usize,
    /// `(index into inputs, the worker's output)`. Empty for a
    /// `for_each`, and a worker that returned null has no entry.
    pub outputs: Vec<(usize, Box<RawValue>)>,
    /// One per invocation, if [`Settings::return_stats`](crate::Settings::return_stats).
    pub invocation_stats: Vec<Record<InvocationStats>>,
    /// One per dispatcher, all in the last batch: they arrive before any
    /// is checked, so a call that fails a check still says what each
    /// dispatcher did. A failed invocation stops the call before they
    /// arrive, so it has none.
    pub dispatcher_stats: Vec<Record<DispatcherStats>>,
}

impl Batch {
    fn is_empty(&self) -> bool {
        self.succeeded == 0
            && self.outputs.is_empty()
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
pub type Batches = BoxStream<'static, Result<Batch, Error>>;

/// Start `inputs` through `call.dispatchers` dispatchers.
///
/// Refuses, with nothing invoked, a call that could not have worked: a
/// split no dispatcher could carry, or a partition over Lambda's
/// request limit.
pub(crate) fn start(
    invoker: Arc<dyn Invoker>,
    call: Call,
    inputs: Vec<Value>,
) -> Result<Batches, Error> {
    let local = call.endpoint_url.is_some();
    let count = call
        .dispatchers
        .unwrap_or_else(|| dispatchers_for(call.concurrency, local));
    let shares = shares(call.concurrency, count, local)?;
    let partitions = partitions(inputs, count);
    let expected: Vec<usize> = partitions.iter().map(Vec::len).collect();

    let payloads = partitions
        .into_iter()
        .zip(&shares)
        .map(|(inputs, &max_in_flight)| {
            serde_json::to_vec(&Request {
                worker_function: call.worker_function.clone(),
                constant_args: call.constant_args.clone(),
                inputs,
                input_key: call.input_key.clone(),
                max_in_flight,
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
        call: call.number,
        call_start: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the clock is after 1970")
            .as_secs_f64(),
    };
    let mut arriving = futures::stream::select_all(payloads.into_iter().enumerate().map(
        |(partition, payload)| {
            frames(invoker.clone(), call.dispatcher_function.clone(), payload)
                .map(move |frame| (partition, frame))
        },
    ));

    Ok(Box::pin(try_stream! {
        let mut summaries: Vec<Option<DispatcherStats>> = vec![None; count];
        while let Some((partition, frame)) = arriving.next().await {
            match frame? {
                Frame::Completed(completed) => {
                    // Retries are already spent, so the call cannot succeed
                    // and this frame's outputs are moot.
                    if let Some(error) = completed.first_error {
                        Err(Error::InvocationFailed(error))?;
                    }
                    // Partition k holds inputs[k::count].
                    let place = |index: usize| partition + index * count;
                    let batch = Batch {
                        succeeded: completed.succeeded,
                        outputs: completed
                            .outputs
                            .into_iter()
                            .map(|output| (place(output.index), output.value))
                            .collect(),
                        invocation_stats: completed
                            .invocation_stats
                            .into_iter()
                            .map(|stat| stamp.on(InvocationStats { index: place(stat.index), ..stat }))
                            .collect(),
                        dispatcher_stats: Vec::new(),
                    };
                    if !batch.is_empty() {
                        yield batch;
                    }
                }
                Frame::Summary(stats) => summaries[partition] = Some(*stats),
            }
        }

        let finished: Vec<DispatcherStats> = summaries
            .into_iter()
            .map(|summary| summary.expect("a partition's stream ends with its summary or an error"))
            .collect();
        yield Batch {
            dispatcher_stats: finished.iter().cloned().map(|r| stamp.on(r)).collect(),
            ..Batch::default()
        };
        for (partition, stats) in finished.iter().enumerate() {
            check(stats, expected[partition])?;
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

/// One dispatcher's frames as they arrive, ending in its summary.
fn frames(
    invoker: Arc<dyn Invoker>,
    function_name: String,
    payload: Vec<u8>,
) -> BoxStream<'static, Result<Frame, Error>> {
    Box::pin(try_stream! {
        let mut chunks = invoker.invoke_streaming(&function_name, payload).await?;
        let mut framer = Framer::default();
        let mut finished = false;
        while let Some(chunk) = chunks.next().await {
            for frame in framer.push(&chunk?)? {
                finished |= matches!(frame, Frame::Summary(_));
                yield frame;
            }
        }
        if !finished {
            Err(Error::NoSummary)?;
        }
    })
}

/// Fail unless a dispatcher did all of its partition.
///
/// A short partition is silent - skipped work just never happens - so
/// the count is checked against `expected`, the number of inputs this
/// side sent.
fn check(stats: &DispatcherStats, expected: usize) -> Result<(), Error> {
    if stats.succeeded != expected {
        return Err(Error::PartitionShort {
            req_id: stats.req_id.clone(),
            succeeded: stats.succeeded,
            expected,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::check;
    use map_lambda_protocol::DispatcherStats;

    fn stats(succeeded: usize) -> DispatcherStats {
        DispatcherStats {
            req_id: "dispatcher-0".to_string(),
            dispatched: succeeded,
            succeeded,
            retried: 0,
            retry_wait_ms: 0.0,
            cold: Some(false),
            connections: 1,
            elapsed_ms: 1.0,
        }
    }

    fn refusal(stats: DispatcherStats, expected: usize) -> String {
        check(&stats, expected).expect_err("refused").to_string()
    }

    #[test]
    fn a_clean_full_partition_passes() {
        check(&stats(42), 42).expect("clean");
    }

    /// The dangerous case: work silently skipped.
    #[test]
    fn a_short_partition_with_no_error_is_refused() {
        let short = DispatcherStats {
            dispatched: 42,
            ..stats(41)
        };
        assert!(refusal(short, 42).contains("completed 41 of 42"));
    }
}
