//! One call: its inputs partitioned across dispatchers, their frames
//! merged as they arrive, and every partition checked once all are in.

use crate::frames::Framer;
use crate::invoker::Invoker;
use crate::plan::{dispatchers_for, partitions, shares};
use crate::Error;
use async_stream::try_stream;
use futures::stream::{BoxStream, Stream, StreamExt};
use map_lambda_protocol::{Combine, Completed, Frame, Request};
pub use map_lambda_protocol::{DispatcherStats, InvocationStats};
use serde::Serialize;
use serde_json::value::RawValue;
use serde_json::{Map, Value};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

/// Lambda's hard cap on a synchronous request payload, which the whole
/// `Request` has to fit: inputs, one copy of `constant_args`, and framing.
pub(crate) const MAX_REQUEST_BYTES: usize = 6 * 1024 * 1024;

/// What one `map_unordered`, `for_each` or `map_combine` is asked to do.
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
    /// Ignored by a combining call, which always gets its states back.
    pub(crate) return_outputs: bool,
    pub(crate) return_stats: bool,
    /// Which call of its session this is, stamped on every record.
    pub(crate) number: usize,
}

/// A call's batches as they arrive. It ends with an error if the call
/// fails; dropping it closes the dispatchers' streams, as
/// [`Session::map_unordered`](crate::Session::map_unordered) describes.
///
/// Read the batches for progress and stats, or use [`outputs`](Self::outputs)
/// or [`finish`](Self::finish) for just the result.
#[must_use = "a call invokes nothing until its batches are read"]
pub struct Batches(BoxStream<'static, Result<Batch, Error>>);

impl Batches {
    /// Each `(index into inputs, output)` as it arrives, without the batches
    /// around them.
    pub fn outputs(
        mut self,
    ) -> impl Stream<Item = Result<(usize, Box<RawValue>), Error>> + Send + Unpin + 'static {
        Box::pin(try_stream! {
            while let Some(batch) = self.next().await {
                for output in batch?.outputs {
                    yield output;
                }
            }
        })
    }

    /// Run the call to the end, which is what checks it.
    pub async fn finish(mut self) -> Result<(), Error> {
        while let Some(batch) = self.next().await {
            batch?;
        }
        Ok(())
    }
}

impl Stream for Batches {
    type Item = Result<Batch, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_next_unpin(cx)
    }
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
    /// dispatcher did. A failed partition stops the call before they
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

/// What a call's dispatchers send, merged as it arrives and numbered against
/// the caller's inputs, for a call's batches to be made from.
pub(crate) enum Event {
    /// A `Completed` frame. One carrying a failure ends the stream instead.
    Completed {
        succeeded: usize,
        outputs: Vec<(usize, Box<RawValue>)>,
        /// A combiner's state, and the indexes it covers.
        states: Vec<(Vec<usize>, Option<Box<RawValue>>)>,
        invocation_stats: Vec<Record<InvocationStats>>,
    },
    /// Every dispatcher's summary, once all are in. A partition that falls
    /// short ends the stream after it.
    Summaries(Vec<Record<DispatcherStats>>),
}

/// What a started call reads its frames against.
struct Started {
    /// How many dispatchers.
    count: usize,
    /// Inputs per partition.
    expected: Vec<usize>,
    number: usize,
    call_start: f64,
}

impl Started {
    /// Partition `k` holds `inputs[k::count]`.
    fn place(&self, partition: usize, index: usize) -> usize {
        partition + index * self.count
    }

    fn record<T>(&self, record: T) -> Record<T> {
        Record {
            record,
            call: self.number,
            call_start: self.call_start,
        }
    }

    /// A `Completed` frame's records, numbered against the caller's inputs.
    /// Its failure, if it carries one, ends the call: retries are already
    /// spent, or a combiner failed, so the call cannot succeed and the
    /// frame's outputs are moot.
    fn progress(
        &self,
        partition: usize,
        first_error: Option<String>,
        invocation_stats: Vec<InvocationStats>,
    ) -> Result<Vec<Record<InvocationStats>>, Error> {
        if let Some(error) = first_error {
            return Err(Error::PartitionFailed(error));
        }
        Ok(invocation_stats
            .into_iter()
            .map(|stats| {
                self.record(InvocationStats {
                    index: self.place(partition, stats.index),
                    ..stats
                })
            })
            .collect())
    }

    /// Every dispatcher's summary, stamped, and whether each did all of
    /// its partition.
    fn settle(
        &self,
        summaries: Vec<Option<DispatcherStats>>,
        succeeded: &[usize],
    ) -> (Vec<Record<DispatcherStats>>, Result<(), Error>) {
        let finished: Vec<DispatcherStats> = summaries
            .into_iter()
            .map(|summary| summary.expect("a partition's stream ends with its summary or an error"))
            .collect();
        let checked = finished
            .iter()
            .zip(succeeded.iter().zip(&self.expected))
            .try_for_each(|(stats, (&succeeded, &expected))| check(stats, succeeded, expected));
        let records = finished
            .into_iter()
            .map(|stats| self.record(stats))
            .collect();
        (records, checked)
    }
}

/// Start `inputs` through `call.dispatchers` dispatchers, for their
/// outputs.
///
/// Refuses, with nothing invoked, a call that could not have worked, as
/// [`events`] says.
pub(crate) fn start(
    invoker: Arc<dyn Invoker>,
    call: Call,
    inputs: Vec<Value>,
) -> Result<Batches, Error> {
    let mut events = events(invoker, call, None, inputs)?;

    Ok(Batches(Box::pin(try_stream! {
        while let Some(event) = events.next().await {
            match event? {
                Event::Completed { succeeded, outputs, invocation_stats, .. } => {
                    let batch = Batch {
                        succeeded,
                        outputs,
                        invocation_stats,
                        dispatcher_stats: Vec::new(),
                    };
                    if !batch.is_empty() {
                        yield batch;
                    }
                }
                Event::Summaries(dispatcher_stats) => {
                    yield Batch { dispatcher_stats, ..Batch::default() };
                }
            }
        }
    })))
}

/// Start `inputs` through `call.dispatchers` dispatchers, combining with
/// `combine` if given, and merge what they send as it arrives.
///
/// Refuses, with nothing invoked, a call that could not have worked: a
/// split no dispatcher could carry, or a partition over Lambda's
/// request limit.
pub(crate) fn events(
    invoker: Arc<dyn Invoker>,
    call: Call,
    combine: Option<Combine>,
    inputs: Vec<Value>,
) -> Result<BoxStream<'static, Result<Event, Error>>, Error> {
    let (started, mut arriving) = launch(invoker, call, combine, inputs)?;

    Ok(Box::pin(try_stream! {
        let mut summaries: Vec<Option<DispatcherStats>> = vec![None; started.count];
        let mut succeeded = vec![0; started.count];
        while let Some((partition, frame)) = arriving.next().await {
            match frame? {
                Frame::Completed(Completed {
                    succeeded: n,
                    outputs,
                    states,
                    invocation_stats,
                    first_error,
                }) => {
                    let invocation_stats = started.progress(partition, first_error, invocation_stats)?;
                    succeeded[partition] += n;
                    let place = |index: usize| started.place(partition, index);
                    yield Event::Completed {
                        succeeded: n,
                        outputs: outputs
                            .into_iter()
                            .map(|output| (place(output.index), output.value))
                            .collect(),
                        states: states
                            .into_iter()
                            .map(|combined| {
                                (combined.indexes.into_iter().map(place).collect(), combined.state)
                            })
                            .collect(),
                        invocation_stats,
                    };
                }
                Frame::Summary(stats) => summaries[partition] = Some(*stats),
            }
        }

        let (dispatcher_stats, checked) = started.settle(summaries, &succeeded);
        yield Event::Summaries(dispatcher_stats);
        checked?;
    }))
}

/// Plan the call, build one request per partition, and start their
/// streams, merged as their frames arrive.
#[allow(clippy::type_complexity)]
fn launch(
    invoker: Arc<dyn Invoker>,
    call: Call,
    combine: Option<Combine>,
    inputs: Vec<Value>,
) -> Result<(Started, BoxStream<'static, (usize, Result<Frame, Error>)>), Error> {
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
                combine: combine.clone(),
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

    let started = Started {
        count,
        expected,
        number: call.number,
        call_start: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the clock is after 1970")
            .as_secs_f64(),
    };
    let arriving = futures::stream::select_all(payloads.into_iter().enumerate().map(
        |(partition, payload)| {
            frames(invoker.clone(), call.dispatcher_function.clone(), payload)
                .map(move |frame| (partition, frame))
        },
    ));
    Ok((started, Box::pin(arriving)))
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
/// `succeeded`, the total across its frames, is checked against `expected`,
/// the number of inputs this side sent.
fn check(stats: &DispatcherStats, succeeded: usize, expected: usize) -> Result<(), Error> {
    if succeeded != expected {
        return Err(Error::PartitionShort {
            req_id: stats.req_id.clone(),
            succeeded,
            expected,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{check, start, Call};
    use crate::invoker::{Chunks, Invoker};
    use crate::Error;
    use futures::executor::block_on;
    use futures::future::BoxFuture;
    use futures::stream::StreamExt;
    use map_lambda_protocol::DispatcherStats;
    use serde_json::{json, Map};
    use std::sync::Arc;

    fn stats(dispatched: usize) -> DispatcherStats {
        DispatcherStats {
            req_id: "dispatcher-0".to_string(),
            dispatched,
            retried: 0,
            retry_wait_ms: 0.0,
            cold: Some(false),
            connections: 1,
            elapsed_ms: 1.0,
        }
    }

    /// Answers with a whole partition's frames, then a stream error.
    struct FailsAfterItsResult;

    impl Invoker for FailsAfterItsResult {
        fn invoke_streaming<'a>(
            &'a self,
            _function: &'a str,
            _payload: Vec<u8>,
        ) -> BoxFuture<'a, Result<Chunks, Error>> {
            let completed = json!({"Completed": {
                "succeeded": 1, "outputs": [], "invocation_stats": [], "first_error": null,
            }});
            let summary = json!({"Summary": stats(1)});
            let chunks: Vec<Result<Vec<u8>, Error>> = vec![
                Ok(format!("{completed}\n{summary}\n").into_bytes()),
                Err(Error::DispatcherFailed("the stream failed".to_string())),
            ];
            Box::pin(async move { Ok(Box::pin(futures::stream::iter(chunks)) as Chunks) })
        }

        fn invoke_detached<'a>(
            &'a self,
            _function: &'a str,
            _payload: Vec<u8>,
        ) -> BoxFuture<'a, Result<(), Error>> {
            unreachable!("a call never invokes detached")
        }
    }

    #[test]
    fn a_stream_failing_after_its_result_fails_the_call() {
        let call = Call {
            dispatcher_function: "dispatcher".to_string(),
            worker_function: "worker".to_string(),
            input_key: "i".to_string(),
            constant_args: Map::new(),
            dispatchers: Some(1),
            concurrency: 1,
            retries: 0,
            endpoint_url: None,
            return_outputs: false,
            return_stats: false,
            number: 0,
        };
        let batches = start(Arc::new(FailsAfterItsResult), call, vec![json!(0)]).expect("starts");

        let mut batches: Vec<_> = block_on(batches.collect());

        assert!(matches!(
            batches.pop(),
            Some(Err(Error::DispatcherFailed(_)))
        ));
        assert!(batches.iter().all(Result::is_ok));
    }

    #[test]
    fn a_clean_full_partition_passes() {
        check(&stats(42), 42, 42).expect("clean");
    }

    /// The dangerous case: work silently skipped.
    #[test]
    fn a_short_partition_with_no_error_is_refused() {
        let refused = check(&stats(42), 41, 42).expect_err("refused");
        assert!(refused.to_string().contains("completed 41 of 42"));
    }
}
