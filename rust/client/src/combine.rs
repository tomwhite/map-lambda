//! A call whose dispatchers combine their outputs: [`CombineBatches`],
//! made from the same stream of frames as a map's [`Batches`](crate::Batches),
//! with states in place of outputs, and checked to cover each input once.

use crate::call::{events, Call, Event, Record};
use crate::invoker::Invoker;
use crate::Error;
use async_stream::try_stream;
use futures::stream::{BoxStream, Stream, StreamExt};
use map_lambda_protocol::{Combine, DispatcherStats, InvocationStats};
use serde_json::value::RawValue;
use serde_json::Value;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// [`Batches`](crate::Batches), for a combining call: its batches carry states in place
/// of outputs.
#[must_use = "a call invokes nothing until its batches are read"]
pub struct CombineBatches(BoxStream<'static, Result<CombineBatch, Error>>);

impl Stream for CombineBatches {
    type Item = Result<CombineBatch, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_next_unpin(cx)
    }
}

/// A [`Batch`](crate::Batch) of a combining call, with states in place of outputs.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct CombineBatch {
    /// As in [`Batch::succeeded`](crate::Batch::succeeded).
    pub succeeded: usize,
    /// The dispatchers' states, one each, as they finish.
    pub states: Vec<State>,
    /// As in [`Batch::invocation_stats`](crate::Batch::invocation_stats).
    pub invocation_stats: Vec<Record<InvocationStats>>,
    /// As in [`Batch::dispatcher_stats`](crate::Batch::dispatcher_stats).
    pub dispatcher_stats: Vec<Record<DispatcherStats>>,
}

impl CombineBatch {
    fn is_empty(&self) -> bool {
        self.succeeded == 0
            && self.states.is_empty()
            && self.invocation_stats.is_empty()
            && self.dispatcher_stats.is_empty()
    }
}

/// A combiner's state, and the inputs combined into it.
#[derive(Debug)]
#[non_exhaustive]
pub struct State {
    /// Indexes into inputs. Each input that succeeded is in exactly one
    /// state.
    pub indexes: Vec<usize>,
    /// The state, as the combiner wrote it.
    pub state: Box<RawValue>,
}

/// Which inputs have reached the caller through a state.
struct Coverage(Vec<bool>);

impl Coverage {
    fn cover(&mut self, index: usize) -> Result<(), Error> {
        if std::mem::replace(&mut self.0[index], true) {
            return Err(Error::CoveredTwice { index });
        }
        Ok(())
    }

    fn check(&self) -> Result<(), Error> {
        let missing = self.0.iter().filter(|covered| !**covered).count();
        if missing > 0 {
            return Err(Error::Uncovered { missing });
        }
        Ok(())
    }
}

/// Start `inputs` through `call.dispatchers` dispatchers, each combining
/// its outputs through `combine`'s combiner.
pub(crate) fn start_combining(
    invoker: Arc<dyn Invoker>,
    call: Call,
    combine: Combine,
    inputs: Vec<Value>,
) -> Result<CombineBatches, Error> {
    let mut coverage = Coverage(vec![false; inputs.len()]);
    let mut events = events(invoker, call, Some(combine), inputs)?;

    Ok(CombineBatches(Box::pin(try_stream! {
        while let Some(event) = events.next().await {
            match event? {
                Event::Completed { succeeded, states, invocation_stats, .. } => {
                    let mut batch = CombineBatch {
                        succeeded,
                        invocation_stats,
                        ..CombineBatch::default()
                    };
                    for (indexes, state) in states {
                        for &index in &indexes {
                            coverage.cover(index)?;
                        }
                        // Covering only workers that returned nothing.
                        if let Some(state) = state {
                            batch.states.push(State { indexes, state });
                        }
                    }
                    if !batch.is_empty() {
                        yield batch;
                    }
                }
                Event::Summaries(dispatcher_stats) => {
                    yield CombineBatch { dispatcher_stats, ..CombineBatch::default() };
                }
            }
        }
        coverage.check()?;
    })))
}

#[cfg(test)]
mod tests {
    use super::start_combining;
    use crate::call::Call;
    use crate::testing::Stub;
    use crate::Error;
    use futures::executor::block_on;
    use futures::StreamExt;
    use map_lambda_protocol::Combine;
    use serde_json::{json, Value};
    use std::sync::Arc;

    type States = Vec<(Vec<usize>, Value)>;

    /// A frame carrying one combined state and nothing else.
    fn combined(indexes: &[usize], state: Value) -> Value {
        json!({"Completed": {
            "succeeded": 0, "outputs": [], "states": [{"indexes": indexes, "state": state}],
            "invocation_stats": [], "first_error": null,
        }})
    }

    /// `frames`, then what a clean partition of two inputs ends with.
    fn partition(frames: &[Value]) -> Vec<Value> {
        let completed = json!({"Completed": {
            "succeeded": 2, "outputs": [], "invocation_stats": [], "first_error": null,
        }});
        let summary = json!({"Summary": {
            "req_id": "c-0", "dispatched": 2, "retried": 0, "retry_wait_ms": 0.0,
            "cold": false, "connections": 1, "elapsed_ms": 1.0,
        }});
        let mut frames = frames.to_vec();
        frames.extend([completed, summary]);
        frames
    }

    fn call() -> Call {
        Call {
            dispatcher_function: "dispatcher-sum".into(),
            worker_function: "worker".into(),
            input_key: "i".into(),
            constant_args: Default::default(),
            dispatchers: Some(2),
            concurrency: 8,
            retries: 0,
            endpoint_url: None,
            return_outputs: true,
            return_stats: false,
            number: 0,
        }
    }

    fn stubbed(partitions: [Vec<Value>; 2]) -> Arc<Stub> {
        let stub = Arc::new(Stub::default());
        for frames in partitions {
            let body = stub.respond();
            for frame in frames {
                body.unbounded_send(format!("{frame}\n").into_bytes())
                    .expect("open");
            }
        }
        stub
    }

    fn inputs() -> Vec<Value> {
        (0..4).map(Value::from).collect()
    }

    fn summing() -> Combine {
        Combine {
            combiner: "sum".into(),
            config: json!({"field": "i"}),
        }
    }

    fn run(partitions: [Vec<Value>; 2]) -> (States, Result<(), Error>, Arc<Stub>) {
        let stub = stubbed(partitions);
        let mut batches =
            start_combining(stub.clone(), call(), summing(), inputs()).expect("starts");
        let mut states = Vec::new();
        let ended = block_on(async {
            while let Some(batch) = batches.next().await {
                for state in batch?.states {
                    let parsed = serde_json::from_str(state.state.get()).expect("json");
                    states.push((state.indexes, parsed));
                }
            }
            Ok(())
        });
        (states, ended, stub)
    }

    /// Partition 1 holds inputs 1 and 3.
    #[test]
    fn states_are_yielded_against_the_callers_inputs() {
        let (mut states, ended, _) = run([
            partition(&[combined(&[0, 1], json!(1))]),
            partition(&[combined(&[1], json!(2)), combined(&[0], json!(3))]),
        ]);

        ended.expect("clean");
        states.sort_by_key(|(_, state)| state.as_i64());
        assert_eq!(
            states,
            [
                (vec![0, 2], json!(1)),
                (vec![3], json!(2)),
                (vec![1], json!(3))
            ]
        );
    }

    #[test]
    fn every_dispatcher_is_asked_to_combine_with_the_same_config() {
        let clean = || partition(&[combined(&[0, 1], json!(1))]);
        let (_, _, stub) = run([clean(), clean()]);

        for request in stub.requests() {
            let request: Value = serde_json::from_slice(&request).expect("json");
            assert_eq!(
                request["combine"],
                json!({"combiner": "sum", "config": {"field": "i"}})
            );
        }
    }

    #[test]
    fn a_state_covering_only_silent_workers_yields_nothing() {
        let (states, ended, _) = run([
            partition(&[combined(&[0, 1], json!(null))]),
            partition(&[combined(&[0, 1], json!(5))]),
        ]);

        ended.expect("clean");
        assert_eq!(states, [(vec![1, 3], json!(5))]);
    }

    /// Partition 0's index 1 is input 2, in both of its states.
    #[test]
    fn an_input_combined_into_two_states_is_refused() {
        let (_, ended, _) = run([
            partition(&[combined(&[0, 1], json!(1)), combined(&[1], json!(1))]),
            partition(&[combined(&[0, 1], json!(1))]),
        ]);

        let refused = ended.expect_err("refused").to_string();
        assert!(
            refused.contains("input 2 was combined into two states"),
            "{refused}"
        );
    }

    #[test]
    fn an_input_that_reached_no_state_is_refused() {
        let (_, ended, _) = run([
            partition(&[combined(&[0], json!(1))]),
            partition(&[combined(&[0, 1], json!(1))]),
        ]);

        assert!(matches!(ended, Err(Error::Uncovered { missing: 1 })));
    }

    /// A combiner's failure comes on a frame, like an invocation's.
    #[test]
    fn a_dispatcher_whose_combiner_failed_fails_the_call() {
        let failing = json!({"Completed": {
            "succeeded": 0, "outputs": [], "invocation_stats": [],
            "first_error": "the combiner refused an output: no",
        }});
        let (_, ended, _) = run([partition(&[combined(&[0, 1], json!(1))]), vec![failing]]);

        assert!(
            matches!(ended, Err(Error::PartitionFailed(ref message)) if message.contains("refused an output")),
            "{ended:?}"
        );
    }
}
