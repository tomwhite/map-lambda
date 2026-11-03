//! Combining a partition's outputs, around the callback its frames go to,
//! for a combining dispatcher.
//!
//! The dispatch loop runs as it does for a map that returns its outputs,
//! null ones included. `Combining` takes them out of each `Completed` frame
//! on its way to the client and combines them on a thread of its own, so
//! that a slow combine holds up neither the invocations nor the frames. The
//! state goes out on one last frame, once every invocation has finished.

use crate::dispatch::ReportCallback;
use crate::protocol::{Combined, Completed, Output};
use futures::FutureExt;
use lambda_runtime::Error;
use map_lambda_combiner::Combiner;
use std::ops::ControlFlow;
use tokio::sync::mpsc;
use tokio::task::{JoinError, JoinHandle};

/// Outputs waiting to be combined, beyond which handing over waits, which
/// holds up the dispatch loop, and so new invocations. Lambda caps a
/// worker's response at 6 MB, so the queue holds at most 384 MB.
const QUEUED_OUTPUTS: usize = 64;

/// A `ReportCallback` that combines the outputs on their way to `inner`,
/// and sends the state once they are all in.
pub(crate) struct Combining<'a, R> {
    inner: &'a mut R,
    /// `None` once the combiner has failed, which a frame has already said.
    thread: Option<CombinerThread>,
}

/// The partition's state, or why the combiner failed.
type Outcome = Result<Option<Combined>, String>;

struct CombinerThread {
    outputs: mpsc::Sender<Output>,
    thread: JoinHandle<Outcome>,
}

impl<'a, R: ReportCallback> Combining<'a, R> {
    pub(crate) fn new<C: Combiner + 'static>(combiner: C, inner: &'a mut R) -> Self {
        Self {
            inner,
            thread: Some(CombinerThread::start(combiner)),
        }
    }

    /// No more outputs are coming: send the state, on a frame of its own.
    pub(crate) async fn finish(mut self) -> Result<(), Error> {
        let Some(CombinerThread { outputs, thread }) = self.thread.take() else {
            return Ok(());
        };
        drop(outputs);
        let mut last = Completed::default();
        match outcome(thread.await) {
            Ok(state) => last.states.extend(state),
            Err(message) => last.first_error = Some(message),
        }
        if !last.is_empty() {
            // Every invocation has finished, so there is nothing to stop.
            let _ = self.inner.completed(last).await?;
        }
        Ok(())
    }
}

impl<R: ReportCallback> ReportCallback for Combining<'_, R> {
    async fn completed(&mut self, mut completed: Completed) -> Result<ControlFlow<()>, Error> {
        let outputs = std::mem::take(&mut completed.outputs);
        let mut stop = false;
        if let Some(running) = &mut self.thread {
            for output in outputs {
                // Refused only once the thread has stopped, which is
                // found out below.
                let _ = running.outputs.send(output).await;
            }
            // While outputs can still come, the thread stops only by failing.
            if let Some(joined) = (&mut running.thread).now_or_never() {
                if let Err(message) = outcome(joined) {
                    completed.first_error.get_or_insert(message);
                }
                self.thread = None;
                stop = true;
            }
        }
        let flow = self.inner.completed(completed).await?;
        Ok(if stop { ControlFlow::Break(()) } else { flow })
    }

    /// Every index that succeeded is in the state, so the combiner has to
    /// hear of those with nothing to combine too.
    fn wants_null_outputs(&self) -> bool {
        true
    }
}

impl CombinerThread {
    fn start<C: Combiner + 'static>(combiner: C) -> Self {
        let (outputs, received) = mpsc::channel(QUEUED_OUTPUTS);
        Self {
            outputs,
            thread: tokio::task::spawn_blocking(move || run(combiner, received)),
        }
    }
}

/// Combine every output handed over, then finish.
fn run<C: Combiner>(mut combiner: C, mut outputs: mpsc::Receiver<Output>) -> Outcome {
    let mut indexes = Vec::new();
    while let Some(Output { index, value }) = outputs.blocking_recv() {
        indexes.push(index);
        // A worker that returned nothing is covered, with nothing to combine.
        if value.get() == "null" {
            continue;
        }
        combiner
            .combine(&value)
            .map_err(|e| format!("the combiner refused an output: {e}"))?;
    }
    let state = combiner
        .finish()
        .map_err(|e| format!("the combiner could not finish: {e}"))?;
    Ok((state.is_some() || !indexes.is_empty()).then_some(Combined { indexes, state }))
}

fn outcome(joined: Result<Outcome, JoinError>) -> Outcome {
    joined.unwrap_or_else(|_| Err("the combiner panicked".to_string()))
}

#[cfg(test)]
mod tests {
    use super::Combining;
    use crate::dispatch::ReportCallback;
    use crate::protocol::{Combined, Completed, Output};
    use lambda_runtime::Error;
    use map_lambda_combiner::Combiner;
    use serde::Deserialize;
    use serde_json::value::{to_raw_value, RawValue};
    use serde_json::{json, Value};
    use std::ops::ControlFlow;

    /// Where the frames would have gone.
    #[derive(Default)]
    struct Recorder {
        frames: Vec<Completed>,
    }

    impl ReportCallback for Recorder {
        async fn completed(&mut self, completed: Completed) -> Result<ControlFlow<()>, Error> {
            self.frames.push(completed);
            Ok(ControlFlow::Continue(()))
        }
    }

    impl Recorder {
        fn combined(&self) -> impl Iterator<Item = &Combined> {
            self.frames.iter().flat_map(|frame| &frame.states)
        }

        fn states(&self) -> Vec<Option<&str>> {
            self.combined()
                .map(|combined| combined.state.as_deref().map(RawValue::get))
                .collect()
        }

        fn covered(&self) -> Vec<usize> {
            let mut covered: Vec<usize> = self
                .combined()
                .flat_map(|combined| combined.indexes.iter().copied())
                .collect();
            covered.sort();
            covered
        }

        fn first_error(&self) -> Option<&str> {
            self.frames
                .iter()
                .find_map(|frame| frame.first_error.as_deref())
        }
    }

    #[derive(Deserialize)]
    struct Copied {
        copied: i64,
    }

    /// Sums `copied`.
    struct Sum(Option<i64>);

    impl Combiner for Sum {
        const NAME: &'static str = "sum";

        fn new(_: Value) -> Result<Self, Error> {
            Ok(Self(None))
        }

        fn combine(&mut self, output: &RawValue) -> Result<(), Error> {
            let Copied { copied } =
                serde_json::from_str(output.get()).map_err(|_| "no `copied`")?;
            self.0 = Some(self.0.unwrap_or(0) + copied);
            Ok(())
        }

        fn finish(self) -> Result<Option<Box<RawValue>>, Error> {
            Ok(self.0.map(|sum| to_raw_value(&sum)).transpose()?)
        }
    }

    /// Panics on its second output.
    struct Panicking(bool);

    impl Combiner for Panicking {
        const NAME: &'static str = "sum";

        fn new(_: Value) -> Result<Self, Error> {
            Ok(Self(false))
        }

        fn combine(&mut self, _: &RawValue) -> Result<(), Error> {
            assert!(!std::mem::replace(&mut self.0, true), "a second output");
            Ok(())
        }

        fn finish(self) -> Result<Option<Box<RawValue>>, Error> {
            Ok(Some(to_raw_value(&1)?))
        }
    }

    fn summing() -> Sum {
        Sum(None)
    }

    /// A frame of `outputs`, each `(index, output)`.
    fn frame(outputs: &[(usize, Value)]) -> Completed {
        Completed {
            succeeded: outputs.len(),
            outputs: outputs
                .iter()
                .map(|(index, value)| Output {
                    index: *index,
                    value: to_raw_value(value).expect("json"),
                })
                .collect(),
            ..Completed::default()
        }
    }

    async fn send<R: ReportCallback>(combining: &mut Combining<'_, R>, completed: Completed) {
        let _ = combining.completed(completed).await.expect("sent");
    }

    fn copied(index: usize) -> (usize, Value) {
        (index, json!({"copied": 1}))
    }

    #[tokio::test]
    async fn states_go_out_in_place_of_the_outputs() {
        let mut recorder = Recorder::default();
        let mut combining = Combining::new(summing(), &mut recorder);
        send(&mut combining, frame(&[copied(0), copied(1), copied(2)])).await;
        combining.finish().await.expect("finished");

        assert!(recorder.frames.iter().all(|frame| frame.outputs.is_empty()));
        assert_eq!(recorder.frames[0].succeeded, 3);
        assert_eq!(recorder.states(), [Some("3")]);
    }

    #[tokio::test]
    async fn one_state_covers_every_index_across_the_frames() {
        let mut recorder = Recorder::default();
        let mut combining = Combining::new(summing(), &mut recorder);
        send(&mut combining, frame(&[copied(0), copied(1), copied(2)])).await;
        send(&mut combining, frame(&[copied(3), copied(4)])).await;
        combining.finish().await.expect("finished");

        assert_eq!(recorder.states(), [Some("5")]);
        assert_eq!(recorder.covered(), [0, 1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn a_silent_worker_is_covered_with_nothing_combined() {
        let mut recorder = Recorder::default();
        let mut combining = Combining::new(summing(), &mut recorder);
        send(&mut combining, frame(&[(0, Value::Null)])).await;
        combining.finish().await.expect("finished");

        assert_eq!(recorder.states(), [None]);
        assert_eq!(recorder.covered(), [0]);
    }

    #[tokio::test]
    async fn an_output_the_combiner_refuses_fails_the_partition_with_no_state() {
        let mut recorder = Recorder::default();
        let mut combining = Combining::new(summing(), &mut recorder);
        send(
            &mut combining,
            frame(&[copied(0), (1, json!({"other": 1}))]),
        )
        .await;
        combining.finish().await.expect("finished");

        assert!(recorder.states().is_empty());
        let reported = recorder.first_error().expect("a message");
        assert!(reported.contains("the combiner refused an output: no `copied`"));
    }

    #[tokio::test]
    async fn a_combiner_that_panics_fails_the_partition() {
        let mut recorder = Recorder::default();
        let mut combining = Combining::new(Panicking(false), &mut recorder);
        send(&mut combining, frame(&[copied(0), copied(1)])).await;
        combining.finish().await.expect("finished");

        assert!(recorder.states().is_empty());
        assert!(recorder
            .first_error()
            .expect("a message")
            .contains("panicked"));
    }

    #[tokio::test]
    async fn a_combining_callback_asks_for_null_outputs() {
        let mut recorder = Recorder::default();
        let combining = Combining::new(summing(), &mut recorder);
        assert!(combining.wants_null_outputs());
    }
}
