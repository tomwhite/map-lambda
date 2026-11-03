//! Folding a partition's finished invocations into the counts it is judged
//! on.

use crate::invoke::{Invocation, Outcome};
use crate::protocol::{Completed, InvocationStats, Output};
use serde_json::value::RawValue;

/// A tally of counts for the partition run so far.
#[derive(Default)]
pub(crate) struct Tally {
    /// What the client asked to be sent back beyond the counts.
    return_stats: bool,
    return_outputs: bool,
    /// Whether a worker that returned nothing gets an `Output` of `null`.
    null_outputs: bool,
    /// What has finished and not gone out in a frame yet.
    succeeded: usize,
    outputs: Vec<Output>,
    invocation_stats: Vec<InvocationStats>,
    /// The first failure of the frame being built, which the client
    /// raises on.
    first_error: Option<String>,
    /// Totals for the partition, which the summary reports.
    retried: usize,
    retry_wait_ms: f64,
}

impl Tally {
    pub(crate) fn new(return_stats: bool, return_outputs: bool, null_outputs: bool) -> Self {
        Self {
            return_stats,
            return_outputs,
            null_outputs,
            ..Self::default()
        }
    }

    /// Everything that has finished since the last call.
    pub(crate) fn take_completed(&mut self) -> Completed {
        Completed {
            succeeded: std::mem::take(&mut self.succeeded),
            outputs: std::mem::take(&mut self.outputs),
            invocation_stats: std::mem::take(&mut self.invocation_stats),
            first_error: std::mem::take(&mut self.first_error),
            ..Completed::default()
        }
    }

    /// Fold in one finished invocation.
    ///
    /// `index` is where its input sat in the partition.
    pub(crate) fn observe(&mut self, index: usize, invocation: Invocation) {
        let Invocation {
            outcome,
            retried,
            invoke_start,
            invoke_end,
            waited_ms,
        } = invocation;
        self.retried += retried;
        self.retry_wait_ms += waited_ms;
        match outcome {
            Outcome::Ok { req_id, output } => {
                self.succeeded += 1;
                if self.return_outputs {
                    let value = output.or_else(|| self.null_outputs.then(null));
                    if let Some(value) = value {
                        self.outputs.push(Output { index, value });
                    }
                }
                if self.return_stats {
                    self.invocation_stats.push(InvocationStats {
                        index,
                        req_id,
                        invoke_start,
                        invoke_end,
                    });
                }
            }
            // The first *invocation* to fail; each reports its last
            // attempt.
            Outcome::WorkerFailed(message)
            | Outcome::InvokeFailed(message)
            | Outcome::Rejected(message) => {
                self.first_error.get_or_insert(message);
            }
        }
    }

    pub(crate) fn retried(&self) -> usize {
        self.retried
    }

    pub(crate) fn retry_wait_ms(&self) -> f64 {
        self.retry_wait_ms
    }
}

fn null() -> Box<RawValue> {
    RawValue::from_string("null".to_string()).expect("null is JSON")
}

#[cfg(test)]
mod tests {
    use super::{Invocation, Outcome, Tally};
    use serde_json::json;

    fn ok(req_id: &str) -> Outcome {
        Outcome::Ok {
            req_id: req_id.to_string(),
            output: Some(serde_json::value::to_raw_value(&json!({"copied": 1})).expect("json")),
        }
    }

    fn silent() -> Outcome {
        Outcome::Ok {
            req_id: "w-0".to_string(),
            output: None,
        }
    }

    fn invocation(
        outcome: Outcome,
        retried: usize,
        invoke_start: f64,
        invoke_end: f64,
    ) -> Invocation {
        Invocation {
            outcome,
            retried,
            invoke_start,
            invoke_end,
            waited_ms: 0.0,
        }
    }

    fn waiting(outcome: Outcome, waited_ms: f64) -> Invocation {
        Invocation {
            waited_ms,
            ..invocation(outcome, 1, 1.0, 2.0)
        }
    }

    /// A tally asked for everything, which is what a `map` asks for.
    fn returning_everything() -> Tally {
        Tally::new(true, true, false)
    }

    fn observe(tally: &mut Tally, outcome: Outcome, attempts: usize) {
        tally.observe(0, invocation(outcome, attempts, 1.0, 2.0));
    }

    #[test]
    fn retries_are_summed_across_every_invocation_including_ones_that_succeeded() {
        let mut tally = returning_everything();
        observe(&mut tally, ok("a"), 0);
        observe(&mut tally, ok("b"), 2);
        observe(&mut tally, Outcome::InvokeFailed("gone".into()), 2);
        assert_eq!(tally.retried, 4);
        assert_eq!(tally.succeeded, 2);
    }

    /// Backoff otherwise turns a throttled run into a slow one with
    /// nothing in the record saying why.
    #[test]
    fn what_the_retries_waited_is_summed_across_the_partition() {
        let mut tally = returning_everything();
        tally.observe(0, waiting(ok("a"), 250.0));
        tally.observe(1, waiting(ok("b"), 750.0));
        tally.observe(2, waiting(Outcome::InvokeFailed("gone".into()), 1000.0));
        assert_eq!(tally.retry_wait_ms, 2000.0);
    }

    #[test]
    fn a_failure_of_any_kind_is_reported_and_not_counted_as_done() {
        for failure in [
            Outcome::WorkerFailed("no".into()),
            Outcome::InvokeFailed("no".into()),
            Outcome::Rejected("no".into()),
        ] {
            let mut tally = returning_everything();
            observe(&mut tally, failure, 0);
            let completed = tally.take_completed();
            assert_eq!(completed.first_error.as_deref(), Some("no"));
            assert_eq!(completed.succeeded, 0);
        }
    }

    /// The client sums the frames and checks the total against the
    /// inputs it sent, so what the frames account for must be exactly
    /// the partition - no double counting, and nothing left behind.
    #[test]
    fn the_frames_account_for_each_success_once() {
        let mut tally = returning_everything();
        observe(&mut tally, ok("a"), 0);
        let first = tally.take_completed();
        observe(&mut tally, ok("b"), 1);
        observe(&mut tally, Outcome::WorkerFailed("no".into()), 0);
        let second = tally.take_completed();

        assert_eq!((first.succeeded, second.succeeded), (1, 1));
        assert_eq!(second.first_error.as_deref(), Some("no"));
    }

    /// The client raises on the frame rather than waiting for the
    /// summary, so the frame has to say what went wrong.
    #[test]
    fn a_frame_carries_the_first_failure_it_counts() {
        let mut tally = returning_everything();
        observe(&mut tally, ok("a"), 0);
        assert_eq!(tally.take_completed().first_error, None);

        observe(&mut tally, Outcome::WorkerFailed("first no".into()), 1);
        observe(&mut tally, Outcome::InvokeFailed("second no".into()), 2);
        let losing = tally.take_completed();
        assert_eq!(losing.first_error.as_deref(), Some("first no"));

        // Taken with the frame, so a later one does not repeat it.
        assert_eq!(tally.take_completed().first_error, None);
    }

    /// A tick with nothing new on it is not sent, so the only frame a
    /// quiet partition costs is the one opening the stream.
    #[test]
    fn a_tick_with_nothing_new_is_empty() {
        let mut tally = returning_everything();
        assert!(tally.take_completed().is_empty());
        observe(&mut tally, ok("a"), 0);
        assert!(!tally.take_completed().is_empty());
        assert!(tally.take_completed().is_empty());
    }

    #[test]
    fn turning_stats_off_keeps_the_counts() {
        let mut tally = Tally {
            return_stats: false,
            ..returning_everything()
        };
        tally.observe(0, invocation(ok("a"), 1, 1.0, 2.0));
        let completed = tally.take_completed();
        assert!(completed.invocation_stats.is_empty());
        assert_eq!(completed.succeeded, 1);
        assert_eq!(tally.retried, 1);
    }

    /// What a `for_each` asks for: the work done and nothing streamed
    /// back to be dropped on arrival.
    #[test]
    fn turning_outputs_off_keeps_the_counts() {
        let mut tally = Tally {
            return_outputs: false,
            ..returning_everything()
        };
        tally.observe(0, invocation(ok("a"), 0, 1.0, 2.0));
        let completed = tally.take_completed();
        assert!(completed.outputs.is_empty());
        assert_eq!(completed.succeeded, 1);
        assert_eq!(completed.invocation_stats.len(), 1);
    }

    /// A null output gets no entry on the wire.
    #[test]
    fn a_worker_that_returned_nothing_has_no_output() {
        let mut tally = returning_everything();
        observe(&mut tally, silent(), 0);
        let completed = tally.take_completed();
        assert!(completed.outputs.is_empty());
        assert_eq!(completed.succeeded, 1);
    }

    /// What a combiner needs, to cover every index that succeeded.
    #[test]
    fn asked_for_null_outputs_a_worker_that_returned_nothing_has_one() {
        let mut tally = Tally {
            null_outputs: true,
            ..returning_everything()
        };
        tally.observe(3, invocation(silent(), 0, 1.0, 2.0));
        let outputs = tally.take_completed().outputs;
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].index, 3);
        assert_eq!(outputs[0].value.get(), "null");
    }

    #[test]
    fn a_kept_stat_carries_the_workers_request_id_and_its_timing() {
        let mut tally = returning_everything();
        tally.observe(4, invocation(ok("w-9"), 0, 10.5, 12.5));
        let stats = tally.take_completed().invocation_stats;
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].index, 4);
        assert_eq!(stats[0].req_id, "w-9");
        assert_eq!(stats[0].invoke_start, 10.5);
        assert_eq!(stats[0].invoke_end, 12.5);
    }
}
