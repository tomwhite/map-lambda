//! Folding a partition's finished invocations into the counts it is judged
//! on.

use crate::invoke::{Attempted, Outcome};
use crate::protocol::{Completed, Output, Response, Stat};

/// A tally of counts for the partition run so far.
#[derive(Default)]
pub(crate) struct Tally {
    /// What the client asked to be sent back.
    returning: Return,
    /// Answers and records that have not gone out in a frame yet.
    outputs: Vec<Output>,
    stats: Vec<Stat>,
    /// What the frames so far have accounted for.
    sent_done: usize,
    succeeded: usize,
    function_errors: usize,
    transport_errors: usize,
    retried: usize,
    retry_wait_ms: f64,
    first_error: Option<String>,
    /// The first error of the frame being built, which the client raises
    /// on. Separate from `first_error`, which keeps the run's first for
    /// the result and so cannot be taken away.
    frame_error: Option<String>,
}

/// What a request wants returned beyond the counts.
#[derive(Default, Clone, Copy)]
pub(crate) struct Return {
    pub(crate) stats: bool,
    pub(crate) outputs: bool,
}

impl Tally {
    pub(crate) fn returning(returning: Return) -> Self {
        Self {
            returning,
            ..Self::default()
        }
    }

    /// Keep a failure's message for the result and for the frame it
    /// falls in, which are taken away at different times.
    fn remember(&mut self, message: String) {
        self.frame_error.get_or_insert_with(|| message.clone());
        self.first_error.get_or_insert(message);
    }

    /// Everything that has finished since the last call.
    pub(crate) fn take_completed(&mut self) -> Completed {
        let completed = Completed {
            done: self.succeeded - self.sent_done,
            outputs: std::mem::take(&mut self.outputs),
            stats: std::mem::take(&mut self.stats),
            first_error: std::mem::take(&mut self.frame_error),
        };
        self.sent_done = self.succeeded;
        completed
    }

    /// Fold in one finished invocation.
    ///
    /// `index` is where its input sat in the partition.
    pub(crate) fn observe(&mut self, index: usize, attempted: Attempted) {
        let Attempted {
            outcome,
            retried,
            invoke_start,
            invoke_end,
            waited_ms,
        } = attempted;
        self.retried += retried;
        self.retry_wait_ms += waited_ms;
        match outcome {
            Outcome::Ok { req_id, body } => {
                self.succeeded += 1;
                // A null answer gets no entry on the wire.
                if self.returning.outputs && !body.is_null() {
                    self.outputs.push(Output {
                        index,
                        response: body,
                    });
                }
                if self.returning.stats {
                    self.stats.push(Stat {
                        index,
                        req_id,
                        invoke_start,
                        invoke_end,
                    });
                }
            }
            // The first *invocation* to fail; each reports its last
            // attempt.
            Outcome::Function(message) => {
                self.function_errors += 1;
                self.remember(message);
            }
            Outcome::Transport(message) => {
                self.transport_errors += 1;
                self.remember(message);
            }
        }
    }

    /// The finished partition, as the result frame reports it.
    ///
    pub(crate) fn into_response(
        self,
        req_id: String,
        dispatched: usize,
        cold: Option<bool>,
        connections: usize,
        elapsed_ms: f64,
    ) -> Response {
        Response {
            req_id,
            dispatched,
            succeeded: self.succeeded,
            function_errors: self.function_errors,
            transport_errors: self.transport_errors,
            retried: self.retried,
            retry_wait_ms: self.retry_wait_ms,
            first_error: self.first_error,
            cold,
            connections,
            elapsed_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Attempted, Outcome, Return, Tally};
    use serde_json::json;

    fn ok(req_id: &str) -> Outcome {
        Outcome::Ok {
            req_id: req_id.to_string(),
            body: json!({"copied": 1}),
        }
    }

    fn attempted(
        outcome: Outcome,
        retried: usize,
        invoke_start: f64,
        invoke_end: f64,
    ) -> Attempted {
        Attempted {
            outcome,
            retried,
            invoke_start,
            invoke_end,
            waited_ms: 0.0,
        }
    }

    fn waiting(outcome: Outcome, waited_ms: f64) -> Attempted {
        Attempted {
            waited_ms,
            ..attempted(outcome, 1, 1.0, 2.0)
        }
    }

    /// A tally asked for everything, which is what a `map` asks for.
    fn returning_everything() -> Tally {
        Tally::returning(Return {
            stats: true,
            outputs: true,
        })
    }

    fn observe(tally: &mut Tally, outcome: Outcome, attempts: usize) {
        tally.observe(0, attempted(outcome, attempts, 1.0, 2.0));
    }

    #[test]
    fn retries_are_summed_across_every_invocation_including_ones_that_succeeded() {
        let mut tally = returning_everything();
        observe(&mut tally, ok("a"), 0);
        observe(&mut tally, ok("b"), 2);
        observe(&mut tally, Outcome::Transport("gone".into()), 2);
        assert_eq!(tally.retried, 4);
        assert_eq!(tally.succeeded, 2);
        assert_eq!(tally.transport_errors, 1);
    }

    /// Backoff otherwise turns a throttled run into a slow one with
    /// nothing in the record saying why.
    #[test]
    fn what_the_retries_waited_is_summed_across_the_partition() {
        let mut tally = returning_everything();
        tally.observe(0, waiting(ok("a"), 250.0));
        tally.observe(1, waiting(ok("b"), 750.0));
        tally.observe(2, waiting(Outcome::Transport("gone".into()), 1000.0));
        assert_eq!(tally.retry_wait_ms, 2000.0);
    }

    #[test]
    fn the_two_failure_kinds_are_counted_separately() {
        let mut tally = returning_everything();
        observe(&mut tally, Outcome::Function("bad input".into()), 0);
        observe(&mut tally, Outcome::Transport("goaway".into()), 1);
        assert_eq!(tally.function_errors, 1);
        assert_eq!(tally.transport_errors, 1);
        assert_eq!(tally.succeeded, 0);
    }

    #[test]
    fn the_first_invocation_to_fail_is_the_one_reported() {
        let mut tally = returning_everything();
        observe(&mut tally, ok("a"), 0);
        observe(&mut tally, Outcome::Transport("the first".into()), 0);
        observe(&mut tally, Outcome::Function("the second".into()), 0);
        assert_eq!(tally.first_error.as_deref(), Some("the first"));
    }

    #[test]
    fn a_clean_partition_reports_no_error() {
        let mut tally = returning_everything();
        observe(&mut tally, ok("a"), 0);
        assert!(tally.first_error.is_none());
        assert_eq!((tally.function_errors, tally.transport_errors), (0, 0));
    }

    /// The client sums the frames and checks the total against the
    /// result, so what the frames account for must be exactly the
    /// partition - no double counting, and nothing left behind.
    #[test]
    fn the_frames_add_up_to_the_count_the_result_reports() {
        let mut tally = returning_everything();
        observe(&mut tally, ok("a"), 0);
        let first = tally.take_completed();
        observe(&mut tally, ok("b"), 1);
        observe(&mut tally, Outcome::Function("no".into()), 0);
        let second = tally.take_completed();

        assert_eq!((first.done, second.done), (1, 1));
        let response = tally.into_response("c-0".to_string(), 3, None, 1, 1.0);
        assert_eq!(first.done + second.done, response.succeeded);
        // A frame reports a failure as the message the client raises on;
        // the count of them is the result's.
        assert_eq!(second.first_error.as_deref(), Some("no"));
        assert_eq!(response.function_errors + response.transport_errors, 1);
    }

    /// The client raises on the frame rather than waiting for the
    /// result, so the frame has to say what went wrong.
    #[test]
    fn a_frame_carries_the_first_failure_it_counts() {
        let mut tally = returning_everything();
        observe(&mut tally, ok("a"), 0);
        assert_eq!(tally.take_completed().first_error, None);

        observe(&mut tally, Outcome::Function("first no".into()), 1);
        observe(&mut tally, Outcome::Transport("second no".into()), 2);
        let losing = tally.take_completed();
        assert_eq!(losing.first_error.as_deref(), Some("first no"));

        // Taken with the frame, so a later one does not repeat it - while
        // the result still reports the run's first.
        assert_eq!(tally.take_completed().first_error, None);
        let response = tally.into_response("c-0".to_string(), 3, None, 1, 1.0);
        assert_eq!(response.first_error.as_deref(), Some("first no"));
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
        let mut tally = Tally::returning(Return {
            stats: false,
            outputs: true,
        });
        tally.observe(0, attempted(ok("a"), 1, 1.0, 2.0));
        let completed = tally.take_completed();
        assert!(completed.stats.is_empty());
        assert_eq!(completed.done, 1);
        assert_eq!(tally.retried, 1);
    }

    /// What a `for_each` asks for: the work done and nothing streamed
    /// back to be dropped on arrival.
    #[test]
    fn turning_outputs_off_keeps_the_counts() {
        let mut tally = Tally::returning(Return {
            stats: true,
            outputs: false,
        });
        tally.observe(0, attempted(ok("a"), 0, 1.0, 2.0));
        let completed = tally.take_completed();
        assert!(completed.outputs.is_empty());
        assert_eq!(completed.done, 1);
        assert_eq!(completed.stats.len(), 1);
    }

    #[test]
    fn a_kept_stat_carries_the_workers_request_id_and_its_timing() {
        let mut tally = returning_everything();
        tally.observe(4, attempted(ok("w-9"), 0, 10.5, 12.5));
        let stats = tally.take_completed().stats;
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].index, 4);
        assert_eq!(stats[0].req_id, "w-9");
        assert_eq!(stats[0].invoke_start, 10.5);
        assert_eq!(stats[0].invoke_end, 12.5);
    }

    /// Where a field added to one and forgotten in the other shows up.
    #[test]
    fn the_response_carries_the_whole_tally() {
        let mut tally = returning_everything();
        observe(&mut tally, ok("w-1"), 1);
        observe(&mut tally, Outcome::Function("bad input".into()), 0);
        observe(&mut tally, Outcome::Transport("goaway".into()), 2);

        let response = tally.into_response("coord-0".to_string(), 3, Some(true), 4, 12.5);
        let record = serde_json::to_value(&response).expect("serializable");
        assert_eq!(record["req_id"], "coord-0");
        assert_eq!(record["dispatched"], 3);
        assert_eq!(record["succeeded"], 1);
        assert_eq!(record["function_errors"], 1);
        assert_eq!(record["transport_errors"], 1);
        assert_eq!(record["retried"], 3);
        assert_eq!(record["retry_wait_ms"], 0.0);
        assert_eq!(record["first_error"], "bad input");
        assert_eq!(record["cold"], true);
        assert_eq!(record["connections"], 4);
        assert_eq!(record["elapsed_ms"], 12.5);
        // Records ride on the frames; repeating them here would be a
        // second copy of the whole partition.
        assert!(record.get("stats").is_none(), "{record}");
    }
}
