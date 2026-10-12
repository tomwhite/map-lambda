//! The dispatch loop: a partition of inputs turned into invocations, and
//! counted.
//!
//! Knows nothing about response streaming: reports go out through
//! `ReportCallback`, so a test can record the frames instead.

use crate::clients::connections_for;
use crate::invoke::{invoke_with_retries, payload_for};
use crate::protocol::{Completed, Request, Response};
use crate::tally::{Return, Tally};
use aws_sdk_lambda::Client;
use futures::stream::StreamExt;
use lambda_runtime::Error;
use std::time::{Duration, Instant};

/// How often a partition reports what has finished since the last time.
const REPORT_INTERVAL: Duration = Duration::from_millis(100);

/// Somewhere to report what a partition has finished, as it runs.
///
/// An `Err` means there is no one left to report to, and the partition
/// stops.
pub(crate) trait ReportCallback {
    async fn completed(&mut self, completed: Completed) -> Result<(), Error>;
}

/// Run one partition: invoke `target_function` once per input group, with
/// `concurrency` in flight, and answer with what became of them.
pub(crate) async fn dispatch<S: ReportCallback>(
    request: Request,
    req_id: String,
    clients: &[Client],
    cold: Option<bool>,
    callback: &mut S,
) -> Result<Response, Error> {
    let Request {
        target_function,
        template,
        inputs,
        input_key,
        concurrency,
        retries,
        return_stats,
        return_outputs,
        // Already spent: the caller built the clients from it.
        endpoint_url: _,
    } = request;

    // A local run has one client.
    let connections = connections_for(concurrency).min(clients.len());
    let dispatched = inputs.len();

    let mut tally = Tally::returning(Return {
        stats: return_stats,
        outputs: return_outputs,
    });

    // Empty, but before any work, so "slow" and "never started" look
    // different from the client.
    callback.completed(tally.take_completed()).await?;

    let started = Instant::now();
    let mut last_report = Instant::now();
    // The invocations, started only as the loop below polls them.
    let mut results = futures::stream::iter(inputs.into_iter().enumerate())
        .map(|(i, input)| {
            let body = payload_for(&template, &input_key, input);
            let client = &clients[i % connections];
            let function_name: &str = &target_function;
            async move {
                let attempted = invoke_with_retries(client, function_name, body, retries).await;
                // `buffer_unordered` yields in completion order.
                (i, attempted)
            }
        })
        .buffer_unordered(concurrency);

    // Each invocation as it finishes, reporting to the callback on a timer.
    while let Some((index, attempted)) = results.next().await {
        tally.observe(index, attempted);
        if last_report.elapsed() >= REPORT_INTERVAL {
            callback.completed(tally.take_completed()).await?;
            last_report = Instant::now();
        }
    }

    // Whatever the last tick missed.
    let batch = tally.take_completed();
    if !batch.is_empty() {
        callback.completed(batch).await?;
    }

    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    Ok(tally.into_response(req_id, dispatched, cold, connections, elapsed_ms))
}

#[cfg(test)]
mod tests {
    use super::{dispatch, Completed, ReportCallback};
    use crate::protocol::Request;
    use aws_config::retry::RetryConfig;
    use aws_credential_types::Credentials;
    use aws_sdk_lambda::config::Region;
    use aws_sdk_lambda::Client;
    use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
    use aws_smithy_types::body::SdkBody;
    use lambda_runtime::Error;
    use serde_json::json;
    use serde_json::Value;

    /// A callback that keeps what it was told instead of writing it.
    #[derive(Default)]
    struct Recorder {
        frames: Vec<Completed>,
    }

    impl Recorder {
        /// The answers across every frame, which is where a client
        /// finds them: nothing repeats them at the end.
        fn answers(&self) -> Vec<Value> {
            self.frames
                .iter()
                .flat_map(|f| &f.outputs)
                .map(|o| serde_json::to_value(o).expect("serializable"))
                .collect()
        }
    }

    impl ReportCallback for Recorder {
        async fn completed(&mut self, completed: Completed) -> Result<(), Error> {
            self.frames.push(completed);
            Ok(())
        }
    }

    /// A callback with nobody on the other end.
    struct Gone;

    impl ReportCallback for Gone {
        async fn completed(&mut self, _: Completed) -> Result<(), Error> {
            Err(Error::from("the client has gone"))
        }
    }

    fn worker_ok() -> http::Response<SdkBody> {
        http::Response::builder()
            .status(200)
            .header("x-amzn-requestid", "w-1")
            .body(SdkBody::from(json!({"copied": 1}).to_string()))
            .expect("a response")
    }

    /// A worker that returns nothing: `Ok(())` in Rust serializes as
    /// `null`, which is also what an unparseable body comes through as.
    fn worker_silent() -> http::Response<SdkBody> {
        http::Response::builder()
            .status(200)
            .header("x-amzn-requestid", "w-1")
            .body(SdkBody::from("null"))
            .expect("a response")
    }

    fn worker_failed() -> http::Response<SdkBody> {
        http::Response::builder()
            .status(200)
            .header("x-amzn-requestid", "w-2")
            .header("X-Amz-Function-Error", "Unhandled")
            .body(SdkBody::from(
                json!({"errorType": "Error", "errorMessage": "boom"}).to_string(),
            ))
            .expect("a response")
    }

    fn replaying(responses: Vec<http::Response<SdkBody>>) -> Vec<Client> {
        let request = || {
            http::Request::builder()
                .uri("https://lambda.eu-west-1.amazonaws.com/2015-03-31/functions/w/invocations")
                .body(SdkBody::from("{}"))
                .expect("a request")
        };
        let http_client = StaticReplayClient::new(
            responses
                .into_iter()
                .map(|r| ReplayEvent::new(request(), r))
                .collect(),
        );
        let config = aws_sdk_lambda::Config::builder()
            .behavior_version(aws_sdk_lambda::config::BehaviorVersion::latest())
            .region(Region::new("eu-west-1"))
            .credentials_provider(Credentials::new("test", "test", None, None, "test"))
            .retry_config(RetryConfig::disabled())
            .http_client(http_client)
            .build();
        vec![Client::from_conf(config)]
    }

    fn a_request(inputs: usize) -> Request {
        serde_json::from_value(json!({
            "target_function": "echo",
            "template": {"store": "s3://bucket/data"},
            "inputs": (0..inputs).map(|i| json!([[i, 0]])).collect::<Vec<_>>(),
            "input_key": "coords",
            "concurrency": 2,
            "retries": 0,
        }))
        .expect("a request")
    }

    /// A worker with nothing to say costs nothing on the wire: no
    /// output frames at all, not empty ones and not a labelled entry
    /// each saying null.
    #[tokio::test]
    async fn a_worker_that_answers_nothing_sends_no_frames() {
        let clients = replaying(vec![worker_silent(), worker_silent(), worker_silent()]);
        let mut callback = Recorder::default();

        dispatch(
            a_request(3),
            "c-0".into(),
            &clients,
            Some(false),
            &mut callback,
        )
        .await
        .expect("dispatched");

        assert!(callback.answers().is_empty(), "{:?}", callback.answers());
    }

    /// Silence is not failure: the partition still ran, and the result
    /// frame says so. Nothing downstream should read an empty `outputs`
    /// as work that did not happen - `check` reads the counts.
    #[tokio::test]
    async fn a_silent_worker_still_counts_as_succeeded() {
        let clients = replaying(vec![worker_silent(), worker_silent()]);
        let mut callback = Recorder::default();

        let response = dispatch(
            a_request(2),
            "c-0".into(),
            &clients,
            Some(false),
            &mut callback,
        )
        .await
        .expect("dispatched");

        assert_eq!(response.succeeded, 2);
        assert_eq!(response.function_errors, 0);
    }

    /// The worker's own body, verbatim, against the position of the
    /// input it answers - which is all the client needs to line answers
    /// up, and all that identifies one.
    #[tokio::test]
    async fn answers_carry_the_workers_body_against_their_inputs_position() {
        let clients = replaying(vec![worker_ok(), worker_ok(), worker_ok()]);
        let mut callback = Recorder::default();

        dispatch(
            a_request(3),
            "c-0".into(),
            &clients,
            Some(false),
            &mut callback,
        )
        .await
        .expect("dispatched");

        let answers = callback.answers();
        assert_eq!(answers.len(), 3);
        let mut positions: Vec<u64> = answers
            .iter()
            .map(|a| a["index"].as_u64().expect("an index"))
            .collect();
        positions.sort();
        assert_eq!(positions, vec![0, 1, 2]);
        // no envelope added, and no copy of the input sent back
        assert!(
            answers
                .iter()
                .all(|a| a["response"] == json!({"copied": 1}) && a.get("i").is_none()),
            "{answers:?}"
        );
    }

    /// The last batch goes out even if no timer tick fell after it.
    /// Three invocations finish far inside one reporting interval, so
    /// without the final drain this reports nothing but the opening
    /// frame.
    #[tokio::test]
    async fn the_last_batch_is_sent_after_the_loop() {
        let clients = replaying(vec![worker_ok(), worker_ok(), worker_ok()]);
        let mut callback = Recorder::default();

        dispatch(
            a_request(3),
            "c-0".into(),
            &clients,
            Some(false),
            &mut callback,
        )
        .await
        .expect("dispatched");

        assert_eq!(callback.frames.len(), 2);
        assert_eq!(callback.frames[1].done, 3);
        assert_eq!(callback.frames[1].outputs.len(), 3);
    }

    #[tokio::test]
    async fn every_input_becomes_an_invocation_and_is_counted() {
        let clients = replaying(vec![worker_ok(), worker_ok(), worker_ok()]);
        let mut callback = Recorder::default();

        let response = dispatch(
            a_request(3),
            "coord-0".into(),
            &clients,
            Some(true),
            &mut callback,
        )
        .await
        .expect("dispatched");

        let record = serde_json::to_value(&response).expect("serializable");
        assert_eq!(record["dispatched"], 3);
        assert_eq!(record["succeeded"], 3);
        assert_eq!(record["req_id"], "coord-0");
        assert_eq!(record["cold"], true);
    }

    /// So "slow" and "never started" do not look the same from the
    /// client.
    #[tokio::test]
    async fn an_empty_frame_is_sent_before_any_invocation_lands() {
        let clients = replaying(vec![worker_ok(), worker_ok()]);
        let mut callback = Recorder::default();

        dispatch(
            a_request(2),
            "coord-0".into(),
            &clients,
            Some(false),
            &mut callback,
        )
        .await
        .expect("dispatched");

        let opening = callback.frames.first().expect("a frame");
        assert!(opening.is_empty());
    }

    /// A failed worker does not stop the partition: the coordinator
    /// counts it and dispatches on, and the client is what raises.
    #[tokio::test]
    async fn a_failed_invocation_is_counted_rather_than_returned() {
        let clients = replaying(vec![worker_ok(), worker_failed()]);
        let mut callback = Recorder::default();

        let response = dispatch(
            a_request(2),
            "coord-0".into(),
            &clients,
            Some(false),
            &mut callback,
        )
        .await
        .expect("dispatched");

        let record = serde_json::to_value(&response).expect("serializable");
        assert_eq!(record["succeeded"], 1);
        assert_eq!(record["function_errors"], 1);
        assert!(record["first_error"]
            .as_str()
            .expect("a message")
            .contains("boom"));
    }

    /// A failure can be the whole of a frame: nothing succeeded
    /// alongside it, and a failure carries neither an answer nor a
    /// record. `is_empty` deciding there was nothing to send would drop
    /// it, and the client would go back to learning at the end.
    #[tokio::test]
    async fn a_frame_carrying_only_a_failure_is_still_sent() {
        let clients = replaying(vec![worker_failed()]);
        let mut callback = Recorder::default();

        dispatch(
            a_request(1),
            "coord-0".into(),
            &clients,
            Some(false),
            &mut callback,
        )
        .await
        .expect("dispatched");

        // The opening frame, then the drain carrying the failure: one
        // invocation inside one interval, so no tick fell between them.
        assert_eq!(callback.frames.len(), 2);
        let reported = callback.frames[1].first_error.as_deref();
        assert!(reported.expect("a message").contains("boom"));
    }

    #[tokio::test]
    async fn a_callback_that_fails_stops_the_partition() {
        let clients = replaying(vec![worker_ok()]);

        let result = dispatch(
            a_request(1),
            "coord-0".into(),
            &clients,
            Some(false),
            &mut Gone,
        )
        .await;

        assert!(
            result.is_err(),
            "a partition nobody is listening to must stop"
        );
    }
}
