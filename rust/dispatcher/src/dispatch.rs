//! The dispatch loop: a partition of inputs turned into invocations, and
//! counted.
//!
//! Knows nothing about response streaming: reports go out through
//! `ReportCallback`, so a test can record the frames instead.

use crate::invoke::{invoke_with_retries, payload_for, Outcome};
use crate::protocol::{connections_for, Completed, DispatcherStats, Request};
use crate::tally::{Return, Tally};
use aws_sdk_lambda::Client as LambdaClient;
use futures::stream::StreamExt;
use lambda_runtime::Error;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::time::MissedTickBehavior;

/// How often a partition reports what has finished since the last time.
const REPORT_INTERVAL: Duration = Duration::from_millis(100);

/// Somewhere to report what a partition has finished, as it runs.
///
/// An `Err` means the report cannot be delivered, and the partition stops.
pub(crate) trait ReportCallback {
    async fn completed(&mut self, completed: Completed) -> Result<(), Error>;
}

/// The connection with the fewest invocations in flight, the earliest of
/// any tied.
///
/// Never above an even share of `max_in_flight`, which `connections_for`
/// keeps within `STREAMS_PER_CONNECTION`. Dealing inputs out by position
/// would not be: the slow ones can land together and stay there.
fn least_busy(in_flight: &[AtomicUsize]) -> usize {
    (0..in_flight.len())
        .min_by_key(|&connection| in_flight[connection].load(Ordering::Relaxed))
        .expect("at least one connection")
}

/// Run one partition: invoke `worker_function` once per input, with
/// `max_in_flight` in flight, and report what became of them.
pub(crate) async fn dispatch<S: ReportCallback>(
    request: Request,
    req_id: String,
    lambda_clients: &[LambdaClient],
    cold: Option<bool>,
    callback: &mut S,
) -> Result<DispatcherStats, Error> {
    let Request {
        worker_function,
        constant_args,
        inputs,
        input_key,
        max_in_flight,
        retries,
        return_stats,
        return_outputs,
        // Already spent: the caller built the Lambda clients from it.
        endpoint_url: _,
    } = request;

    // A local run has one Lambda client.
    let connections = connections_for(max_in_flight).min(lambda_clients.len());
    let dispatched = inputs.len();

    let mut tally = Tally::returning(Return {
        stats: return_stats,
        outputs: return_outputs,
    });

    // Empty, but before any work, so "slow" and "never started" look
    // different from the client.
    callback.completed(tally.take_completed()).await?;

    let started = Instant::now();
    // Atomic because the handler's spawned task has to be `Send`.
    let in_flight: Vec<AtomicUsize> = (0..connections).map(|_| AtomicUsize::new(0)).collect();
    let in_flight = in_flight.as_slice();
    // The call can no longer succeed, and the client going away will not
    // reach this dispatcher.
    let failed = AtomicBool::new(false);
    let failed = &failed;
    // The invocations, started only as the loop below polls them.
    let mut results = futures::stream::iter(inputs.into_iter().enumerate())
        .take_while(|_| std::future::ready(!failed.load(Ordering::Relaxed)))
        .map(|(i, input)| {
            let body = payload_for(&constant_args, &input_key, input);
            let connection = least_busy(in_flight);
            in_flight[connection].fetch_add(1, Ordering::Relaxed);
            let client = &lambda_clients[connection];
            let worker: &str = &worker_function;
            async move {
                let invocation = invoke_with_retries(client, worker, body, retries).await;
                in_flight[connection].fetch_sub(1, Ordering::Relaxed);
                if !matches!(invocation.outcome, Outcome::Ok { .. }) {
                    failed.store(true, Ordering::Relaxed);
                }
                // `buffer_unordered` yields in completion order.
                (i, invocation)
            }
        })
        .buffer_unordered(max_in_flight);

    let mut report = tokio::time::interval_at(
        tokio::time::Instant::now() + REPORT_INTERVAL,
        REPORT_INTERVAL,
    );
    report.set_missed_tick_behavior(MissedTickBehavior::Delay);

    // Each invocation as it finishes, reporting to the callback on a timer.
    loop {
        tokio::select! {
            finished = results.next() => match finished {
                Some((index, invocation)) => tally.observe(index, invocation),
                None => break,
            },
            _ = report.tick() => {
                let batch = tally.take_completed();
                if !batch.is_empty() {
                    callback.completed(batch).await?;
                }
            }
        }
    }

    // Whatever the last tick missed.
    let batch = tally.take_completed();
    if !batch.is_empty() {
        callback.completed(batch).await?;
    }

    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    Ok(tally.into_stats(req_id, dispatched, cold, connections, elapsed_ms))
}

#[cfg(test)]
mod tests {
    use super::{dispatch, Completed, ReportCallback};
    use crate::protocol::{Request, STREAMS_PER_CONNECTION};
    use aws_config::retry::RetryConfig;
    use aws_credential_types::Credentials;
    use aws_sdk_lambda::config::Region;
    use aws_sdk_lambda::Client as LambdaClient;
    use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
    use aws_smithy_runtime_api::client::http::{
        HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpConnector,
    };
    use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
    use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
    use aws_smithy_runtime_api::shared::IntoShared;
    use aws_smithy_types::body::SdkBody;
    use lambda_runtime::Error;
    use serde_json::json;
    use serde_json::Value;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::Instant;

    /// A callback that keeps what it was told instead of writing it.
    #[derive(Default)]
    struct Recorder {
        frames: Vec<Completed>,
    }

    impl Recorder {
        /// The outputs across every frame, which is where a client
        /// finds them: nothing repeats them at the end.
        fn outputs(&self) -> Vec<Value> {
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

    fn replaying(responses: Vec<http::Response<SdkBody>>) -> Vec<LambdaClient> {
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
        vec![LambdaClient::from_conf(config)]
    }

    /// Lambda with a first input that returns at once and the rest after
    /// `SLOW_WORKER`, which a test with a paused clock doesn't wait for.
    #[derive(Debug, Clone)]
    struct FirstInputIsFast {
        fast: fn() -> http::Response<SdkBody>,
    }

    const SLOW_WORKER: Duration = Duration::from_secs(60);

    impl HttpConnector for FirstInputIsFast {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            let first = request
                .body()
                .bytes()
                .is_some_and(|body| body.windows(7).any(|window| window == b"[[0,0]]"));
            let fast = self.fast;
            HttpConnectorFuture::new(async move {
                let response = if first {
                    fast()
                } else {
                    tokio::time::sleep(SLOW_WORKER).await;
                    worker_ok()
                };
                Ok(HttpResponse::try_from(response).expect("a response"))
            })
        }
    }

    impl HttpClient for FirstInputIsFast {
        fn http_connector(
            &self,
            _: &HttpConnectorSettings,
            _: &RuntimeComponents,
        ) -> SharedHttpConnector {
            self.clone().into_shared()
        }
    }

    fn first_input_is_fast(fast: fn() -> http::Response<SdkBody>) -> Vec<LambdaClient> {
        vec![LambdaClient::from_conf(
            aws_sdk_lambda::Config::builder()
                .behavior_version(aws_sdk_lambda::config::BehaviorVersion::latest())
                .region(Region::new("eu-west-1"))
                .credentials_provider(Credentials::new("test", "test", None, None, "test"))
                .retry_config(RetryConfig::disabled())
                .http_client(FirstInputIsFast { fast })
                .build(),
        )]
    }

    /// Lambda with a slow worker for even inputs and a fast one for odd,
    /// counting the invocations in flight on this connection.
    #[derive(Debug, Clone, Default)]
    struct CountingStreams {
        in_flight: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
    }

    impl HttpConnector for CountingStreams {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            let body: Value = serde_json::from_slice(request.body().bytes().expect("a body"))
                .expect("a worker payload");
            let slow = body["coords"][0][0]
                .as_u64()
                .expect("an input")
                .is_multiple_of(2);
            let (in_flight, peak) = (self.in_flight.clone(), self.peak.clone());
            HttpConnectorFuture::new(async move {
                let now = in_flight.fetch_add(1, Ordering::Relaxed) + 1;
                peak.fetch_max(now, Ordering::Relaxed);
                tokio::time::sleep(if slow {
                    SLOW_WORKER
                } else {
                    Duration::from_millis(1)
                })
                .await;
                in_flight.fetch_sub(1, Ordering::Relaxed);
                Ok(HttpResponse::try_from(worker_ok()).expect("a response"))
            })
        }
    }

    impl HttpClient for CountingStreams {
        fn http_connector(
            &self,
            _: &HttpConnectorSettings,
            _: &RuntimeComponents,
        ) -> SharedHttpConnector {
            self.clone().into_shared()
        }
    }

    fn counting(connection: &CountingStreams) -> LambdaClient {
        LambdaClient::from_conf(
            aws_sdk_lambda::Config::builder()
                .behavior_version(aws_sdk_lambda::config::BehaviorVersion::latest())
                .region(Region::new("eu-west-1"))
                .credentials_provider(Credentials::new("test", "test", None, None, "test"))
                .retry_config(RetryConfig::disabled())
                .http_client(connection.clone())
                .build(),
        )
    }

    /// When each frame went out, against a paused clock.
    struct Timed {
        started: Instant,
        frames: Vec<(Duration, Completed)>,
    }

    impl Timed {
        fn new() -> Self {
            Self {
                started: Instant::now(),
                frames: Vec::new(),
            }
        }
    }

    impl ReportCallback for Timed {
        async fn completed(&mut self, completed: Completed) -> Result<(), Error> {
            self.frames.push((self.started.elapsed(), completed));
            Ok(())
        }
    }

    fn a_request(inputs: usize) -> Request {
        serde_json::from_value(json!({
            "worker_function": "echo",
            "constant_args": {"store": "s3://bucket/data"},
            "inputs": (0..inputs).map(|i| json!([[i, 0]])).collect::<Vec<_>>(),
            "input_key": "coords",
            "max_in_flight": 2,
            "retries": 0,
            "return_stats": true,
            "return_outputs": true,
        }))
        .expect("a request")
    }

    /// A worker with nothing to say costs nothing on the wire: no
    /// output frames at all, not empty ones and not a labelled entry
    /// each saying null.
    #[tokio::test]
    async fn a_worker_that_returns_nothing_sends_no_frames() {
        let lambda_clients = replaying(vec![worker_silent(), worker_silent(), worker_silent()]);
        let mut callback = Recorder::default();

        dispatch(
            a_request(3),
            "c-0".into(),
            &lambda_clients,
            Some(false),
            &mut callback,
        )
        .await
        .expect("dispatched");

        assert!(callback.outputs().is_empty(), "{:?}", callback.outputs());
    }

    /// Silence is not failure: the partition still ran, and the summary
    /// frame says so. Nothing downstream should read an empty `outputs`
    /// as work that did not happen - `check` reads the counts.
    #[tokio::test]
    async fn a_silent_worker_still_counts_as_succeeded() {
        let lambda_clients = replaying(vec![worker_silent(), worker_silent()]);
        let mut callback = Recorder::default();

        let stats = dispatch(
            a_request(2),
            "c-0".into(),
            &lambda_clients,
            Some(false),
            &mut callback,
        )
        .await
        .expect("dispatched");

        assert_eq!(stats.succeeded, 2);
    }

    /// The worker's own body, verbatim, against the position of the
    /// input it came from - which is all the client needs to line outputs
    /// up, and all that identifies one.
    #[tokio::test]
    async fn outputs_carry_the_workers_body_against_their_inputs_position() {
        let lambda_clients = replaying(vec![worker_ok(), worker_ok(), worker_ok()]);
        let mut callback = Recorder::default();

        dispatch(
            a_request(3),
            "c-0".into(),
            &lambda_clients,
            Some(false),
            &mut callback,
        )
        .await
        .expect("dispatched");

        let outputs = callback.outputs();
        assert_eq!(outputs.len(), 3);
        let mut positions: Vec<u64> = outputs
            .iter()
            .map(|a| a["index"].as_u64().expect("an index"))
            .collect();
        positions.sort();
        assert_eq!(positions, vec![0, 1, 2]);
        // no envelope added, and no copy of the input sent back
        assert!(
            outputs
                .iter()
                .all(|a| a["value"] == json!({"copied": 1}) && a.get("i").is_none()),
            "{outputs:?}"
        );
    }

    /// The last batch goes out even if no timer tick fell after it.
    /// Three invocations finish far inside one reporting interval, so
    /// without the final drain this reports nothing but the opening
    /// frame.
    #[tokio::test]
    async fn the_last_batch_is_sent_after_the_loop() {
        let lambda_clients = replaying(vec![worker_ok(), worker_ok(), worker_ok()]);
        let mut callback = Recorder::default();

        dispatch(
            a_request(3),
            "c-0".into(),
            &lambda_clients,
            Some(false),
            &mut callback,
        )
        .await
        .expect("dispatched");

        assert_eq!(callback.frames.len(), 2);
        assert_eq!(callback.frames[1].succeeded, 3);
        assert_eq!(callback.frames[1].outputs.len(), 3);
    }

    #[tokio::test]
    async fn every_input_becomes_an_invocation_and_is_counted() {
        let lambda_clients = replaying(vec![worker_ok(), worker_ok(), worker_ok()]);
        let mut callback = Recorder::default();

        let stats = dispatch(
            a_request(3),
            "dispatcher-0".into(),
            &lambda_clients,
            Some(true),
            &mut callback,
        )
        .await
        .expect("dispatched");

        let record = serde_json::to_value(&stats).expect("serializable");
        assert_eq!(record["dispatched"], 3);
        assert_eq!(record["succeeded"], 3);
        assert_eq!(record["req_id"], "dispatcher-0");
        assert_eq!(record["cold"], true);
    }

    /// So "slow" and "never started" do not look the same from the
    /// client.
    #[tokio::test]
    async fn an_empty_frame_is_sent_before_any_invocation_lands() {
        let lambda_clients = replaying(vec![worker_ok(), worker_ok()]);
        let mut callback = Recorder::default();

        dispatch(
            a_request(2),
            "dispatcher-0".into(),
            &lambda_clients,
            Some(false),
            &mut callback,
        )
        .await
        .expect("dispatched");

        let opening = callback.frames.first().expect("a frame");
        assert!(opening.is_empty());
    }

    /// A failed worker does not end the partition: the dispatcher reports
    /// it and waits for what is in flight, and the client is what raises.
    #[tokio::test]
    async fn a_failed_invocation_is_reported_rather_than_returned() {
        let lambda_clients = replaying(vec![worker_ok(), worker_failed()]);
        let mut callback = Recorder::default();

        let stats = dispatch(
            a_request(2),
            "dispatcher-0".into(),
            &lambda_clients,
            Some(false),
            &mut callback,
        )
        .await
        .expect("dispatched");

        assert_eq!(stats.succeeded, 1);
        let reported = callback
            .frames
            .iter()
            .find_map(|frame| frame.first_error.as_deref());
        assert!(reported.expect("a message").contains("boom"));
    }

    #[tokio::test]
    async fn a_failed_invocation_starts_no_more() {
        let lambda_clients = replaying(vec![worker_failed(), worker_ok(), worker_ok()]);
        let mut request = a_request(3);
        request.max_in_flight = 1;

        let stats = dispatch(
            request,
            "dispatcher-0".into(),
            &lambda_clients,
            Some(false),
            &mut Recorder::default(),
        )
        .await
        .expect("dispatched");

        assert_eq!(stats.succeeded, 0);
    }

    /// A failure can be the whole of a frame: nothing succeeded
    /// alongside it, and a failure carries neither an output nor a
    /// record. `is_empty` deciding there was nothing to send would drop
    /// it, and the client would go back to learning at the end.
    #[tokio::test]
    async fn a_frame_carrying_only_a_failure_is_still_sent() {
        let lambda_clients = replaying(vec![worker_failed()]);
        let mut callback = Recorder::default();

        dispatch(
            a_request(1),
            "dispatcher-0".into(),
            &lambda_clients,
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

    /// An output must not depend on a slower worker to carry it out: the
    /// dispatcher could die first, and the client would never see it.
    #[tokio::test(start_paused = true)]
    async fn an_output_goes_out_while_another_invocation_is_still_running() {
        let lambda_clients = first_input_is_fast(worker_ok);
        let mut callback = Timed::new();

        dispatch(
            a_request(2),
            "c-0".into(),
            &lambda_clients,
            Some(false),
            &mut callback,
        )
        .await
        .expect("dispatched");

        let (at, frame) = &callback.frames[1];
        assert_eq!(frame.outputs.len(), 1);
        assert!(*at < SLOW_WORKER, "the output waited until {at:?}");
    }

    /// The client stops a call on its first failure, so holding one back
    /// bills for all the work that follows.
    #[tokio::test(start_paused = true)]
    async fn a_failure_goes_out_while_another_invocation_is_still_running() {
        let lambda_clients = first_input_is_fast(worker_failed);
        let mut callback = Timed::new();

        let stats = dispatch(
            a_request(2),
            "c-0".into(),
            &lambda_clients,
            Some(false),
            &mut callback,
        )
        .await
        .expect("dispatched");

        let (at, frame) = &callback.frames[1];
        assert!(frame.first_error.is_some());
        assert!(*at < SLOW_WORKER, "the failure waited until {at:?}");
        assert_eq!(stats.succeeded, 1, "what was in flight still finishes");
    }

    /// Ticks with nothing to report cost nothing on the wire.
    #[tokio::test(start_paused = true)]
    async fn a_long_wait_with_nothing_new_sends_no_frames() {
        let lambda_clients = first_input_is_fast(worker_ok);
        let mut callback = Timed::new();

        dispatch(
            a_request(2),
            "c-0".into(),
            &lambda_clients,
            Some(false),
            &mut callback,
        )
        .await
        .expect("dispatched");

        // The opening frame, the fast worker and the slow one.
        assert_eq!(callback.frames.len(), 3);
    }

    /// Past `MAX_STREAMS_PER_CONNECTION` requests queue on their connection
    /// however idle the others are, so slow inputs must not pile up on one.
    #[tokio::test(start_paused = true)]
    async fn slow_invocations_do_not_pile_up_on_one_connection() {
        let connections = [CountingStreams::default(), CountingStreams::default()];
        let lambda_clients: Vec<LambdaClient> = connections.iter().map(counting).collect();
        let mut request = a_request(1_000);
        request.max_in_flight = 2 * STREAMS_PER_CONNECTION;

        dispatch(
            request,
            "c-0".into(),
            &lambda_clients,
            Some(false),
            &mut Recorder::default(),
        )
        .await
        .expect("dispatched");

        let peaks = connections
            .each_ref()
            .map(|c| c.peak.load(Ordering::Relaxed));
        assert!(
            peaks.iter().all(|peak| *peak <= STREAMS_PER_CONNECTION),
            "{peaks:?}"
        );
    }

    #[tokio::test]
    async fn a_callback_that_fails_stops_the_partition() {
        let lambda_clients = replaying(vec![worker_ok()]);

        let result = dispatch(
            a_request(1),
            "dispatcher-0".into(),
            &lambda_clients,
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
