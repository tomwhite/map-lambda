//! One worker invocation: what to send, what came back, and how many
//! times to try.

use aws_sdk_lambda::operation::RequestId;
use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::Client;
use serde_json::{Map, Value};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// What became of one invocation.
#[derive(Debug)]
pub(crate) enum Outcome {
    Ok {
        req_id: String,
        /// The worker's whole response body. Null for a worker that
        /// returned nothing.
        body: Value,
    },
    /// Lambda ran the worker and the worker failed.
    Function(String),
    /// The request never reached a working worker.
    Transport(String),
}

/// An invocation Lambda ran without the error channel firing: a
/// success, whatever the body says.
///
/// A body that is not JSON comes through as null, the same as answering
/// nothing, rather than failing the invocation. `req_id` is the
/// response's `x-amzn-requestid`, as in `read_error_channel`.
pub(crate) fn read_body(req_id: &str, body: Option<&[u8]>) -> Outcome {
    Outcome::Ok {
        req_id: req_id.to_string(),
        body: body
            .and_then(|bytes| serde_json::from_slice(bytes).ok())
            .unwrap_or(Value::Null),
    }
}

/// What Lambda's error channel is saying.
///
/// `req_id` is the response's `x-amzn-requestid` header.
pub(crate) fn read_error_channel(
    function_error: &str,
    req_id: &str,
    payload: Option<&[u8]>,
) -> Outcome {
    let message = payload
        .and_then(|body| serde_json::from_slice::<Value>(body).ok())
        .and_then(|parsed| {
            parsed
                .get("errorMessage")
                .and_then(Value::as_str)
                .map(str::to_string)
        });
    Outcome::Function(match message {
        Some(message) => format!("{message} ({function_error} function error, req_id={req_id})"),
        None => format!("{function_error} (function error, req_id={req_id})"),
    })
}

/// The worker payload for one invocation: the template with this
/// group added under `input_key`.
pub(crate) fn payload_for(template: &Map<String, Value>, input_key: &str, input: Value) -> Vec<u8> {
    let mut body = template.clone();
    body.insert(input_key.to_string(), input);
    Value::Object(body).to_string().into_bytes()
}

fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after 1970")
        .as_secs_f64()
}

/// Every message in an error's source chain, joined by ": ".
///
/// An SDK error's outermost message is `service error` or `dispatch
/// failure`; what went wrong is two or three sources down.
///
/// Not `DisplayErrorContext`, which appends a Debug dump of the whole
/// error - headers, body and all - to a message a person reads.
fn error_chain(error: &dyn std::error::Error) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(error) = source {
        message.push_str(": ");
        message.push_str(&error.to_string());
        source = error.source();
    }
    message
}

/// Bounds on the wait between attempts at a transport failure. Full
/// jitter - the wait is sampled from the whole range rather than centred
/// on it - because a throttle refuses every invocation at once, and an
/// unspread retry arrives as the same herd.
const RETRY_BASE_MS: u64 = 500;
const RETRY_CAP_MS: u64 = 8_000;

fn backoff_ms(attempts_beyond_first: usize) -> u64 {
    let ceiling =
        (0..attempts_beyond_first).fold(RETRY_BASE_MS, |ms, _| (ms * 2).min(RETRY_CAP_MS));
    fastrand::u64(0..=ceiling)
}

/// What one invocation came to.
pub(crate) struct Attempted {
    pub(crate) outcome: Outcome,
    /// Attempts beyond the first.
    pub(crate) retried: usize,
    pub(crate) invoke_start: f64,
    pub(crate) invoke_end: f64,
    /// Waited between attempts, which the interval above includes.
    pub(crate) waited_ms: f64,
}

/// One invocation, with `retries` further attempts after a *transport*
/// failure.
pub(crate) async fn invoke_with_retries(
    client: &Client,
    function_name: &str,
    payload: Vec<u8>,
    retries: usize,
) -> Attempted {
    let start = now();
    let mut attempts_beyond_first = 0;
    let mut waited_ms = 0.0;
    loop {
        let outcome = match client
            .invoke()
            .function_name(function_name)
            .payload(Blob::new(payload.clone()))
            .send()
            .await
        {
            // A function error is an HTTP 200, not an `Err`.
            Ok(output) if output.function_error().is_some() => read_error_channel(
                output.function_error().unwrap_or_default(),
                output.request_id().unwrap_or_default(),
                output.payload().map(|blob| blob.as_ref()),
            ),
            Ok(output) => read_body(
                output.request_id().unwrap_or_default(),
                output.payload().map(|blob| blob.as_ref()),
            ),
            Err(e) => Outcome::Transport(error_chain(&e)),
        };
        if !matches!(outcome, Outcome::Transport(_)) || attempts_beyond_first >= retries {
            return Attempted {
                outcome,
                retried: attempts_beyond_first,
                invoke_start: start,
                invoke_end: now(),
                waited_ms,
            };
        }
        let wait = backoff_ms(attempts_beyond_first);
        waited_ms += wait as f64;
        tokio::time::sleep(Duration::from_millis(wait)).await;
        attempts_beyond_first += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::{
        backoff_ms, error_chain, invoke_with_retries, payload_for, read_body, read_error_channel,
        Client, Outcome, RETRY_BASE_MS, RETRY_CAP_MS,
    };
    use aws_config::retry::RetryConfig;
    use aws_credential_types::Credentials;
    use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
    use aws_smithy_types::body::SdkBody;
    use serde_json::{json, Map, Value};

    // ---- the retry loop ----
    //
    // A replay client that runs out of events answers `ConnectorError`, a
    // transport error, so "it stopped after exactly N attempts" is
    // assertable by counting the requests actually made.

    /// What Lambda returns for an invocation that ran and succeeded.
    fn worker_ok(req_id: &str) -> http::Response<SdkBody> {
        http::Response::builder()
            .status(200)
            .header("x-amzn-requestid", req_id)
            .body(SdkBody::from(json!({"written": 1}).to_string()))
            .expect("a valid response")
    }

    /// Lambda's error channel: a 200 with `X-Amz-Function-Error`, which
    /// is how *every* worker failure arrives.
    ///
    /// `x-amzn-requestid` is set because real Lambda sets it; without it
    /// the request id would silently be empty.
    fn worker_panicked(message: &str) -> http::Response<SdkBody> {
        let body = json!({"errorType": "Error", "errorMessage": message});
        http::Response::builder()
            .status(200)
            .header("X-Amz-Function-Error", "Unhandled")
            .header("x-amzn-requestid", "w-panic")
            .body(SdkBody::from(body.to_string()))
            .expect("a valid response")
    }

    /// The request never got a usable answer - the transport-error case
    /// a GOAWAY lands in.
    fn transport_failed() -> http::Response<SdkBody> {
        http::Response::builder()
            .status(500)
            .body(SdkBody::from("{}"))
            .expect("a valid response")
    }

    fn a_request() -> http::Request<SdkBody> {
        http::Request::builder()
            .uri("https://lambda.eu-west-1.amazonaws.com/2015-03-31/functions/echo/invocations")
            .body(SdkBody::from("{}"))
            .expect("a valid request")
    }

    /// A Lambda client that answers with `responses`, in order.
    ///
    /// `RetryConfig::disabled()` as in `clients()`: without it the SDK
    /// re-sends a 500 itself, eats the next scripted response and
    /// reports one attempt where three happened.
    fn replaying(responses: Vec<http::Response<SdkBody>>) -> (Client, StaticReplayClient) {
        let http_client = StaticReplayClient::new(
            responses
                .into_iter()
                .map(|r| ReplayEvent::new(a_request(), r))
                .collect(),
        );
        let client = Client::from_conf(
            aws_sdk_lambda::config::Builder::new()
                .http_client(http_client.clone())
                .credentials_provider(Credentials::for_tests())
                .region(aws_sdk_lambda::config::Region::new("eu-west-1"))
                .behavior_version(aws_sdk_lambda::config::BehaviorVersion::latest())
                .retry_config(RetryConfig::disabled())
                .build(),
        );
        (client, http_client)
    }

    fn attempts_made(http_client: &StaticReplayClient) -> usize {
        http_client.actual_requests().count()
    }

    async fn invoke(
        responses: Vec<http::Response<SdkBody>>,
        retries: usize,
    ) -> (Outcome, usize, usize) {
        let (client, http_client) = replaying(responses);
        let attempted = invoke_with_retries(&client, "echo", b"{}".to_vec(), retries).await;
        (
            attempted.outcome,
            attempted.retried,
            attempts_made(&http_client),
        )
    }

    // ---- the backoff ----

    #[test]
    fn the_wait_doubles_until_it_reaches_the_cap() {
        let most = |attempt| {
            (0..500)
                .map(|_| backoff_ms(attempt))
                .max()
                .expect("samples")
        };
        assert!(most(0) <= RETRY_BASE_MS);
        assert!(most(1) > RETRY_BASE_MS);
        assert!(most(20) <= RETRY_CAP_MS);
    }

    /// The whole point of the jitter: a fixed wait would bring every
    /// invocation back at the same instant, which is what throttled them.
    #[test]
    fn two_invocations_do_not_wait_the_same_time() {
        let waits: std::collections::HashSet<u64> = (0..50).map(|_| backoff_ms(2)).collect();
        assert!(waits.len() > 1, "{waits:?}");
    }

    /// `start_paused` advances the clock for the sleep, so this asserts
    /// the wiring without waiting for it.
    #[tokio::test(start_paused = true)]
    async fn a_transport_failure_waits_before_the_next_attempt() {
        let (client, http_client) = replaying(vec![
            transport_failed(),
            transport_failed(),
            worker_ok("w-1"),
        ]);
        let before = tokio::time::Instant::now();
        let attempted = invoke_with_retries(&client, "echo", b"{}".to_vec(), 2).await;

        assert!(matches!(attempted.outcome, Outcome::Ok { .. }));
        assert_eq!(attempts_made(&http_client), 3);
        // every millisecond that passed was one this chose to wait
        assert_eq!(before.elapsed().as_millis() as f64, attempted.waited_ms);
    }

    /// A worker that raised will raise again on the same payload, so the
    /// further attempts only bill for a run that is already lost.
    #[tokio::test(start_paused = true)]
    async fn a_function_error_is_not_retried() {
        let (client, _) = replaying(vec![
            worker_panicked("one"),
            worker_ok("never reached"),
            worker_ok("never reached"),
        ]);
        let before = tokio::time::Instant::now();
        let attempted = invoke_with_retries(&client, "echo", b"{}".to_vec(), 2).await;

        assert!(matches!(attempted.outcome, Outcome::Function(_)));
        assert_eq!(attempted.retried, 0);
        assert_eq!(attempted.waited_ms, 0.0);
        assert_eq!(before.elapsed().as_millis(), 0);
    }

    #[tokio::test]
    async fn a_first_attempt_success_is_not_retried() {
        let (outcome, retried, attempts) = invoke(vec![worker_ok("w-1")], 2).await;
        assert!(matches!(outcome, Outcome::Ok { ref req_id, .. } if req_id == "w-1"));
        assert_eq!(retried, 0);
        assert_eq!(attempts, 1, "a success must not be re-sent");
    }

    /// The branch a GOAWAY takes.
    #[tokio::test]
    async fn a_transport_failure_is_retried_and_recovers() {
        let (outcome, retried, attempts) =
            invoke(vec![transport_failed(), worker_ok("w-3")], 2).await;
        assert!(matches!(outcome, Outcome::Ok { .. }));
        assert_eq!(retried, 1);
        assert_eq!(attempts, 2);
    }

    #[tokio::test]
    async fn two_transport_failures_inside_the_budget_still_succeed() {
        let (outcome, retried, attempts) = invoke(
            vec![transport_failed(), transport_failed(), worker_ok("w-4")],
            2,
        )
        .await;
        assert!(matches!(outcome, Outcome::Ok { .. }));
        assert_eq!(retried, 2);
        assert_eq!(attempts, 3);
    }

    /// More failures are queued than the budget allows, so "it stopped"
    /// is the count of requests actually made rather than the absence of
    /// a further answer.
    #[tokio::test]
    async fn an_invocation_that_never_succeeds_stops_after_exactly_retries() {
        let (outcome, retried, attempts) = invoke(
            vec![
                transport_failed(),
                transport_failed(),
                transport_failed(),
                worker_ok("never reached"),
                worker_ok("never reached"),
            ],
            2,
        )
        .await;
        assert!(matches!(outcome, Outcome::Transport(_)));
        assert_eq!(retried, 2);
        assert_eq!(attempts, 3, "retries=2 is three attempts, not four");
    }

    #[tokio::test]
    async fn zero_retries_is_a_single_attempt() {
        let (outcome, retried, attempts) = invoke(
            vec![worker_panicked("only try"), worker_ok("never reached")],
            0,
        )
        .await;
        assert!(matches!(outcome, Outcome::Function(_)));
        assert_eq!(retried, 0);
        assert_eq!(attempts, 1);
    }

    /// Unlike `first_error`, which is the first across *invocations*,
    /// a single invocation reports its **last** attempt.
    #[tokio::test]
    async fn an_invocation_reports_its_last_attempt_not_its_first() {
        let (outcome, _, _) = invoke(
            vec![
                transport_failed(),
                transport_failed(),
                worker_panicked("the last"),
            ],
            2,
        )
        .await;
        match outcome {
            Outcome::Function(message) => assert!(
                message.contains("the last"),
                "expected the last attempt's message, got {message:?}"
            ),
            other => panic!("expected a function error, got {other:?}"),
        }
    }

    /// Lambda's error channel is an HTTP 200, so it is not an `Err` on
    /// the SDK call and has to be checked explicitly.
    #[tokio::test]
    async fn an_unhandled_worker_error_is_a_function_error() {
        let (outcome, retried, attempts) =
            invoke(vec![worker_panicked("boom"), worker_ok("never reached")], 2).await;
        assert!(matches!(outcome, Outcome::Function(_)));
        assert_eq!(retried, 0);
        assert_eq!(
            attempts, 1,
            "a worker that raised is not sent the same payload again"
        );
    }

    /// Through the real SDK client, since the id comes from a header.
    #[tokio::test]
    async fn an_error_channel_failure_carries_its_message_and_request_id() {
        let (outcome, _, _) = invoke(vec![worker_panicked("chunk 3 is short")], 0).await;
        match outcome {
            Outcome::Function(message) => {
                assert!(
                    message.contains("chunk 3 is short"),
                    "expected the runtime's errorMessage, got {message:?}"
                );
                assert!(
                    message.contains("req_id=w-panic"),
                    "expected the CloudWatch join key, got {message:?}"
                );
                assert!(
                    message.contains("Unhandled"),
                    "expected the error channel to be named, got {message:?}"
                );
            }
            other => panic!("expected a function error, got {other:?}"),
        }
    }

    #[test]
    fn an_error_channel_with_no_message_still_carries_its_request_id() {
        match read_error_channel("Unhandled", "w-7", Some(b"not json")) {
            Outcome::Function(message) => {
                assert_eq!(message, "Unhandled (function error, req_id=w-7)");
            }
            other => panic!("expected a function error, got {other:?}"),
        }
    }

    /// A nestable error, for building the shape an SDK error has.
    #[derive(Debug)]
    struct Layer(&'static str, Option<Box<Layer>>);

    impl std::fmt::Display for Layer {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.0)
        }
    }

    impl std::error::Error for Layer {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.1
                .as_deref()
                .map(|layer| layer as &dyn std::error::Error)
        }
    }

    #[test]
    fn an_error_reports_every_message_in_its_chain() {
        let error = Layer(
            "service error",
            Some(Box::new(Layer(
                "unhandled error (AccessDeniedException)",
                Some(Box::new(Layer(
                    "not authorized to perform: lambda:InvokeFunction",
                    None,
                ))),
            ))),
        );
        assert_eq!(
            error_chain(&error),
            "service error: unhandled error (AccessDeniedException): \
             not authorized to perform: lambda:InvokeFunction"
        );
    }

    #[test]
    fn an_error_message_is_not_followed_by_a_debug_dump_of_itself() {
        let error = Layer("boom", Some(Box::new(Layer("connection reset", None))));
        assert_eq!(error_chain(&error), "boom: connection reset");
    }

    /// The stats record covers every attempt, not just the one that
    /// worked, or retried invocations would look as quick as the rest.
    #[tokio::test]
    async fn the_timing_spans_every_attempt() {
        let (client, _) = replaying(vec![
            worker_panicked("one"),
            worker_panicked("two"),
            worker_ok("w-6"),
        ]);
        let before = super::now();
        let attempted = invoke_with_retries(&client, "echo", b"{}".to_vec(), 2).await;
        assert!(
            attempted.invoke_start >= before,
            "start is stamped before the first attempt"
        );
        assert!(attempted.invoke_end >= attempted.invoke_start);
    }

    fn a_template() -> Map<String, Value> {
        json!({
            "store": "s3://bucket/data",
            "tile_shape": [64, 721, 1440],
        })
        .as_object()
        .expect("an object")
        .clone()
    }

    #[test]
    fn the_template_is_passed_through_with_the_input_added() {
        let body: Value =
            serde_json::from_slice(&payload_for(&a_template(), "coord", json!([3, 0, 1])))
                .expect("valid json");

        assert_eq!(body["store"], "s3://bucket/data");
        assert_eq!(body["tile_shape"], json!([64, 721, 1440]));
        // verbatim: a coordinate, not a list of one
        assert_eq!(body["coord"], json!([3, 0, 1]));
    }

    #[test]
    fn the_key_and_the_inputs_shape_are_the_requests_to_choose() {
        let body: Value = serde_json::from_slice(&payload_for(
            &a_template(),
            "coords",
            json!([[0, 0], [0, 1], [1, 0]]),
        ))
        .expect("valid json");

        assert_eq!(body["coords"], json!([[0, 0], [0, 1], [1, 0]]));
        assert!(body.get("coord").is_none());
    }

    #[test]
    fn a_scalar_input_arrives_as_a_scalar() {
        let body: Value =
            serde_json::from_slice(&payload_for(&a_template(), "i", json!(7))).expect("valid json");

        assert_eq!(body["i"], json!(7));
    }

    #[test]
    fn a_success_takes_its_request_id_from_the_response() {
        match read_body("abc-123", Some(br#"{"written":1}"#)) {
            Outcome::Ok { req_id, body } => {
                assert_eq!(req_id, "abc-123");
                assert_eq!(body["written"], 1);
            }
            other => panic!("expected success, got {other:?}"),
        }
    }

    #[test]
    fn a_worker_that_answers_with_nothing_useful_still_succeeded() {
        for body in [None, Some(&b"not json"[..]), Some(&b""[..])] {
            match read_body("abc-123", body) {
                Outcome::Ok { req_id, body } => {
                    assert_eq!(req_id, "abc-123");
                    assert_eq!(body, Value::Null);
                }
                other => panic!("expected success, got {other:?}"),
            }
        }
    }
}
