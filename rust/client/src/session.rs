//! A session with the deployed lambdas, and the calls made through it.

use crate::run::{dispatch, Call, Run};
use crate::transport::{Lambda, Transport};
use crate::Error;
use serde_json::{Map, Value};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// The name the dispatcher is deployed under, by default.
pub const DEFAULT_DISPATCHER_FUNCTION: &str = "dispatcher";

/// Worker invocations in flight per call, by default.
pub const DEFAULT_CONCURRENCY: usize = 100;

/// How the calls through a [`Lambdas`] run.
///
/// ```
/// let mut settings = map_lambda_client::Settings::default();
/// settings.concurrency = 1_000;
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Settings {
    /// The name the dispatcher is deployed under.
    pub dispatcher_function: String,
    /// How many dispatchers a call's inputs are partitioned across, or
    /// `None` for the fewest that respect Lambda's limits.
    pub dispatchers: Option<usize>,
    /// Worker invocations in flight across a whole call.
    pub concurrency: usize,
    /// Extra attempts after a *transport* failure, per invocation.
    pub retries: usize,
    /// Whether batches carry [`Batch::invocation_stats`](crate::Batch::invocation_stats).
    pub return_stats: bool,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            dispatcher_function: DEFAULT_DISPATCHER_FUNCTION.to_string(),
            dispatchers: None,
            concurrency: DEFAULT_CONCURRENCY,
            retries: 2,
            return_stats: false,
        }
    }
}

/// A session with the deployed lambdas, which numbers its calls.
pub struct Lambdas {
    transport: Arc<dyn Transport>,
    endpoint_url: Option<String>,
    settings: Settings,
    calls: AtomicUsize,
}

impl Lambdas {
    /// Real Lambda, or a local `cargo lambda watch` at `endpoint_url`.
    pub async fn connect(endpoint_url: Option<&str>) -> Self {
        let lambda = Lambda::connect(endpoint_url).await;
        Self::new(Arc::new(lambda), endpoint_url.map(str::to_string))
    }

    /// A session through any [`Transport`], with the dispatchers invoking
    /// workers at `endpoint_url`, or real Lambda for `None`.
    pub fn new(transport: Arc<dyn Transport>, endpoint_url: Option<String>) -> Self {
        Self {
            transport,
            endpoint_url,
            settings: Settings::default(),
            calls: AtomicUsize::new(0),
        }
    }

    /// The session, with `settings`.
    pub fn with_settings(mut self, settings: Settings) -> Self {
        self.settings = settings;
        self
    }

    /// The settings calls run with.
    pub fn settings(&self) -> &Settings {
        &self.settings
    }

    /// The settings calls run with, to change.
    pub fn settings_mut(&mut self) -> &mut Settings {
        &mut self.settings
    }

    /// Invoke `function` once per input, with `constant_args` and the
    /// input under `input_key`, streaming back batches of answers in the
    /// order they finished.
    ///
    /// Refuses, with nothing invoked, a call that could not work. Dropping
    /// the stream tears down the dispatchers still running.
    pub fn map_unordered(
        &self,
        function: &str,
        inputs: Vec<Value>,
        input_key: &str,
        constant_args: Map<String, Value>,
    ) -> Result<Run, Error> {
        self.start(function, inputs, input_key, constant_args, true)
    }

    /// [`map_unordered`](Self::map_unordered), with no answers sent back.
    pub fn for_each(
        &self,
        function: &str,
        inputs: Vec<Value>,
        input_key: &str,
        constant_args: Map<String, Value>,
    ) -> Result<Run, Error> {
        self.start(function, inputs, input_key, constant_args, false)
    }

    /// Invoke `function` once with `payload`, without waiting for it to
    /// run.
    pub fn invoke(
        &self,
        function: &str,
        payload: Vec<u8>,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'static {
        let transport = self.transport.clone();
        let function = function.to_string();
        async move { transport.invoke(&function, payload).await }
    }

    fn start(
        &self,
        function: &str,
        inputs: Vec<Value>,
        input_key: &str,
        constant_args: Map<String, Value>,
        return_outputs: bool,
    ) -> Result<Run, Error> {
        let settings = &self.settings;
        let call = Call {
            dispatcher_function: settings.dispatcher_function.clone(),
            target_function: function.to_string(),
            input_key: input_key.to_string(),
            template: constant_args,
            dispatchers: settings.dispatchers,
            concurrency: settings.concurrency,
            retries: settings.retries,
            endpoint_url: self.endpoint_url.clone(),
            return_outputs,
            return_stats: settings.return_stats,
            call: self.calls.fetch_add(1, Ordering::Relaxed),
        };
        dispatch(self.transport.clone(), call, inputs)
    }
}

#[cfg(test)]
mod tests {
    use super::{Lambdas, Settings};
    use crate::testing::Stub;
    use futures::executor::block_on;
    use futures::StreamExt;
    use serde_json::{json, Map, Value};
    use std::sync::Arc;

    /// A stub with `calls` dispatchers queued, each finishing one input.
    fn answering(calls: usize) -> Arc<Stub> {
        let stub = Arc::new(Stub::default());
        for _ in 0..calls {
            let body = stub.respond();
            let result = json!({"Result": {
                "req_id": "d-0", "dispatched": 1, "succeeded": 1, "function_errors": 0,
                "transport_errors": 0, "retried": 0, "retry_wait_ms": 0.0,
                "first_error": null, "cold": false, "connections": 1, "elapsed_ms": 1.0,
            }});
            let completed = json!({"Completed": {
                "done": 1, "outputs": [], "stats": [], "first_error": null,
            }});
            for frame in [completed, result] {
                body.unbounded_send(format!("{frame}\n").into_bytes())
                    .expect("open");
            }
        }
        stub
    }

    fn one_dispatcher() -> Settings {
        Settings {
            dispatchers: Some(1),
            concurrency: 4,
            ..Settings::default()
        }
    }

    /// The `call` each dispatcher record was stamped with.
    fn drain(lambdas: &Lambdas, for_each: bool) -> Vec<usize> {
        let inputs = vec![json!(0)];
        let mut run = if for_each {
            lambdas.for_each("worker", inputs, "i", Map::new())
        } else {
            lambdas.map_unordered("worker", inputs, "i", Map::new())
        }
        .expect("starts");
        block_on(async {
            let mut calls = Vec::new();
            while let Some(batch) = run.next().await {
                calls.extend(
                    batch
                        .expect("clean")
                        .dispatcher_stats
                        .iter()
                        .map(|r| r.call),
                );
            }
            calls
        })
    }

    fn requests(stub: &Stub) -> Vec<Value> {
        stub.requests()
            .iter()
            .map(|request| serde_json::from_slice(request).expect("json"))
            .collect()
    }

    #[test]
    fn a_session_numbers_its_calls_in_order() {
        let lambdas = Lambdas::new(answering(2), None).with_settings(one_dispatcher());

        assert_eq!(drain(&lambdas, false), [0]);
        assert_eq!(drain(&lambdas, true), [1]);
    }

    #[test]
    fn a_call_runs_with_the_sessions_settings() {
        let stub = answering(1);
        let mut settings = one_dispatcher();
        settings.retries = 5;
        settings.return_stats = true;
        let lambdas = Lambdas::new(stub.clone(), Some("http://localhost:9000".into()))
            .with_settings(settings);

        drain(&lambdas, false);

        let request = &requests(&stub)[0];
        assert_eq!(request["concurrency"], 4);
        assert_eq!(request["retries"], 5);
        assert_eq!(request["return_stats"], true);
        assert_eq!(request["endpoint_url"], "http://localhost:9000");
    }

    /// Nothing streamed back to be dropped on arrival.
    #[test]
    fn for_each_asks_for_no_answers() {
        let stub = answering(2);
        let lambdas = Lambdas::new(stub.clone(), None).with_settings(one_dispatcher());

        drain(&lambdas, false);
        drain(&lambdas, true);

        let asked: Vec<_> = requests(&stub)
            .iter()
            .map(|r| r["return_outputs"].clone())
            .collect();
        assert_eq!(asked, [json!(true), json!(false)]);
    }
}
