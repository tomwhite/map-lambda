//! A session with the deployed lambdas, and the calls made through it.

use crate::call::{start, Batches, Call};
use crate::invoker::{connect, Invoker};
use crate::Error;
use serde_json::{Map, Value};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// The dispatcher's name, before a [`Settings::project`] prefix.
pub const DISPATCHER: &str = "dispatcher";

/// Worker invocations in flight per call, by default.
pub const DEFAULT_CONCURRENCY: usize = 100;

/// A session with the deployed lambdas, which numbers its calls.
pub struct Session {
    invoker: Arc<dyn Invoker>,
    endpoint_url: Option<String>,
    settings: Settings,
    calls: AtomicUsize,
}

impl Session {
    /// Real Lambda, or a local `cargo lambda watch` at `endpoint_url`.
    pub async fn connect(endpoint_url: Option<&str>) -> Self {
        Self::new(
            connect(endpoint_url).await,
            endpoint_url.map(str::to_string),
        )
    }

    /// A session through any [`Invoker`], with the dispatchers invoking
    /// workers at `endpoint_url`, or real Lambda for `None`.
    pub fn new(invoker: Arc<dyn Invoker>, endpoint_url: Option<String>) -> Self {
        Self {
            invoker,
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

    /// The settings calls run with, to change.
    pub fn settings_mut(&mut self) -> &mut Settings {
        &mut self.settings
    }

    /// Invoke `worker` once per input, with `constant_args` and the input
    /// under `input_key`, streaming back batches of outputs in the order
    /// they finished.
    ///
    /// Refuses, with nothing invoked, a call that could not work. Dropping
    /// the stream closes the dispatchers' streams, but Lambda does not tell
    /// them, so each still runs the rest of its partition.
    pub fn map_unordered(
        &self,
        worker: impl Into<Worker>,
        inputs: Vec<Value>,
        input_key: &str,
        constant_args: Map<String, Value>,
    ) -> Result<Batches, Error> {
        self.start(worker.into(), inputs, input_key, constant_args, true)
    }

    /// [`map_unordered`](Self::map_unordered), with no outputs sent back.
    pub fn for_each(
        &self,
        worker: impl Into<Worker>,
        inputs: Vec<Value>,
        input_key: &str,
        constant_args: Map<String, Value>,
    ) -> Result<Batches, Error> {
        self.start(worker.into(), inputs, input_key, constant_args, false)
    }

    /// Invoke `function` once with `payload`, without waiting for it to run.
    pub fn invoke_detached(
        &self,
        function: impl Into<Worker>,
        payload: Vec<u8>,
    ) -> impl Future<Output = Result<(), Error>> + Send + 'static {
        let invoker = self.invoker.clone();
        let function_name = self.function_name(&function.into());
        async move { invoker.invoke_detached(&function_name, payload).await }
    }

    fn function_name(&self, worker: &Worker) -> String {
        function_name(
            worker,
            self.settings.project.as_deref(),
            self.endpoint_url.is_some(),
        )
    }

    fn start(
        &self,
        worker: Worker,
        inputs: Vec<Value>,
        input_key: &str,
        constant_args: Map<String, Value>,
        return_outputs: bool,
    ) -> Result<Batches, Error> {
        let settings = &self.settings;
        let call = Call {
            dispatcher_function: self.function_name(&Worker::from(DISPATCHER)),
            worker_function: self.function_name(&worker),
            input_key: input_key.to_string(),
            constant_args,
            dispatchers: settings.dispatchers,
            concurrency: settings.concurrency,
            retries: settings.retries,
            endpoint_url: self.endpoint_url.clone(),
            return_outputs,
            return_stats: settings.return_stats,
            number: self.calls.fetch_add(1, Ordering::Relaxed),
        };
        start(self.invoker.clone(), call, inputs)
    }
}

/// The name `worker` is deployed under: prefixed with the project and
/// suffixed with the memory size on AWS, and plain when `local`, as `cargo
/// lambda watch` serves each function under its binary's name.
pub fn function_name(worker: &Worker, project: Option<&str>, local: bool) -> String {
    if local {
        return worker.name.clone();
    }
    let mut name = match project {
        Some(project) => format!("{project}-{}", worker.name),
        None => worker.name.clone(),
    };
    if let Some(memory) = worker.memory {
        name = format!("{name}-{memory}");
    }
    name
}

/// A function to call, by the name its project gave it, and optionally one
/// of the memory sizes it was deployed at.
///
/// ```
/// use map_lambda_client::Worker;
///
/// let default_size: Worker = "copy".into();
/// let bigger = Worker::new("copy").memory(3538);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Worker {
    name: String,
    memory: Option<u32>,
}

impl Worker {
    /// `name` at its default memory size.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            memory: None,
        }
    }

    /// The worker deployed at `memory` MB.
    pub fn memory(mut self, memory: u32) -> Self {
        self.memory = Some(memory);
        self
    }
}

impl From<&str> for Worker {
    fn from(name: &str) -> Self {
        Self::new(name)
    }
}

impl From<String> for Worker {
    fn from(name: String) -> Self {
        Self::new(name)
    }
}

impl From<&String> for Worker {
    fn from(name: &String) -> Self {
        Self::new(name.as_str())
    }
}

/// How the calls through a [`Session`] run.
///
/// ```
/// let mut settings = map_lambda_client::Settings::default();
/// settings.concurrency = 1_000;
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Settings {
    /// The project the functions were deployed as, which prefixes every
    /// name on AWS: the dispatcher becomes `<project>-dispatcher`, and a
    /// worker `copy` becomes `<project>-copy`. `None` uses the names as
    /// they are.
    pub project: Option<String>,
    /// How many dispatchers a call's inputs are partitioned across, or
    /// `None` for the fewest that respect Lambda's limits.
    pub dispatchers: Option<usize>,
    /// Worker invocations in flight across a whole call.
    pub concurrency: usize,
    /// Extra attempts after a failed invoke, per invocation.
    pub retries: usize,
    /// Whether batches carry [`Batch::invocation_stats`](crate::Batch::invocation_stats).
    pub return_stats: bool,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            project: None,
            dispatchers: None,
            concurrency: DEFAULT_CONCURRENCY,
            retries: 2,
            return_stats: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Session, Settings, Worker};
    use crate::testing::Stub;
    use crate::Error;
    use futures::executor::block_on;
    use futures::StreamExt;
    use serde_json::{json, Map, Value};
    use std::sync::Arc;

    /// A stub with `calls` dispatchers queued, each finishing one input.
    fn responding(calls: usize) -> Arc<Stub> {
        let stub = Arc::new(Stub::default());
        for _ in 0..calls {
            let body = stub.respond();
            let summary = json!({"Summary": {
                "req_id": "d-0", "dispatched": 1, "retried": 0,
                "retry_wait_ms": 0.0, "cold": false, "connections": 1, "elapsed_ms": 1.0,
            }});
            let completed = json!({"Completed": {
                "succeeded": 1, "outputs": [], "invocation_stats": [], "first_error": null,
            }});
            for frame in [completed, summary] {
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
    fn drain(session: &Session, for_each: bool) -> Vec<usize> {
        let inputs = vec![json!(0)];
        let mut batches = if for_each {
            session.for_each("worker", inputs, "i", Map::new())
        } else {
            session.map_unordered("worker", inputs, "i", Map::new())
        }
        .expect("starts");
        block_on(async {
            let mut calls = Vec::new();
            while let Some(batch) = batches.next().await {
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

    /// A stub with one dispatcher queued, sending `frames` as they are.
    fn sending(frames: &[Value]) -> Arc<Stub> {
        let stub = Arc::new(Stub::default());
        let body = stub.respond();
        for frame in frames {
            body.unbounded_send(format!("{frame}\n").into_bytes())
                .expect("open");
        }
        stub
    }

    fn summary(dispatched: usize) -> Value {
        json!({"Summary": {
            "req_id": "d-0", "dispatched": dispatched, "retried": 0,
            "retry_wait_ms": 0.0, "cold": false, "connections": 1, "elapsed_ms": 1.0,
        }})
    }

    fn requests(stub: &Stub) -> Vec<Value> {
        stub.requests()
            .iter()
            .map(|request| serde_json::from_slice(request).expect("json"))
            .collect()
    }

    #[test]
    fn a_session_numbers_its_calls_in_order() {
        let session = Session::new(responding(2), None).with_settings(one_dispatcher());

        assert_eq!(drain(&session, false), [0]);
        assert_eq!(drain(&session, true), [1]);
    }

    #[test]
    fn a_call_runs_with_the_sessions_settings() {
        let stub = responding(1);
        let mut settings = one_dispatcher();
        settings.retries = 5;
        settings.return_stats = true;
        let session = Session::new(stub.clone(), Some("http://localhost:9000".into()))
            .with_settings(settings);

        drain(&session, false);

        let request = &requests(&stub)[0];
        assert_eq!(request["max_in_flight"], 4);
        assert_eq!(request["retries"], 5);
        assert_eq!(request["return_stats"], true);
        assert_eq!(request["endpoint_url"], "http://localhost:9000");
    }

    fn in_project(project: Option<&str>) -> Settings {
        Settings {
            project: project.map(str::to_string),
            ..one_dispatcher()
        }
    }

    /// The function each dispatcher invocation went to, and the worker
    /// it was asked to invoke, for one call of `worker`.
    fn names(endpoint_url: Option<&str>, settings: Settings, worker: Worker) -> (String, String) {
        let stub = responding(1);
        let session =
            Session::new(stub.clone(), endpoint_url.map(str::to_string)).with_settings(settings);
        block_on(
            session
                .for_each(worker, vec![json!(0)], "i", Map::new())
                .expect("starts")
                .finish(),
        )
        .expect("clean");
        let worker = requests(&stub)[0]["worker_function"]
            .as_str()
            .expect("named")
            .to_string();
        (stub.dispatched_to().remove(0), worker)
    }

    #[test]
    fn without_a_project_names_are_used_as_they_are() {
        assert_eq!(
            names(None, in_project(None), "copy".into()),
            ("dispatcher".into(), "copy".into())
        );
    }

    #[test]
    fn a_project_prefixes_the_dispatcher_and_the_worker() {
        assert_eq!(
            names(None, in_project(Some("proj")), "copy".into()),
            ("proj-dispatcher".into(), "proj-copy".into())
        );
    }

    #[test]
    fn a_memory_size_suffixes_the_worker_alone() {
        assert_eq!(
            names(
                None,
                in_project(Some("proj")),
                Worker::new("copy").memory(3538)
            ),
            ("proj-dispatcher".into(), "proj-copy-3538".into())
        );
    }

    /// `cargo lambda watch` serves each function under its binary's name.
    #[test]
    fn a_local_run_uses_the_plain_names() {
        let local = Some("http://localhost:9000");
        assert_eq!(
            names(
                local,
                in_project(Some("proj")),
                Worker::new("copy").memory(3538)
            ),
            ("dispatcher".into(), "copy".into())
        );
    }

    #[test]
    fn a_detached_invocation_is_named_the_same_way() {
        let stub = Arc::new(Stub::default());
        let session = Session::new(stub.clone(), None).with_settings(in_project(Some("proj")));
        let name = String::from("copy");

        block_on(session.invoke_detached(&name, Vec::new())).expect("submitted");

        assert_eq!(stub.invoked()[0].0, "proj-copy");
    }

    /// Nothing streamed back to be dropped on arrival.
    #[test]
    fn for_each_asks_for_no_outputs() {
        let stub = responding(2);
        let session = Session::new(stub.clone(), None).with_settings(one_dispatcher());

        drain(&session, false);
        drain(&session, true);

        let asked: Vec<_> = requests(&stub)
            .iter()
            .map(|r| r["return_outputs"].clone())
            .collect();
        assert_eq!(asked, [json!(true), json!(false)]);
    }

    #[test]
    fn outputs_are_every_batchs_outputs_in_turn() {
        let stub = sending(&[
            json!({"Completed": {
                "succeeded": 1, "outputs": [{"index": 0, "value": "a"}],
                "invocation_stats": [], "first_error": null,
            }}),
            json!({"Completed": {
                "succeeded": 1, "outputs": [{"index": 1, "value": "b"}],
                "invocation_stats": [], "first_error": null,
            }}),
            summary(2),
        ]);
        let session = Session::new(stub, None).with_settings(one_dispatcher());
        let outputs = session
            .map_unordered("worker", vec![json!(0), json!(1)], "i", Map::new())
            .expect("starts")
            .outputs();

        let outputs: Vec<_> = block_on(outputs.collect::<Vec<_>>())
            .into_iter()
            .map(|output| {
                let (index, value) = output.expect("clean");
                (index, value.get().to_string())
            })
            .collect();
        assert_eq!(
            outputs,
            [(0, r#""a""#.to_string()), (1, r#""b""#.to_string())]
        );
    }

    #[test]
    fn finish_runs_a_clean_call_to_the_end() {
        let session = Session::new(responding(1), None).with_settings(one_dispatcher());
        let batches = session
            .for_each("worker", vec![json!(0)], "i", Map::new())
            .expect("starts");

        block_on(batches.finish()).expect("clean");
    }

    /// Draining is what checks a call, so `finish` has to report what it finds.
    #[test]
    fn finish_reports_a_failed_invocation() {
        let stub = sending(&[json!({"Completed": {
            "succeeded": 0, "outputs": [], "invocation_stats": [], "first_error": "boom",
        }})]);
        let session = Session::new(stub, None).with_settings(one_dispatcher());
        let batches = session
            .for_each("worker", vec![json!(0)], "i", Map::new())
            .expect("starts");

        let error = block_on(batches.finish()).expect_err("failed");
        assert!(
            matches!(error, Error::InvocationFailed(ref message) if message == "boom"),
            "{error}"
        );
    }
}
