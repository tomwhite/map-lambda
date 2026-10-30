//! `map_lambda._native`: the Rust client, as Python sees it.
//!
//! - `connect` - an `Invoker` for real Lambda or a local `cargo lambda watch`.
//! - `Stub` - a stand-in for Lambda, for tests.
//! - `Session` - a `map_lambda_client::Session` through either: its calls,
//!   as async iterators of `Batch`es, and `invoke_detached`.

use futures::channel::mpsc::UnboundedSender;
use futures::StreamExt;
use map_lambda_client::{
    testing, Error, DEFAULT_CONCURRENCY, DEFAULT_DISPATCHER_FUNCTION, LOCAL_ENDPOINT_URL,
};
use map_lambda_protocol::MAX_STREAMS_PER_CONNECTION;
use pyo3::exceptions::{PyRuntimeError, PyStopAsyncIteration, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use pyo3_async_runtimes::tokio::future_into_py;
use serde_json::{Map, Value};
use std::sync::{Arc, Mutex};

fn to_py(error: Error) -> PyErr {
    if error.is_refusal() {
        PyValueError::new_err(error.to_string())
    } else {
        PyRuntimeError::new_err(error.to_string())
    }
}

/// An invoker, for real Lambda or a local `cargo lambda watch`.
#[pyclass(frozen)]
struct Invoker(Arc<dyn map_lambda_client::Invoker>);

/// An `Invoker` with credentials and region from the environment, or for
/// the emulator at `endpoint_url`.
#[pyfunction]
#[pyo3(signature = (endpoint_url=None))]
fn connect(py: Python<'_>, endpoint_url: Option<String>) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async move {
        Ok(Invoker(
            map_lambda_client::connect(endpoint_url.as_deref()).await,
        ))
    })
}

/// A stand-in for Lambda: see `map_lambda_client::testing::Stub`.
///
/// Subclassable, and accepting any arguments, so a test's own subclass
/// can take its canned responses in `__init__`.
#[pyclass(frozen, subclass)]
struct Stub(Arc<testing::Stub>);

#[pymethods]
impl Stub {
    #[new]
    #[pyo3(signature = (*_args, **_kwargs))]
    fn new(_args: &Bound<'_, PyTuple>, _kwargs: Option<&Bound<'_, PyDict>>) -> Self {
        Self(Arc::default())
    }

    /// The body the next dispatcher invocation responds with.
    fn respond(&self) -> Body {
        Body(self.0.respond())
    }

    /// Every dispatcher request, as sent.
    #[getter]
    fn requests(&self) -> Vec<String> {
        self.0
            .requests()
            .iter()
            .map(|r| String::from_utf8_lossy(r).into())
            .collect()
    }

    /// Every `invoke_detached`, as `(function_name, payload)`.
    #[getter]
    fn invocations(&self) -> Vec<(String, String)> {
        self.0
            .invoked()
            .into_iter()
            .map(|(function_name, payload)| {
                (function_name, String::from_utf8_lossy(&payload).into())
            })
            .collect()
    }
}

/// One dispatcher's response body, written as the test goes.
#[pyclass(frozen)]
struct Body(UnboundedSender<Vec<u8>>);

#[pymethods]
impl Body {
    fn write(&self, chunk: Vec<u8>) -> PyResult<()> {
        self.0
            .unbounded_send(chunk)
            .map_err(|_| PyRuntimeError::new_err("the client has stopped reading this body"))
    }

    /// End the body, as a dispatcher's stream ends.
    fn close(&self) {
        self.0.close_channel();
    }

    /// Whether the client has stopped reading - or the test has closed it.
    #[getter]
    fn closed(&self) -> bool {
        self.0.is_closed()
    }
}

fn invoker_of(invoker: &Bound<'_, PyAny>) -> PyResult<Arc<dyn map_lambda_client::Invoker>> {
    if let Ok(invoker) = invoker.cast::<Invoker>() {
        return Ok(invoker.get().0.clone());
    }
    if let Ok(stub) = invoker.cast::<Stub>() {
        return Ok(stub.get().0.clone());
    }
    Err(PyTypeError::new_err(format!(
        "expected an Invoker or a Stub, not {}",
        invoker.get_type().name()?
    )))
}

/// What arrived since the last batch: see `map_lambda_client::Batch`.
#[pyclass(frozen, get_all)]
struct Batch {
    succeeded: usize,
    /// `(index, output)` pairs.
    outputs: Py<PyList>,
    invocation_stats: Py<PyList>,
    dispatcher_stats: Py<PyList>,
}

impl Batch {
    fn from_rust(py: Python<'_>, batch: map_lambda_client::Batch) -> PyResult<Self> {
        let outputs = PyList::empty(py);
        for (index, output) in batch.outputs {
            let output: Value = serde_json::from_str(output.get())
                .map_err(|e| PyValueError::new_err(format!("an output is not JSON: {e}")))?;
            outputs.append((index, pythonize::pythonize(py, &output)?))?;
        }
        Ok(Self {
            succeeded: batch.succeeded,
            outputs: outputs.unbind(),
            invocation_stats: records(py, &batch.invocation_stats)?,
            dispatcher_stats: records(py, &batch.dispatcher_stats)?,
        })
    }
}

fn records<T: serde::Serialize>(py: Python<'_>, records: &[T]) -> PyResult<Py<PyList>> {
    let list = PyList::empty(py);
    for record in records {
        list.append(pythonize::pythonize(py, record)?)?;
    }
    Ok(list.unbind())
}

/// One call's batches, as an async iterator.
#[pyclass(frozen)]
struct Batches {
    /// Taken by `__anext__` while it waits, and put back with a batch:
    /// a call that failed or finished is gone, and so is one whose
    /// `__anext__` was cancelled.
    batches: Arc<Mutex<Option<map_lambda_client::Batches>>>,
}

#[pymethods]
impl Batches {
    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let slot = self.batches.clone();
        let taken = slot.lock().expect("unpoisoned").take();
        future_into_py(py, async move {
            let Some(mut batches) = taken else {
                return Err(PyStopAsyncIteration::new_err(()));
            };
            match batches.next().await {
                None => Err(PyStopAsyncIteration::new_err(())),
                Some(Err(error)) => Err(to_py(error)),
                Some(Ok(batch)) => {
                    *slot.lock().expect("unpoisoned") = Some(batches);
                    Python::attach(|py| Batch::from_rust(py, batch))
                }
            }
        })
    }

    /// Tear down whatever dispatchers are still running.
    fn close(&self) {
        drop(self.batches.lock().expect("unpoisoned").take());
    }
}

fn json<T: serde::de::DeserializeOwned>(value: &Bound<'_, PyAny>, what: &str) -> PyResult<T> {
    let py = value.py();
    let text: String = py
        .import("json")?
        .call_method1("dumps", (value,))?
        .extract()?;
    serde_json::from_str(&text)
        .map_err(|e| PyValueError::new_err(format!("{what} are not valid JSON: {e}")))
}

/// A `map_lambda_client::Session` through an `Invoker` or a `Stub`.
///
/// Settings come with each call, since a Python `Session` can change them.
#[pyclass(frozen)]
struct Session(Mutex<map_lambda_client::Session>);

#[pymethods]
impl Session {
    #[new]
    #[pyo3(signature = (invoker, endpoint_url=None))]
    fn new(invoker: &Bound<'_, PyAny>, endpoint_url: Option<String>) -> PyResult<Self> {
        Ok(Self(Mutex::new(map_lambda_client::Session::new(
            invoker_of(invoker)?,
            endpoint_url,
        ))))
    }

    /// A `map_unordered` if `return_outputs`, else a `for_each`.
    ///
    /// Inputs and constant args go through `json.dumps`, so they are
    /// marshalled exactly as they always were.
    #[pyo3(signature = (
        worker_function, inputs, *, input_key, constant_args, dispatcher_function, dispatchers,
        concurrency, retries, return_outputs, return_stats,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn start(
        &self,
        worker_function: &str,
        inputs: &Bound<'_, PyAny>,
        input_key: &str,
        constant_args: &Bound<'_, PyAny>,
        dispatcher_function: String,
        dispatchers: Option<usize>,
        concurrency: usize,
        retries: usize,
        return_outputs: bool,
        return_stats: bool,
    ) -> PyResult<Batches> {
        // The dispatcher builds the worker's payload on it, so
        // anything else fails inside AWS with every invocation billed for.
        if !constant_args.is_instance_of::<PyDict>() {
            return Err(PyTypeError::new_err(format!(
                "constant_args must be a dict, not {}",
                constant_args.get_type().name()?
            )));
        }
        let constant_args: Map<String, Value> = json(constant_args, "constant_args")?;
        let inputs: Vec<Value> = json(inputs, "inputs")?;

        let mut session = self.0.lock().expect("unpoisoned");
        let settings = session.settings_mut();
        settings.dispatcher_function = dispatcher_function;
        settings.dispatchers = dispatchers;
        settings.concurrency = concurrency;
        settings.retries = retries;
        settings.return_stats = return_stats;
        let batches = if return_outputs {
            session.map_unordered(worker_function, inputs, input_key, constant_args)
        } else {
            session.for_each(worker_function, inputs, input_key, constant_args)
        };
        Ok(Batches {
            batches: Arc::new(Mutex::new(Some(batches.map_err(to_py)?))),
        })
    }

    /// Invoke `function_name` once with `payload` and do not wait for it to run.
    fn invoke_detached<'py>(
        &self,
        py: Python<'py>,
        function_name: String,
        payload: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let invoked = self
            .0
            .lock()
            .expect("unpoisoned")
            .invoke_detached(&function_name, payload.into_bytes());
        future_into_py(py, async move { invoked.await.map_err(to_py) })
    }
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Batch>()?;
    m.add_class::<Body>()?;
    m.add_class::<Invoker>()?;
    m.add_class::<Batches>()?;
    m.add_class::<Session>()?;
    m.add_class::<Stub>()?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add("DEFAULT_CONCURRENCY", DEFAULT_CONCURRENCY)?;
    m.add("DEFAULT_DISPATCHER_FUNCTION", DEFAULT_DISPATCHER_FUNCTION)?;
    m.add("LOCAL_ENDPOINT_URL", LOCAL_ENDPOINT_URL)?;
    m.add("MAX_STREAMS_PER_CONNECTION", MAX_STREAMS_PER_CONNECTION)?;
    Ok(())
}
