//! `map_lambda._native`: the Rust client, as Python sees it.
//!
//! - `connect` - a `Client` for real Lambda or a local `cargo lambda watch`.
//! - `Stub` - a stand-in for Lambda, for tests.
//! - `dispatch` - one call, as an async iterator of `Batch`es.
//! - `invoke` - one invocation, not waited for.

use futures::channel::mpsc::UnboundedSender;
use futures::StreamExt;
use map_lambda_client::protocol::MAX_STREAMS_PER_CONNECTION;
use map_lambda_client::{testing, Call, Error, Lambda, Transport, LOCAL_ENDPOINT_URL};
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

/// A Lambda client, for real Lambda or a local `cargo lambda watch`.
#[pyclass(frozen)]
struct Client(Arc<Lambda>);

/// A `Client` with credentials and region from the environment, or for
/// the emulator at `endpoint_url`.
#[pyfunction]
#[pyo3(signature = (endpoint_url=None))]
fn connect(py: Python<'_>, endpoint_url: Option<String>) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async move {
        Ok(Client(Arc::new(
            Lambda::connect(endpoint_url.as_deref()).await,
        )))
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

    /// The body the next dispatcher invocation answers with.
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

    /// Every `invoke`, as `(function, payload)`.
    #[getter]
    fn invocations(&self) -> Vec<(String, String)> {
        self.0
            .invoked()
            .into_iter()
            .map(|(function, payload)| (function, String::from_utf8_lossy(&payload).into()))
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

fn transport_of(client: &Bound<'_, PyAny>) -> PyResult<Arc<dyn Transport>> {
    if let Ok(client) = client.cast::<Client>() {
        return Ok(client.get().0.clone());
    }
    if let Ok(stub) = client.cast::<Stub>() {
        return Ok(stub.get().0.clone());
    }
    Err(PyTypeError::new_err(format!(
        "expected a Client or a Stub, not {}",
        client.get_type().name()?
    )))
}

/// Invoke `function` once with `payload` and do not wait for it to run.
#[pyfunction]
fn invoke<'py>(
    client: &Bound<'py, PyAny>,
    function: String,
    payload: String,
) -> PyResult<Bound<'py, PyAny>> {
    let transport = transport_of(client)?;
    future_into_py(client.py(), async move {
        transport
            .invoke(&function, payload.into_bytes())
            .await
            .map_err(to_py)
    })
}

/// What arrived since the last batch: see `map_lambda_client::Batch`.
#[pyclass(frozen, get_all)]
struct Batch {
    done: usize,
    /// `(index, answer)` pairs.
    answers: Py<PyList>,
    invocation_stats: Py<PyList>,
    dispatcher_stats: Py<PyList>,
}

impl Batch {
    fn from_rust(py: Python<'_>, batch: map_lambda_client::Batch) -> PyResult<Self> {
        let answers = PyList::empty(py);
        for (index, answer) in batch.outputs {
            answers.append((index, pythonize::pythonize(py, &answer)?))?;
        }
        Ok(Self {
            done: batch.done,
            answers: answers.unbind(),
            invocation_stats: records(py, &batch.invocations)?,
            dispatcher_stats: records(py, &batch.dispatchers)?,
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
struct Run {
    /// Taken by `__anext__` while it waits, and put back with a batch:
    /// a run that failed or finished is gone, and so is one whose
    /// `__anext__` was cancelled.
    run: Arc<Mutex<Option<map_lambda_client::Run>>>,
}

#[pymethods]
impl Run {
    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let slot = self.run.clone();
        let taken = slot.lock().expect("unpoisoned").take();
        future_into_py(py, async move {
            let Some(mut run) = taken else {
                return Err(PyStopAsyncIteration::new_err(()));
            };
            match run.next().await {
                None => Err(PyStopAsyncIteration::new_err(())),
                Some(Err(error)) => Err(to_py(error)),
                Some(Ok(batch)) => {
                    *slot.lock().expect("unpoisoned") = Some(run);
                    Python::attach(|py| Batch::from_rust(py, batch))
                }
            }
        })
    }

    /// Tear down whatever dispatchers are still running.
    fn close(&self) {
        drop(self.run.lock().expect("unpoisoned").take());
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

/// Start one call through `client`, a `Client` or a `Stub`.
///
/// Inputs and constant args go through `json.dumps`, so they are
/// marshalled exactly as they always were.
#[pyfunction]
#[pyo3(signature = (
    client, inputs, *, constant_args, dispatcher_function, target_function, input_key,
    dispatchers, concurrency, retries, endpoint_url, return_outputs, return_stats, call,
))]
#[allow(clippy::too_many_arguments)]
fn dispatch(
    client: &Bound<'_, PyAny>,
    inputs: &Bound<'_, PyAny>,
    constant_args: &Bound<'_, PyAny>,
    dispatcher_function: String,
    target_function: String,
    input_key: String,
    dispatchers: Option<usize>,
    concurrency: usize,
    retries: usize,
    endpoint_url: Option<String>,
    return_outputs: bool,
    return_stats: bool,
    call: usize,
) -> PyResult<Run> {
    // The dispatcher reads it as the worker's payload template, so
    // anything else fails inside AWS with every invocation billed for.
    if !constant_args.is_instance_of::<PyDict>() {
        return Err(PyTypeError::new_err(format!(
            "constant_args must be a dict, not {}",
            constant_args.get_type().name()?
        )));
    }
    let template: Map<String, Value> = json(constant_args, "constant_args")?;
    let inputs: Vec<Value> = json(inputs, "inputs")?;

    let transport = transport_of(client)?;
    let call = Call {
        dispatcher_function,
        target_function,
        input_key,
        template,
        dispatchers,
        concurrency,
        retries,
        endpoint_url,
        return_outputs,
        return_stats,
        call,
    };
    let run = map_lambda_client::dispatch(transport, call, inputs).map_err(to_py)?;
    Ok(Run {
        run: Arc::new(Mutex::new(Some(run))),
    })
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Batch>()?;
    m.add_class::<Body>()?;
    m.add_class::<Client>()?;
    m.add_class::<Run>()?;
    m.add_class::<Stub>()?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_function(wrap_pyfunction!(invoke, m)?)?;
    m.add_function(wrap_pyfunction!(dispatch, m)?)?;
    m.add("LOCAL_ENDPOINT_URL", LOCAL_ENDPOINT_URL)?;
    m.add("MAX_STREAMS_PER_CONNECTION", MAX_STREAMS_PER_CONNECTION)?;
    Ok(())
}
