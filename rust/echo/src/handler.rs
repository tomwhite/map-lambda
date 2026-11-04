use lambda_runtime::{Error, LambdaEvent};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::thread::sleep;
use std::time::Duration;

fn default_sleep_secs() -> f64 {
    0.01
}

/// A worker that can run normally, run slowly, or fail, depending on
/// the input and, with `path`, how many times it has already been invoked
/// for that input.
#[derive(Deserialize)]
pub(crate) struct Request {
    /// Directory to keep per-input invocation counts in, so that successive
    /// attempts at an input follow successive `timing_map` codes. Without
    /// it, every invocation uses the first code.
    ///
    /// Only useful against `cargo lambda watch`, which answers a failing
    /// worker with a 500 that the dispatcher retries.
    #[serde(default)]
    path: Option<String>,
    /// The input value being processed.
    i: i64,
    /// Maps an input value (as a string, since JSON object keys are
    /// always strings) to a sequence of timing codes for successive
    /// invocations of that input. The magnitude is the number of
    /// seconds to sleep; the sign indicates the input should be
    /// returned normally (>= 0) or a deliberate failure raised (< 0).
    /// An input missing from the map, or with fewer codes than its
    /// invocation count, runs normally after `default_sleep`.
    #[serde(default)]
    timing_map: HashMap<String, Vec<f64>>,
    /// Sleep duration (in seconds) used when `timing_map` doesn't cover
    /// this input/invocation.
    #[serde(default = "default_sleep_secs")]
    default_sleep: f64,
}

#[derive(Serialize)]
pub(crate) struct Response {
    i: i64,
}

/// Read the current invocation count for `i` from `dir`, write back the
/// incremented count, and return the count as it was *before* this call
/// (i.e. 0 on the first invocation).
fn increment_invocation_count(dir: &Path, i: i64) -> Result<u64, Error> {
    fs::create_dir_all(dir)?;
    let count_path = dir.join(i.to_string());
    let invocation_count: u64 = match fs::read_to_string(&count_path) {
        Ok(s) => s.trim().parse().unwrap_or(0),
        Err(_) => 0,
    };
    fs::write(&count_path, (invocation_count + 1).to_string())?;
    Ok(invocation_count)
}

pub(crate) async fn function_handler(event: LambdaEvent<Request>) -> Result<Response, Error> {
    let payload = event.payload;
    let i = payload.i;

    let invocation_count = match &payload.path {
        Some(path) => increment_invocation_count(Path::new(path), i)?,
        None => 0,
    };

    let timing_code = payload
        .timing_map
        .get(&i.to_string())
        .and_then(|codes| codes.get(invocation_count as usize).copied())
        .unwrap_or(payload.default_sleep);

    if timing_code >= 0.0 {
        sleep(Duration::from_secs_f64(timing_code));
        Ok(Response { i })
    } else {
        sleep(Duration::from_secs_f64(-timing_code));
        Err(Error::from(format!(
            "Deliberately fail on invocation number {} for input {}",
            invocation_count + 1,
            i
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lambda_runtime::Context;

    fn temp_dir() -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_str().unwrap().to_string();
        (dir, path)
    }

    #[tokio::test]
    async fn test_success() {
        let (_dir, path) = temp_dir();
        let event = LambdaEvent::new(
            Request {
                path: Some(path),
                i: 7,
                timing_map: HashMap::new(),
                default_sleep: 0.0,
            },
            Context::default(),
        );
        let response = function_handler(event).await.unwrap();
        assert_eq!(response.i, 7)
    }

    #[tokio::test]
    async fn test_failure_then_success() {
        let (_dir, path) = temp_dir();
        let mut timing_map = HashMap::new();
        timing_map.insert("7".to_string(), vec![-0.01]);

        // first invocation fails
        let event = LambdaEvent::new(
            Request {
                path: Some(path.clone()),
                i: 7,
                timing_map: timing_map.clone(),
                default_sleep: 0.0,
            },
            Context::default(),
        );
        match function_handler(event).await {
            Ok(response) => panic!("expected a deliberate failure, got i={}", response.i),
            Err(error) => assert_eq!(
                error.to_string(),
                "Deliberately fail on invocation number 1 for input 7"
            ),
        }

        // second invocation (beyond the timing_map's single entry) succeeds
        let event = LambdaEvent::new(
            Request {
                path: Some(path),
                i: 7,
                timing_map,
                default_sleep: 0.0,
            },
            Context::default(),
        );
        let response = function_handler(event).await.unwrap();
        assert_eq!(response.i, 7)
    }

    /// Nothing is counted, so a second attempt fails just like the first.
    #[tokio::test]
    async fn without_a_path_every_invocation_uses_the_first_code() {
        let timing_map = HashMap::from([("7".to_string(), vec![-0.01])]);
        for _ in 0..2 {
            let event = LambdaEvent::new(
                Request {
                    path: None,
                    i: 7,
                    timing_map: timing_map.clone(),
                    default_sleep: 0.0,
                },
                Context::default(),
            );
            assert!(function_handler(event).await.is_err());
        }
    }
}
