# Map-lambda

<!-- --8<-- [start:intro] -->
A highly-scalable parallel Python map function running on Rust AWS lambdas.

The framework imposes very little overhead for latency:

> **1000 workers launch, run and return outputs in ~1s cold and ~0.5s warm** (for a [no-op worker](https://github.com/tomwhite/map-lambda/blob/main/benchmarks/echo_latency.py))

While also being ideal for data-intensive applications:

> **a four stage rechunk of the original ERA5 dataset (1 variable) 1.4TB takes ~140s and costs ~$2.45; 3.7x faster than Cubed** (from [rechunk-lambda-benchmarks](https://github.com/tomwhite/rechunk-lambda-benchmarks))

Note: This library is alpha-quality. The API and its behaviour may change in the future.

## How it works

There are two levels of lambdas: a small number of **coordinator** lambdas dispatch to a large number of **worker** lambdas that run a pre-deployed compute function. The map **inputs** are split into **partitions**, and each partition is handled by a coordinator.

Having coordinators run in AWS removes the need for the client to manage a large number of network connections to the workers. The client still gets progress updates since they are sent periodically by the coordinators over a small number of persistent HTTP connections.

There is no persistent state for coordination - everything flows through the lambda functions. As a result the system can scale to a thousand workers in ~1s.

<!-- --8<-- [end:intro] -->

<img src="./docs/map-lambda-architecture.svg">

<!-- --8<-- [start:design] -->
## Design

- **AWS only**. The implementation is deliberately targetted only at AWS Lambda and not other serverless function platforms, which avoids lowest-common denominator effects, and makes it easy to build on existing projects like [Cargo Lambda](https://www.cargo-lambda.info/). Knowledge of the limits in AWS Lambda (like number of concurrent lambdas, scaling rate, etc) can be built directly into the implementation.
- **Rust on the backend, Python on the frontend**. Taking advantage of Rust's high-performance and small binary size for AWS Lambdas means that cold start time is low (tens of milliseconds), while offering a Python API to make it easy to integrate in data processing applications.
- **Execution not storage**. Most map-lambda applications will process data on S3, however this is not a part of the library so it is up to applications to make sure the lambda is deployed in the same AWS region, manage S3 request rates, etc. 

<!-- --8<-- [end:design] -->

## Using it

<!-- --8<-- [start:using] -->
The `map_lambda` Python module provides a small API for running map functions. The following shows a simple example map for squaring integers.

```python
import map_lambda

async for index, answer in map_lambda.map_unordered("square", [1, 2, 3], input_key="input"):
    print(index, answer)
```

`square` is the name of an AWS Lambda function, implemented in Rust using Cargo Lambda, which receives a request struct with a field containing the input value, and returns a response struct with the output:

```rust
use lambda_runtime::{Error, LambdaEvent};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub(crate) struct Request {
    /// The input value being processed.
    input: i64,
}

#[derive(Serialize)]
pub(crate) struct Response {
    /// The output value of the function.
    output: i64,
}

pub(crate) async fn function_handler(event: LambdaEvent<Request>) -> Result<Response, Error> {
    let input = event.payload.input;
    let output = input * input;
    Ok(Response { output })
}
```

The worker lambda must be **idempotent** since inputs may be rerun in some circumstances (see "Failures and retries" below).

**Extra arguments** may be passed to each worker invocation via the `constant_args` Python argument. These are sent to the Rust `Request`, which must have the corresponding structure.

Inputs and outputs are sent as JSON, so they must be JSON-serialisable (e.g. Python `pickle` is *not* used). Inputs and outputs are used to coordinate a computation, so they should be small (a few KB) - large data values may be processed by providing a reference to them; they should not be passed as inputs or outputs.

`map_unordered` yields a **pair** per answer: the position in `inputs` of the invocation that produced it, and a **dict** of the fields of the Rust `Response` object. Answers arrive **as the workers finish them**, in no particular order, so `index` is what ties an answer to its input. For the example above the output is some permutation of:

```
0 {'output': 1}
1 {'output': 4}
2 {'output': 9}
```

There is also a `for_each` function, which takes exactly the same arguments as `map_unordered` but doesn't return any values. The return type of the Rust `function_handler` in this case should be `Result<(), Error>` to indicate an empty response.

Map-lambda provides **diagnostics** in the form of progress updates as the `map_unordered` (or `for_each`) call runs, coordinator and worker invocation stats at the end of the run, and way to visualise the invocations over time. See the API doc and/or examples for details.

## Failures and retries

A lambda function is retried only if there is a transport error - where the request may not have reached the worker. (Note that the client can't tell if the worker ran successfully or not - retrying is permitted only because the function is idempotent.)

The number of retries is set by the `retries` argument to the call, and defaults to 2. If the worker returns an `Error` it is *not* retried; it is assumed that the worker function will retry I/O errors itself.

The call fails with a `RuntimeError` in any of the following cases:
- if the inputs cannot be dispatched,
- if any invocation fails (a worker error, or a transport error after retrying),
- if there is a stream error,
- if the stream does not end with a result frame,
- or if the coordinator fails.

## Out of scope

What is not guaranteed by the framework.

- **Coordinator retries**. There is no mechanism for retrying coordinators. If a coordinator fails then the call will fail and will need to be re-run by the client.
- **Cancellation**. There is no cancellation mechanism, so once lambdas have started they will continue running in AWS. This can happen if the client fails or any of the connections to the coordinators fail.
- **Payload validation**. It is up to the user to ensure that the deployed lambda function matches the Python frontend. A mismatch will be treated as a deserialisation error in the worker for each invocation.

<!-- --8<-- [end:using] -->

## Deployment

<!-- --8<-- [start:deployment] -->
There are two functions to deploy: your worker, and the coordinator from this repo. Each is an ordinary Lambda, deployed with [cargo lambda](https://www.cargo-lambda.info/). The examples below deploy `echo` - a simple function that returns its input - substitute your own worker's crate name.

### 1. IAM roles

The worker and the coordinator need different permissions, so they get a role each.

```shell
# Roles Lambda can assume, with permission to write logs
for role in map-lambda-worker-role map-lambda-coordinator-role; do
  aws iam create-role --role-name $role \
    --assume-role-policy-document file://aws/trust-policy.json >/dev/null
  aws iam attach-role-policy --role-name $role \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole >/dev/null
done

# ...and the coordinator may invoke this project's workers
aws iam put-role-policy --role-name map-lambda-coordinator-role \
  --policy-name invoke-map-lambda-workers \
  --policy-document file://aws/invoke-map-lambda-workers.json >/dev/null
```

A worker that reads or writes S3 needs its own policy on `map-lambda-worker-role` as well. `echo` does not; worker roles are per project and shared with nobody.

### 2. Build and deploy

```shell
cd rust
cargo lambda build --release --arm64  # every crate: echo and coordinator

# --role takes an ARN rather than a name, so ask IAM for it
worker_role=$(aws iam get-role --role-name map-lambda-worker-role --query Role.Arn --output text)
coordinator_role=$(aws iam get-role --role-name map-lambda-coordinator-role --query Role.Arn --output text)

cargo lambda deploy --binary-name echo --role "$worker_role" echo
cargo lambda deploy --binary-name coordinator --role "$coordinator_role" coordinator
```

### 3. Tearing it down

Delete the logs (which are billed for storage):

```shell
aws logs delete-log-group --log-group-name /aws/lambda/echo
aws logs delete-log-group --log-group-name /aws/lambda/coordinator
```

The functions and roles should only be deleted if you don't need to run them again.

```shell
aws lambda delete-function --function-name echo
aws lambda delete-function --function-name coordinator

for role in map-lambda-worker-role map-lambda-coordinator-role; do
  aws iam detach-role-policy --role-name $role \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
  aws iam delete-role --role-name $role
done
```

<!-- --8<-- [end:deployment] -->

## Building and testing map-lambda

<!-- --8<-- [start:development] -->
In the `rust/` directory:

`cargo lambda build`

`cargo test` runs the Rust unit tests for every crate in the workspace.

`cargo lambda watch` runs a local server at `http://localhost:9000` serving *every* lambda crate at once, routed by function name in the URL, rebuilding on file changes.

`uv run pytest` runs the Python unit tests - make sure that the local server is running first.

To invoke a worker manually, use `cargo lambda` or `curl` directly:

```shell
cargo lambda invoke echo \
  --data-ascii '{"path":"/tmp/map-lambda-echo","i":1,"timing_map":{},"default_sleep":0.0}'

curl -X POST \
  http://localhost:9000/2015-03-31/functions/echo/invocations \
  -d '{"path":"/tmp/map-lambda-echo","i":1,"timing_map":{},"default_sleep":0.0}'
```

<!-- --8<-- [end:development] -->

## AI usage

Code and tests were written with AI assistance from Claude. Most of the documentation was written by hand.
