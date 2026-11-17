//! Fan work out to AWS Lambda through dispatcher lambdas: the client.
//!
//! A [`Session`] partitions a call's inputs across dispatchers,
//! which invoke the workers from inside AWS and stream back [`Batch`]es of
//! what finished.
//!
//! ```no_run
//! use futures::StreamExt;
//! use map_lambda_client::{Session, Settings};
//! use serde_json::{json, Map};
//!
//! # async fn example() -> Result<(), map_lambda_client::Error> {
//! let mut settings = Settings::default();
//! settings.concurrency = 500;
//! let session = Session::connect(None).await.with_settings(settings);
//!
//! let inputs = (0..1_000).map(|i| json!(i)).collect();
//! let mut outputs = session.map_unordered("echo", inputs, "i", Map::new())?.outputs();
//! while let Some(output) = outputs.next().await {
//!     let (index, output) = output?;
//!     println!("input {index} returned {output}");
//! }
//! # Ok(())
//! # }
//! ```
#![warn(missing_docs)]

// A call flows `session` -> `call` -> `plan`, then `invoker` -> `frames`
// once per partition, and comes back from `call` as `Batches`, or through
// `combine` as `CombineBatches` for a combining call:
//
// - `session` - the settings calls run with, and the calls themselves.
// - `call` - one call: planned, invoked, its frames merged and checked.
// - `combine` - a combining call, made from the same frames as a map.
// - `plan` - how a call is divided, decided before anything is invoked.
// - `invoker` - how a request reaches a dispatcher, behind a trait.
// - `frames` - a response body back into frames.
// - `error` - what can go wrong, for every step above.
// - `testing` - a stand-in for Lambda.

mod call;
mod combine;
mod error;
mod frames;
mod invoker;
mod plan;
mod session;
pub mod testing;

pub use call::{Batch, Batches, DispatcherStats, InvocationStats, Record};
pub use combine::{CombineBatch, CombineBatches, State};
pub use error::Error;
pub use invoker::{connect, Chunks, Invoker, LOCAL_ENDPOINT_URL};
pub use session::{function_name, Session, Settings, Worker, DEFAULT_CONCURRENCY, DISPATCHER};

pub use map_lambda_protocol::Combine;
