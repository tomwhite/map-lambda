//! Fan work out to AWS Lambda through dispatcher lambdas: the client.
//!
//! A [`Lambdas`] session partitions a call's inputs across dispatchers,
//! which invoke the workers from inside AWS and stream back [`Batch`]es of
//! what finished.
//!
//! ```no_run
//! use futures::StreamExt;
//! use map_lambda_client::{Lambdas, Settings};
//! use serde_json::{json, Map};
//!
//! # async fn example() -> Result<(), map_lambda_client::Error> {
//! let mut settings = Settings::default();
//! settings.concurrency = 500;
//! let lambdas = Lambdas::connect(None).await.with_settings(settings);
//!
//! let inputs = (0..1_000).map(|i| json!(i)).collect();
//! let mut run = lambdas.map_unordered("echo", inputs, "i", Map::new())?;
//! while let Some(batch) = run.next().await {
//!     for (index, answer) in batch?.answers {
//!         println!("input {index} answered {answer}");
//!     }
//! }
//! # Ok(())
//! # }
//! ```
#![warn(missing_docs)]

// - `plan` - how a call is divided, decided before anything is invoked.
// - `transport` - how a request reaches a dispatcher, behind a trait.
// - `frames` - a response body back into frames.
// - `run` - one call.
// - `session` - the settings calls run with, and the calls themselves.
// - `testing` - a stand-in for Lambda.

mod error;
mod frames;
mod plan;
mod run;
mod session;
pub mod testing;
mod transport;

pub use error::Error;
pub use run::{Batch, DispatcherStats, InvocationStats, Record, Run};
pub use session::{Lambdas, Settings, DEFAULT_CONCURRENCY, DEFAULT_DISPATCHER_FUNCTION};
pub use transport::{Chunks, Lambda, Transport, LOCAL_ENDPOINT_URL};
