//! Fanning work out through dispatcher lambdas: the client half.
//!
//! One call becomes N dispatcher invocations, each dispatching a strided
//! partition of the inputs from inside AWS and streaming back what
//! finished, then a result. `dispatch` hands those back as they arrive,
//! numbered against the caller's inputs, and checks every partition once
//! all are in.
//!
//! - `plan` - how a call is divided, decided before anything is invoked.
//! - `transport` - how a request reaches a dispatcher, behind a trait.
//! - `frames` - a response body back into frames.
//! - `run` - the call itself.
//! - `testing` - a stand-in for Lambda.

mod error;
mod frames;
mod plan;
mod run;
pub mod testing;
mod transport;

pub use error::Error;
pub use plan::dispatchers_for;
pub use run::{dispatch, Batch, Call, Record, Run};
pub use transport::{Chunks, Lambda, Transport, LOCAL_ENDPOINT_URL};

/// The wire contract, as the dispatcher on the other end of it sees it.
pub use map_lambda_protocol as protocol;
