//! The hook a dispatcher combines its workers' outputs through.
//!
//! A crate of its own, so a client merging states needn't depend on the
//! dispatcher.
#![warn(missing_docs)]

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

/// Why a combiner failed.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Merges partials, one at a time, into a running state.
///
/// A partial is a worker's output or a state, which share one type.
/// Combining states must give the same state as combining the outputs
/// behind them, so the states a call ends with, one per dispatcher, can be
/// merged further with the same combiner.
///
/// Outputs arrive in completion order, so the merge must be associative and
/// commutative (the states form a commutative semigroup under the merge).
/// The dispatcher combines each input at most once.
pub trait Combiner: Sized + Send {
    /// The name calls give; the dispatcher is deployed as `dispatcher-NAME`.
    const NAME: &'static str;

    /// A worker's output, or a state, as JSON. `Box<RawValue>` leaves it
    /// unparsed.
    type Partial: Serialize + DeserializeOwned;

    /// Built once per partition from the call's config.
    fn new(config: Value) -> Result<Self, Error>;

    /// Combine one partial. After an `Err` the combiner is not used again.
    fn combine(&mut self, partial: Self::Partial) -> Result<(), Error>;

    /// The state, once every partial is combined, or `None` if none was.
    fn into_state(self) -> Result<Option<Self::Partial>, Error>;
}
