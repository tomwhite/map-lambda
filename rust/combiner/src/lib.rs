//! The hook a dispatcher combines its workers' outputs through.
//!
//! A crate of its own, so a client merging states needn't depend on the
//! dispatcher.
#![warn(missing_docs)]

use serde_json::value::RawValue;
use serde_json::Value;

/// Why a combiner failed.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Merges a partition's outputs, one at a time, into a running state.
///
/// Outputs arrive in completion order, so the merge must be associative and
/// commutative (the states form a commutative semigroup under the merge).
/// The dispatcher combines each input at most once.
///
/// The finished state must combine like an output: passing states back
/// through [`combine`](Self::combine) and finishing gives their merge. So
/// the states a call ends with, one per dispatcher, can be merged further
/// with the same combiner.
pub trait Combiner: Sized + Send {
    /// The name calls give; the dispatcher is deployed as `dispatcher-NAME`.
    const NAME: &'static str;

    /// Built once per partition from the call's config.
    fn new(config: Value) -> Result<Self, Error>;

    /// Combine one worker's output, as unparsed JSON. After an `Err` the
    /// combiner is not used again.
    fn combine(&mut self, output: &RawValue) -> Result<(), Error>;

    /// The state, once every output is combined, or `None` if none was.
    fn finish(self) -> Result<Option<Box<RawValue>>, Error>;
}
