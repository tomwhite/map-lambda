//! How a run is divided: how many dispatchers, which inputs each one
//! gets, and how much of the concurrency goes with them.
//!
//! All of it decided before anything is invoked, so a run that could not
//! have worked costs nothing.

use crate::Error;
use map_lambda_protocol::{MAX_IN_FLIGHT, MAX_IN_FLIGHT_LOCAL};

fn carried(local: bool) -> usize {
    if local {
        MAX_IN_FLIGHT_LOCAL
    } else {
        MAX_IN_FLIGHT
    }
}

/// The fewest dispatchers that can hold `concurrency` in flight.
pub(crate) fn dispatchers_for(concurrency: usize, local: bool) -> usize {
    concurrency.div_ceil(carried(local)).max(1)
}

/// The inputs, dealt round-robin into `count` partitions: partition `k`
/// holds `inputs[k::count]`.
///
/// Strided because inputs often get cheaper towards the end (a grid
/// enumerated in C order puts its edge cases last), and contiguous
/// blocks would hand the final partition a lighter slice.
pub(crate) fn stride<T>(inputs: Vec<T>, count: usize) -> Vec<Vec<T>> {
    let mut partitions: Vec<Vec<T>> = (0..count).map(|_| Vec::new()).collect();
    for (index, input) in inputs.into_iter().enumerate() {
        partitions[index % count].push(input);
    }
    partitions
}

/// `concurrency` in flight, split as evenly as possible across `count`
/// dispatchers. Remainders go to the earliest partitions, which are
/// also the ones striding gives an extra input.
///
/// Refuses a share more than one dispatcher can carry, which a local
/// one can far less of: it would not fail, just queue, and `check` would
/// only say so once the slow run had happened.
pub(crate) fn split(concurrency: usize, count: usize, local: bool) -> Result<Vec<usize>, Error> {
    if count == 0 {
        return Err(Error::NoDispatchers);
    }
    let (base, extra) = (concurrency / count, concurrency % count);
    if base == 0 && extra == 0 {
        return Err(Error::ConcurrencyTooSmall {
            concurrency,
            dispatchers: count,
        });
    }
    let carries = carried(local);
    let widest = base + usize::from(extra > 0);
    if widest > carries {
        return Err(Error::TooMuchInFlight {
            concurrency,
            dispatchers: count,
            widest,
            carries,
            holder: if local { "a local dispatcher" } else { "one" },
            need: dispatchers_for(concurrency, local),
        });
    }
    Ok((0..count).map(|i| base + usize::from(i < extra)).collect())
}

#[cfg(test)]
mod tests {
    use super::{dispatchers_for, split, stride};
    use map_lambda_protocol::{MAX_IN_FLIGHT, MAX_IN_FLIGHT_LOCAL};

    #[test]
    fn auto_takes_the_fewest_dispatchers_that_hold_the_concurrency() {
        assert_eq!(dispatchers_for(1, false), 1);
        assert_eq!(dispatchers_for(900, false), 1);
        assert_eq!(dispatchers_for(MAX_IN_FLIGHT, false), 1);
        assert_eq!(dispatchers_for(MAX_IN_FLIGHT + 1, false), 2);
        assert_eq!(dispatchers_for(5000, false), 4);
        // a local dispatcher carries one connection's worth, so it needs more
        assert_eq!(dispatchers_for(500, true), 5);
    }

    #[test]
    fn partitions_are_strided_and_between_them_cover_every_input() {
        let partitions = stride((0..10).collect(), 3);

        assert_eq!(partitions, [vec![0, 3, 6, 9], vec![1, 4, 7], vec![2, 5, 8]]);
    }

    #[test]
    fn more_dispatchers_than_inputs_leaves_some_with_nothing() {
        assert_eq!(stride(vec![0, 1], 5)[2], Vec::<i32>::new());
    }

    #[test]
    fn concurrency_is_split_with_the_remainder_to_the_earliest() {
        assert_eq!(
            split(500, 8, false).expect("splits"),
            [63, 63, 63, 63, 62, 62, 62, 62]
        );
        assert_eq!(split(4, 4, false).expect("splits"), [1, 1, 1, 1]);
    }

    /// Silently giving a dispatcher zero in flight would hang it.
    #[test]
    fn a_concurrency_too_small_to_split_is_refused() {
        let refused = split(0, 4, false).expect_err("refused");
        assert!(refused.to_string().contains("too small to split"));
    }

    #[test]
    fn at_least_one_dispatcher_is_needed() {
        let refused = split(500, 0, false).expect_err("refused");
        assert!(refused.to_string().contains("at least one dispatcher"));
    }

    /// The run would work, just several times slower, so nothing else
    /// would report it.
    #[test]
    fn a_share_no_dispatcher_could_carry_is_refused() {
        assert_eq!(
            split(MAX_IN_FLIGHT, 1, false).expect("fits"),
            [MAX_IN_FLIGHT]
        );
        assert_eq!(
            split(MAX_IN_FLIGHT * 2, 2, false).expect("fits"),
            [MAX_IN_FLIGHT; 2]
        );

        let one_over = split(MAX_IN_FLIGHT + 1, 1, false).expect_err("refused");
        assert!(one_over
            .to_string()
            .contains("more than the 1600 one can carry"));
        // the remainder lands on the earliest share, so this is over by one there
        let over_there = split(MAX_IN_FLIGHT * 2 + 1, 2, false).expect_err("refused");
        assert!(over_there
            .to_string()
            .contains("Use at least 3 dispatchers"));
    }

    #[test]
    fn a_local_dispatcher_carries_one_connections_worth() {
        assert!(split(MAX_IN_FLIGHT_LOCAL, 1, true).is_ok());
        let refused = split(MAX_IN_FLIGHT_LOCAL + 1, 1, true).expect_err("refused");
        assert!(refused
            .to_string()
            .contains("more than the 100 a local dispatcher can carry"));
        // the same share is fine against real Lambda
        assert!(split(MAX_IN_FLIGHT_LOCAL + 1, 1, false).is_ok());
    }
}
