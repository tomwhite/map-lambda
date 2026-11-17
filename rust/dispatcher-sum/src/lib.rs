//! A dispatcher whose combiner sums one field of its workers' outputs, for
//! the integration tests.

use map_lambda_combiner::{Combiner, Error};
use serde::Deserialize;
use serde_json::{Map, Value};

/// Outputs and states are both `{field: n}`, so a state combines like an
/// output.
pub struct Sum {
    config: Config,
    sum: Option<i64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    /// The integer field to sum, in each output and in the state.
    field: String,
}

impl Combiner for Sum {
    const NAME: &'static str = "sum";
    type Partial = Map<String, Value>;

    fn new(config: Value) -> Result<Self, Error> {
        Ok(Self {
            config: serde_json::from_value(config)?,
            sum: None,
        })
    }

    fn combine(&mut self, partial: Map<String, Value>) -> Result<(), Error> {
        let field = &self.config.field;
        let n = partial
            .get(field)
            .and_then(Value::as_i64)
            .ok_or_else(|| format!("the partial has no integer `{field}`"))?;
        self.sum = Some(self.sum.unwrap_or(0) + n);
        Ok(())
    }

    fn into_state(self) -> Result<Option<Map<String, Value>>, Error> {
        let field = self.config.field;
        Ok(self
            .sum
            .map(|sum| Map::from_iter([(field, Value::from(sum))])))
    }
}

#[cfg(test)]
mod tests {
    use super::{Combiner, Sum};
    use serde_json::{json, Map, Value};

    fn partial(value: Value) -> Map<String, Value> {
        value.as_object().expect("an object").clone()
    }

    fn summed(partials: Vec<Map<String, Value>>) -> Option<Map<String, Value>> {
        let mut sum = Sum::new(json!({"field": "i"})).expect("a combiner");
        for partial in partials {
            sum.combine(partial).expect("combined");
        }
        sum.into_state().expect("a state")
    }

    fn ints(is: &[i64]) -> Vec<Map<String, Value>> {
        is.iter().map(|i| partial(json!({ "i": i }))).collect()
    }

    #[test]
    fn outputs_are_summed_into_a_state_of_the_same_shape() {
        assert_eq!(summed(ints(&[2, 3])), Some(partial(json!({"i": 5}))));
    }

    #[test]
    fn nothing_combined_is_no_state() {
        assert!(summed(vec![]).is_none());
    }

    /// The `Combiner` contract: two dispatchers' states merge through
    /// the same combiner, as outputs would.
    #[test]
    fn states_merge_like_outputs() {
        let states = vec![
            summed(ints(&[1, 2])).expect("a state"),
            summed(ints(&[3, 4, 5])).expect("a state"),
        ];
        assert_eq!(summed(states), Some(partial(json!({"i": 15}))));
    }

    #[test]
    fn an_output_without_the_field_is_refused() {
        let mut sum = Sum::new(json!({"field": "i"})).expect("a combiner");
        let refused = sum.combine(partial(json!({"j": 1}))).expect_err("refused");
        assert!(refused.to_string().contains("no integer `i`"));
    }

    #[test]
    fn a_config_it_does_not_know_is_refused() {
        assert!(Sum::new(json!({"field": "i", "flush_every": 2})).is_err());
    }
}
