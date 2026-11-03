//! A dispatcher whose combiner sums one field of its workers' outputs, for
//! the integration tests.

use map_lambda_combiner::{Combiner, Error};
use serde::Deserialize;
use serde_json::value::{to_raw_value, RawValue};
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

    fn new(config: Value) -> Result<Self, Error> {
        Ok(Self {
            config: serde_json::from_value(config)?,
            sum: None,
        })
    }

    fn combine(&mut self, output: &RawValue) -> Result<(), Error> {
        let output: Map<String, Value> = serde_json::from_str(output.get())?;
        let field = &self.config.field;
        let n = output
            .get(field)
            .and_then(Value::as_i64)
            .ok_or_else(|| format!("the output has no integer `{field}`"))?;
        self.sum = Some(self.sum.unwrap_or(0) + n);
        Ok(())
    }

    fn finish(self) -> Result<Option<Box<RawValue>>, Error> {
        let Some(sum) = self.sum else {
            return Ok(None);
        };
        let state = Map::from_iter([(self.config.field, Value::from(sum))]);
        Ok(Some(to_raw_value(&state)?))
    }
}

#[cfg(test)]
mod tests {
    use super::{Combiner, Sum};
    use serde_json::json;
    use serde_json::value::{to_raw_value, RawValue};

    fn raw(value: serde_json::Value) -> Box<RawValue> {
        to_raw_value(&value).expect("json")
    }

    fn summed(outputs: &[Box<RawValue>]) -> Option<Box<RawValue>> {
        let mut sum = Sum::new(json!({"field": "i"})).expect("a combiner");
        for output in outputs {
            sum.combine(output).expect("combined");
        }
        sum.finish().expect("finished")
    }

    #[test]
    fn outputs_are_summed_into_a_state_of_the_same_shape() {
        let state = summed(&[raw(json!({"i": 2})), raw(json!({"i": 3}))]).expect("a state");
        assert_eq!(state.get(), r#"{"i":5}"#);
    }

    #[test]
    fn nothing_combined_is_no_state() {
        assert!(summed(&[]).is_none());
    }

    /// The `Combiner` contract: states two dispatchers finished merge
    /// through the same combiner, as outputs would.
    #[test]
    fn finished_states_merge_like_outputs() {
        let ints = |is: &[i64]| {
            is.iter()
                .map(|i| raw(json!({ "i": i })))
                .collect::<Vec<_>>()
        };
        let states = [
            summed(&ints(&[1, 2])).expect("a state"),
            summed(&ints(&[3, 4, 5])).expect("a state"),
        ];

        let merged = summed(&states).expect("a state");
        assert_eq!(merged.get(), r#"{"i":15}"#);
    }

    #[test]
    fn an_output_without_the_field_is_refused() {
        let mut sum = Sum::new(json!({"field": "i"})).expect("a combiner");
        let refused = sum.combine(&raw(json!({"j": 1}))).expect_err("refused");
        assert!(refused.to_string().contains("no integer `i`"));
    }

    #[test]
    fn a_config_it_does_not_know_is_refused() {
        assert!(Sum::new(json!({"field": "i", "flush_every": 2})).is_err());
    }
}
