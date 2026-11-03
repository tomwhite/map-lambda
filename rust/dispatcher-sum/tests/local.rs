//! `echo`'s outputs combined through `dispatcher-sum`, both served by
//! `cargo lambda watch` on its default port:
//!
//!     cargo test -p dispatcher-sum -- --ignored

use futures::StreamExt;
use map_lambda_client::{Combine, Error, Session, Settings, State, LOCAL_ENDPOINT_URL};
use serde_json::{json, Value};

const NEEDS: &str = "needs `cargo lambda watch` serving dispatcher-sum and echo";

fn summing(config: Value) -> Combine {
    Combine {
        combiner: "sum".into(),
        config,
    }
}

async fn run(combine: Combine, inputs: usize) -> Result<Vec<State>, Error> {
    let mut settings = Settings::default();
    settings.dispatchers = Some(2);
    settings.concurrency = 8;
    settings.retries = 0;
    let session = Session::connect(Some(LOCAL_ENDPOINT_URL))
        .await
        .with_settings(settings);
    let echo_args = json!({"default_sleep": 0.0});
    let constant_args = echo_args.as_object().expect("an object").clone();
    let inputs = (0..inputs).map(Value::from).collect();
    let mut batches = session.map_combine("echo", inputs, "i", constant_args, combine)?;
    let mut states = Vec::new();
    while let Some(batch) = batches.next().await {
        states.extend(batch?.states);
    }
    Ok(states)
}

fn total(states: &[State]) -> i64 {
    states
        .iter()
        .map(|state| {
            let state: Value = serde_json::from_str(state.state.get()).expect("json");
            state["i"].as_i64().expect("an integer")
        })
        .sum()
}

fn covered(states: &[State]) -> Vec<usize> {
    let mut covered: Vec<usize> = states.iter().flat_map(|s| s.indexes.clone()).collect();
    covered.sort();
    covered
}

#[tokio::test]
#[ignore = "needs `cargo lambda watch` serving dispatcher-sum and echo"]
async fn the_states_add_up_to_every_output() {
    let states = run(summing(json!({"field": "i"})), 20).await.expect(NEEDS);

    assert_eq!(states.len(), 2);
    assert_eq!(total(&states), (0..20).sum::<i64>());
    assert_eq!(covered(&states), (0..20).collect::<Vec<_>>());
}

#[tokio::test]
#[ignore = "needs `cargo lambda watch` serving dispatcher-sum and echo"]
async fn an_output_the_combiner_refuses_fails_the_call() {
    let refused = run(summing(json!({"field": "missing"})), 4)
        .await
        .expect_err("refused");

    assert!(matches!(refused, Error::PartitionFailed(_)), "{refused}");
    let expected = "the combiner refused an output: the output has no integer `missing`";
    assert!(refused.to_string().contains(expected), "{refused}");
}

#[tokio::test]
#[ignore = "needs `cargo lambda watch` serving dispatcher-sum and echo"]
async fn a_config_the_combiner_will_not_build_from_fails_the_call() {
    let refused = run(summing(json!({})), 4).await.expect_err("refused");

    assert!(matches!(refused, Error::PartitionFailed(_)), "{refused}");
    assert!(
        refused.to_string().contains("could not build its combiner"),
        "{refused}"
    );
}
