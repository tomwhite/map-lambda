use dispatcher_sum::Sum;
use lambda_runtime::Error;

#[tokio::main]
async fn main() -> Result<(), Error> {
    map_lambda_dispatcher::run_with::<Sum>().await
}
