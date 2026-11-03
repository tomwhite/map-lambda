//! The Lambda clients a dispatcher invokes workers through.

use aws_config::retry::RetryConfig;
use aws_credential_types::Credentials;
use aws_sdk_lambda::config::Region;
use aws_sdk_lambda::Client as LambdaClient;
use aws_smithy_http_client::{tls, Builder as HttpBuilder};
use map_lambda_protocol::MAX_LAMBDA_CLIENTS;
use std::borrow::Cow;
use tokio::sync::OnceCell;

/// One set per execution environment, so a warm environment reuses
/// their connection pools.
static CLIENTS: OnceCell<Vec<LambdaClient>> = OnceCell::const_new();

/// The clients one request dispatches over, and whether the shared set
/// had to be built - which is what `cold` means in the stats.
///
/// Borrowed from `CLIENTS` for a deployed run, so a warm environment
/// reuses its connection pools, and owned for a local one, which gets a
/// throwaway.
pub(crate) async fn for_request(
    endpoint_url: Option<&str>,
) -> (Cow<'static, [LambdaClient]>, Option<bool>) {
    match endpoint_url {
        Some(url) => (Cow::Owned(vec![local_client(url).await]), None),
        None => {
            let cold = CLIENTS.get().is_none();
            (Cow::Borrowed(shared().await), Some(cold))
        }
    }
}

async fn shared() -> &'static [LambdaClient] {
    CLIENTS
        .get_or_init(|| async {
            let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                // Re-implemented in `invoke_with_retries`, so that every
                // re-send is counted.
                .retry_config(RetryConfig::disabled())
                .load()
                .await;
            (0..MAX_LAMBDA_CLIENTS)
                .map(|_| {
                    // A fresh HTTP client each, so each has its own
                    // connection pool.
                    let http = HttpBuilder::new()
                        .tls_provider(tls::Provider::Rustls(
                            tls::rustls_provider::CryptoMode::AwsLc,
                        ))
                        .build_https();
                    LambdaClient::from_conf(
                        aws_sdk_lambda::config::Builder::from(&config)
                            .http_client(http)
                            .build(),
                    )
                })
                .collect()
        })
        .await
}

/// One throwaway client aimed at a local `cargo lambda watch`.
///
/// The emulator does not check credentials, but the SDK will not sign
/// without them.
async fn local_client(endpoint_url: &str) -> LambdaClient {
    // Keep in sync with the client's local config in `LambdaInvoker::connect`.
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new("test", "test", None, None, "local"))
        .retry_config(RetryConfig::disabled())
        .endpoint_url(endpoint_url)
        .load()
        .await;
    LambdaClient::new(&config)
}
