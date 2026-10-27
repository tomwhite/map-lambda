//! The Lambda clients a dispatcher invokes workers through.

use aws_config::retry::RetryConfig;
use aws_credential_types::Credentials;
use aws_sdk_lambda::config::Region;
use aws_sdk_lambda::Client;
use aws_smithy_http_client::{tls, Builder as HttpBuilder};
use map_lambda_protocol::MAX_HTTP_CLIENTS;
use std::borrow::Cow;
use tokio::sync::OnceCell;

/// One set per execution environment, so a warm environment reuses
/// their connection pools.
///
/// The SDK's retries are disabled and re-implemented in
/// `invoke_with_retries`, so that every re-send is counted.
static CLIENTS: OnceCell<Vec<Client>> = OnceCell::const_new();

async fn clients() -> &'static [Client] {
    CLIENTS
        .get_or_init(|| async {
            let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .retry_config(RetryConfig::disabled())
                .load()
                .await;
            (0..MAX_HTTP_CLIENTS)
                .map(|_| {
                    // A fresh HTTP client each, so each has its own
                    // connection pool.
                    let http = HttpBuilder::new()
                        .tls_provider(tls::Provider::Rustls(
                            tls::rustls_provider::CryptoMode::AwsLc,
                        ))
                        .build_https();
                    Client::from_conf(
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
async fn local_client(endpoint_url: &str) -> Client {
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new("test", "test", None, None, "local"))
        .retry_config(RetryConfig::disabled())
        .endpoint_url(endpoint_url)
        .load()
        .await;
    Client::new(&config)
}

/// The clients one request dispatches over, and whether the shared set
/// had to be built - which is what `cold` means in a response.
///
/// Borrowed from `CLIENTS` for a deployed run, so a warm environment
/// reuses its connection pools, and owned for a local one, which gets a
/// throwaway.
pub(crate) async fn for_request(
    endpoint_url: Option<&str>,
) -> (Cow<'static, [Client]>, Option<bool>) {
    match endpoint_url {
        Some(url) => (Cow::Owned(vec![local_client(url).await]), None),
        None => {
            let cold = CLIENTS.get().is_none();
            (Cow::Borrowed(clients().await), Some(cold))
        }
    }
}
