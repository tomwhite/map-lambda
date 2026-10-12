//! The Lambda clients a coordinator invokes workers through.

use aws_config::retry::RetryConfig;
use aws_credential_types::Credentials;
use aws_sdk_lambda::config::Region;
use aws_sdk_lambda::Client;
use aws_smithy_http_client::{tls, Builder as HttpBuilder};
use std::borrow::Cow;
use tokio::sync::OnceCell;

/// How many invocations to put through one HTTP/2 connection.
const STREAMS_PER_CONNECTION: usize = 100;

/// The most invocations one coordinator will carry at once.
///
/// Keep MAX_IN_FLIGHT_PER_COORDINATOR in map_lambda/coordinator.py in
/// step with this.
const MAX_IN_FLIGHT: usize = 1_600;

/// Clients built at cold start, enough to cover `MAX_IN_FLIGHT`.
const MAX_HTTP_CLIENTS: usize = MAX_IN_FLIGHT.div_ceil(STREAMS_PER_CONNECTION);

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
/// without them. Matches `lambda_client` in map_lambda/client.py.
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

/// How many connections `concurrency` invocations need.
pub(crate) fn connections_for(concurrency: usize) -> usize {
    concurrency
        .div_ceil(STREAMS_PER_CONNECTION)
        .clamp(1, MAX_HTTP_CLIENTS)
}

#[cfg(test)]
mod tests {
    use super::{connections_for, MAX_HTTP_CLIENTS, MAX_IN_FLIGHT, STREAMS_PER_CONNECTION};

    #[test]
    fn connections_cover_the_concurrency_asked_for() {
        // Too few connections does not error; the run is just slower.
        for concurrency in [1usize, 99, 100, 101, 128, 900, MAX_IN_FLIGHT] {
            let n = connections_for(concurrency);
            assert!(
                n * STREAMS_PER_CONNECTION >= concurrency,
                "{concurrency} in flight over {n} connection(s)"
            );
            assert!((1..=MAX_HTTP_CLIENTS).contains(&n));
        }
    }

    #[test]
    fn the_clients_built_cover_the_in_flight_limit() {
        // `div_ceil` guarantees the arithmetic; this is the clamp.
        assert_eq!(connections_for(MAX_IN_FLIGHT), MAX_HTTP_CLIENTS);
    }

    #[test]
    fn one_connection_is_enough_below_the_stream_limit() {
        assert_eq!(connections_for(1), 1);
        assert_eq!(connections_for(STREAMS_PER_CONNECTION), 1);
        assert_eq!(connections_for(STREAMS_PER_CONNECTION + 1), 2);
    }
}
