//! Lambda response streaming: the only place that knows a frame is a
//! line of JSON written onto a response body.
//!
//! Everything about dispatching a partition is in `dispatch`, which reports
//! through a `ReportCallback` and never sees a `Sender`. A regular
//! dispatcher, built with `run`, serves `function_handler`, which refuses to
//! combine; a combining dispatcher, built with `run_with`, serves
//! `combining_handler`.

use crate::combine::Combining;
use crate::dispatch::{dispatch, ReportCallback};
use crate::lambda_clients;
use crate::protocol::{Combine, Completed, Frame, Request};
use futures::future::BoxFuture;
use lambda_runtime::streaming::{channel, Body, Response as StreamResponse, Sender};
use lambda_runtime::{tracing, Error, LambdaEvent};
use map_lambda_combiner::Combiner;
use std::ops::ControlFlow;

/// A partition's reports, as frames on the response body.
struct FrameWriter(Sender);

impl FrameWriter {
    /// Send one frame as a JSON line.
    ///
    /// Fails if the runtime stops reading the body, which a client going away
    /// does not cause; the caller then aborts the body rather than leave a
    /// stream that looks complete.
    async fn send(&mut self, frame: &Frame) -> Result<(), Error> {
        let mut line = serde_json::to_vec(frame)?;
        line.push(b'\n');
        self.0
            .send_data(line.into())
            .await
            .map_err(|e| Error::from(e.to_string()))
    }
}

impl ReportCallback for FrameWriter {
    async fn completed(&mut self, completed: Completed) -> Result<ControlFlow<()>, Error> {
        self.send(&Frame::Completed(completed)).await?;
        Ok(ControlFlow::Continue(()))
    }
}

/// The handler of a regular dispatcher, built with `run`, which does not combine.
pub(crate) async fn function_handler(
    event: LambdaEvent<Request>,
) -> Result<StreamResponse<Body>, Error> {
    Ok(respond(event, |event, writer| {
        Box::pin(map_partition(event, writer))
    }))
}

/// The handler of a combining dispatcher, built with `run_with::<C>()`,
/// which combines through `C` when a call asks it to.
pub(crate) async fn combining_handler<C: Combiner + 'static>(
    event: LambdaEvent<Request>,
) -> Result<StreamResponse<Body>, Error> {
    Ok(respond(event, |event, writer| {
        Box::pin(combining_partition::<C>(event, writer))
    }))
}

/// Returns as soon as the stream exists, with `partition` writing its
/// frames onto it.
///
/// The work has to be spawned rather than awaited here: the runtime
/// does not start reading the body until this returns, and `send_data`
/// waits for a reader, so doing the dispatch inline would deadlock on
/// the first frame.
fn respond<F>(event: LambdaEvent<Request>, partition: F) -> StreamResponse<Body>
where
    F: for<'a> FnOnce(
            LambdaEvent<Request>,
            &'a mut FrameWriter,
        ) -> BoxFuture<'a, Result<(), Error>>
        + Send
        + 'static,
{
    let (tx, rx) = channel();
    tokio::spawn(async move {
        let mut writer = FrameWriter(tx);
        if let Err(error) = partition(event, &mut writer).await {
            // The client sees only that the stream ended early, not why.
            tracing::error!(%error, "partition failed");
            // A truncated stream tells the client it failed; a well-formed
            // one lacking a summary frame would not.
            writer.0.abort();
        }
    });
    StreamResponse::from(rx)
}

/// Dispatch the partition and send its summary as the last frame. A call
/// asking to combine is refused.
async fn map_partition(event: LambdaEvent<Request>, writer: &mut FrameWriter) -> Result<(), Error> {
    let req_id = event.context.request_id;
    let request = event.payload;
    if let Some(combine) = &request.combine {
        return refuse(writer, not_built_with(&combine.combiner)).await;
    }
    let (lambda_clients, cold) = lambda_clients::for_request(request.endpoint_url.as_deref()).await;

    let stats = dispatch(request, req_id, &lambda_clients, cold, writer).await?;
    writer.send(&Frame::Summary(Box::new(stats))).await
}

/// Dispatch the partition, combining its outputs through `C` if it asks
/// to, and send its summary as the last frame.
async fn combining_partition<C: Combiner + 'static>(
    event: LambdaEvent<Request>,
    writer: &mut FrameWriter,
) -> Result<(), Error> {
    let req_id = event.context.request_id;
    let mut request = event.payload;
    let combiner = match request.combine.take().map(build::<C>).transpose() {
        Ok(combiner) => combiner,
        Err(reason) => return refuse(writer, reason).await,
    };
    let (lambda_clients, cold) = lambda_clients::for_request(request.endpoint_url.as_deref()).await;

    let stats = match combiner {
        None => dispatch(request, req_id, &lambda_clients, cold, writer).await?,
        Some(combiner) => {
            // A combiner needs every output, whatever the call asked for.
            request.return_outputs = true;
            let mut combining = Combining::new(combiner, writer);
            let stats = dispatch(request, req_id, &lambda_clients, cold, &mut combining).await?;
            combining.finish().await?;
            stats
        }
    };

    writer.send(&Frame::Summary(Box::new(stats))).await
}

/// Fail the call before invoking anything, on a frame the client stops at.
async fn refuse(writer: &mut FrameWriter, reason: String) -> Result<(), Error> {
    let refused = Completed {
        first_error: Some(reason),
        ..Completed::default()
    };
    writer.send(&Frame::Completed(refused)).await
}

/// The combiner a call asked for, or why it cannot have it.
fn build<C: Combiner>(combine: Combine) -> Result<C, String> {
    if combine.combiner != C::NAME {
        return Err(not_built_with(&combine.combiner));
    }
    C::new(combine.config).map_err(|e| format!("the dispatcher could not build its combiner: {e}"))
}

fn not_built_with(combiner: &str) -> String {
    format!("asked to combine with `{combiner}`, which this dispatcher does not")
}

#[cfg(test)]
mod tests {
    use super::build;
    use crate::protocol::Combine;
    use lambda_runtime::Error;
    use map_lambda_combiner::Combiner;
    use serde_json::value::RawValue;
    use serde_json::{json, Value};

    /// Wants a `field` in its config.
    struct Fussy;

    impl Combiner for Fussy {
        const NAME: &'static str = "fussy";

        fn new(config: Value) -> Result<Self, Error> {
            config.get("field").ok_or("no `field`")?;
            Ok(Self)
        }

        fn combine(&mut self, _: &RawValue) -> Result<(), Error> {
            Ok(())
        }

        fn finish(self) -> Result<Option<Box<RawValue>>, Error> {
            Ok(None)
        }
    }

    fn asking(combiner: &str, config: Value) -> Combine {
        Combine {
            combiner: combiner.into(),
            config,
        }
    }

    #[test]
    fn a_combiner_is_built_from_the_calls_config() {
        assert!(build::<Fussy>(asking("fussy", json!({"field": "i"}))).is_ok());
    }

    #[test]
    fn a_combiner_this_dispatcher_is_not_built_with_is_refused() {
        let Err(reason) = build::<Fussy>(asking("sum", json!({}))) else {
            panic!("expected a refusal");
        };
        assert!(reason.contains("asked to combine with `sum`, which this dispatcher does not"));
    }

    #[test]
    fn a_config_the_combiner_will_not_build_from_is_refused() {
        let Err(reason) = build::<Fussy>(asking("fussy", json!({}))) else {
            panic!("expected a refusal");
        };
        assert!(reason.contains("could not build its combiner: no `field`"));
    }
}
