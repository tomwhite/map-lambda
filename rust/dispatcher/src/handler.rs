//! Lambda response streaming: the only place that knows a frame is a
//! line of JSON written onto a response body.
//!
//! Everything about dispatching a partition is in `dispatch`, which reports
//! through a `ReportCallback` and never sees a `Sender`.

use crate::clients;
use crate::dispatch::{dispatch, ReportCallback};
use crate::protocol::{Completed, Frame, Request};
use lambda_runtime::streaming::{channel, Body, Response as StreamResponse, Sender};
use lambda_runtime::{Error, LambdaEvent};

/// Send one frame as a JSON line.
///
/// Fails when the client has gone; the caller then aborts the body
/// rather than leave a stream that looks complete.
async fn send(tx: &mut Sender, frame: &Frame) -> Result<(), Error> {
    let mut line = serde_json::to_vec(frame)?;
    line.push(b'\n');
    tx.send_data(line.into())
        .await
        .map_err(|e| Error::from(e.to_string()))
}

/// A partition's reports, as frames on the response body.
struct Frames<'a>(&'a mut Sender);

impl ReportCallback for Frames<'_> {
    async fn completed(&mut self, completed: Completed) -> Result<(), Error> {
        send(self.0, &Frame::Completed(completed)).await
    }
}

/// Dispatch the partition and send its result as the last frame.
async fn run_partition(event: LambdaEvent<Request>, tx: &mut Sender) -> Result<(), Error> {
    let req_id = event.context.request_id;
    let (clients, cold) = clients::for_request(event.payload.endpoint_url.as_deref()).await;

    let response = dispatch(event.payload, req_id, &clients, cold, &mut Frames(tx)).await?;

    send(tx, &Frame::Result(Box::new(response))).await
}

/// Returns as soon as the stream exists, with the partition running behind
/// it.
///
/// The work has to be spawned rather than awaited here: the runtime
/// does not start reading the body until this returns, and `send_data`
/// waits for a reader, so doing the dispatch inline would deadlock on
/// the first frame.
pub(crate) async fn function_handler(
    event: LambdaEvent<Request>,
) -> Result<StreamResponse<Body>, Error> {
    let (mut tx, rx) = channel();
    tokio::spawn(async move {
        if run_partition(event, &mut tx).await.is_err() {
            // A truncated stream says why it failed; a well-formed one
            // lacking a result frame would not.
            tx.abort();
        }
    });
    Ok(StreamResponse::from(rx))
}
