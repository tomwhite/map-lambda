//! A stand-in for Lambda, so a test can drive the client without AWS.

use crate::transport::{Chunks, Transport};
use crate::Error;
use futures::channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender};
use futures::future::BoxFuture;
use futures::StreamExt;
use std::collections::VecDeque;
use std::sync::Mutex;

/// Answers each dispatcher invocation, in order, with a body the test
/// writes, and records what it was sent.
#[derive(Default)]
pub struct Stub {
    bodies: Mutex<VecDeque<UnboundedReceiver<Vec<u8>>>>,
    requests: Mutex<Vec<Vec<u8>>>,
    invoked: Mutex<Vec<(String, Vec<u8>)>>,
}

impl Stub {
    /// The body the next dispatcher invocation answers with, delivered a
    /// chunk per send and ended by dropping the sender. It reports
    /// `is_closed` once the client has stopped reading.
    pub fn respond(&self) -> UnboundedSender<Vec<u8>> {
        let (tx, rx) = unbounded();
        self.bodies.lock().expect("unpoisoned").push_back(rx);
        tx
    }

    /// Every dispatcher request, as sent.
    pub fn requests(&self) -> Vec<Vec<u8>> {
        self.requests.lock().expect("unpoisoned").clone()
    }

    /// Every fire-and-forget invocation, as `(function, payload)`.
    pub fn invoked(&self) -> Vec<(String, Vec<u8>)> {
        self.invoked.lock().expect("unpoisoned").clone()
    }
}

impl Transport for Stub {
    fn stream<'a>(
        &'a self,
        _function: &'a str,
        payload: Vec<u8>,
    ) -> BoxFuture<'a, Result<Chunks, Error>> {
        self.requests.lock().expect("unpoisoned").push(payload);
        let body = self.bodies.lock().expect("unpoisoned").pop_front();
        Box::pin(async move {
            let body = body.ok_or_else(|| Error::Transport("the stub has no body left".into()))?;
            Ok(Box::pin(body.map(Ok)) as Chunks)
        })
    }

    fn invoke<'a>(
        &'a self,
        function: &'a str,
        payload: Vec<u8>,
    ) -> BoxFuture<'a, Result<(), Error>> {
        self.invoked
            .lock()
            .expect("unpoisoned")
            .push((function.to_string(), payload));
        Box::pin(async { Ok(()) })
    }
}
