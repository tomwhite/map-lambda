//! Turning a coordinator's response body back into frames.

use crate::Error;
use map_lambda_protocol::Frame;

/// Lambda's response stream opens with a metadata prelude ending in
/// eight NUL bytes, which the SDK does not strip. `cargo lambda watch`
/// strips it itself.
const PRELUDE_SEPARATOR: &[u8] = &[0; 8];

/// Chunks in, frames out. A frame split across chunks is carried over.
#[derive(Default)]
pub(crate) struct Framer {
    buffered: Vec<u8>,
    prelude_settled: bool,
}

impl Framer {
    pub(crate) fn push(&mut self, chunk: &[u8]) -> Result<Vec<Frame>, Error> {
        self.buffered.extend_from_slice(chunk);
        if !self.prelude_settled && !self.settle_prelude() {
            return Ok(Vec::new());
        }

        let mut frames = Vec::new();
        while let Some(end) = self.buffered.iter().position(|b| *b == b'\n') {
            let line: Vec<u8> = self.buffered.drain(..=end).collect();
            let line = &line[..end];
            if !line.iter().all(u8::is_ascii_whitespace) {
                frames.push(
                    serde_json::from_slice(line)
                        .map_err(Error::json("reading a coordinator frame"))?,
                );
            }
        }
        Ok(frames)
    }

    /// Whether the prelude question is settled, consuming it if there was
    /// one.
    ///
    /// Whichever of a NUL (prelude) or a newline (frame) comes first
    /// says which kind of stream this is; guessing wrong either fails to
    /// parse or never yields.
    fn settle_prelude(&mut self) -> bool {
        let nul = self.buffered.iter().position(|b| *b == 0);
        let newline = self.buffered.iter().position(|b| *b == b'\n');
        match (nul, newline) {
            (None, None) => return false,
            (Some(nul), newline) if newline.is_none_or(|line| nul < line) => {
                let Some(at) = self
                    .buffered
                    .windows(PRELUDE_SEPARATOR.len())
                    .position(|window| window == PRELUDE_SEPARATOR)
                else {
                    return false;
                };
                self.buffered.drain(..at + PRELUDE_SEPARATOR.len());
            }
            _ => {}
        }
        self.prelude_settled = true;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::{Framer, PRELUDE_SEPARATOR};
    use map_lambda_protocol::Frame;

    const COMPLETED: &[u8] =
        br#"{"Completed":{"done":2,"outputs":[],"stats":[],"first_error":null}}"#;

    fn done(frames: &[Frame]) -> Vec<usize> {
        frames
            .iter()
            .map(|frame| match frame {
                Frame::Completed(completed) => completed.done,
                Frame::Result(_) => usize::MAX,
            })
            .collect()
    }

    fn deployed(frames: &[&[u8]]) -> Vec<u8> {
        let mut body = br#"{"statusCode":200,"headers":{}}"#.to_vec();
        body.extend_from_slice(PRELUDE_SEPARATOR);
        for frame in frames {
            body.extend_from_slice(frame);
            body.push(b'\n');
        }
        body
    }

    #[test]
    fn the_metadata_prelude_is_not_parsed_as_a_frame() {
        let frames = Framer::default()
            .push(&deployed(&[COMPLETED]))
            .expect("parses");

        assert_eq!(done(&frames), [2]);
    }

    /// As on a local run. Waiting for a prelude would never yield.
    #[test]
    fn a_stream_with_no_prelude_is_read_too() {
        let frames = Framer::default()
            .push(&[COMPLETED, b"\n"].concat())
            .expect("parses");

        assert_eq!(done(&frames), [2]);
    }

    /// A chunk boundary can fall anywhere, including inside the prelude
    /// or the eight NULs it ends with.
    #[test]
    fn a_body_split_at_every_point_reads_the_same() {
        let body = deployed(&[COMPLETED, COMPLETED]);
        for cut in 1..body.len() {
            let mut framer = Framer::default();
            let mut frames = framer.push(&body[..cut]).expect("parses");
            frames.extend(framer.push(&body[cut..]).expect("parses"));

            assert_eq!(done(&frames), [2, 2], "cut at {cut}");
        }
    }

    /// A trailing newline, or a stream that ends on one, is not a frame.
    #[test]
    fn blank_lines_are_not_frames() {
        assert!(Framer::default().push(b"\n\n").expect("parses").is_empty());
    }

    #[test]
    fn an_unknown_frame_kind_is_named_rather_than_ignored() {
        let refused = Framer::default()
            .push(b"{\"something-new\":{}}\n")
            .expect_err("refused");

        assert!(refused
            .to_string()
            .contains("unknown variant `something-new`"));
    }
}
