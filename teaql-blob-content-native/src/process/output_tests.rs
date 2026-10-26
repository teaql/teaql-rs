use super::*;
use std::{collections::VecDeque, io};
enum Step {
    Block,
    Interrupted,
    Bytes(Vec<u8>),
    Eof,
}
struct Reader(VecDeque<Step>);
impl Read for Reader {
    fn read(&mut self, target: &mut [u8]) -> io::Result<usize> {
        match self.0.pop_front().unwrap_or(Step::Eof) {
            Step::Block => Err(io::ErrorKind::WouldBlock.into()),
            Step::Interrupted => Err(io::ErrorKind::Interrupted.into()),
            Step::Eof => Ok(0),
            Step::Bytes(bytes) => {
                let n = bytes.len().min(target.len());
                target[..n].copy_from_slice(&bytes[..n]);
                if n < bytes.len() {
                    self.0.push_front(Step::Bytes(bytes[n..].to_vec()));
                }
                Ok(n)
            }
        }
    }
}
#[test]
fn exit_between_nonblocking_read_and_wait_still_drains_every_reply_byte_before_eof() {
    for length in [32, 600, MAX_REPLY] {
        let expected = vec![b'x'; length];
        let mut reader = Reader(VecDeque::from([
            Step::Block,
            Step::Bytes(expected.clone()),
            Step::Interrupted,
            Step::Eof,
        ]));
        let mut output = OutputState::default();
        let mut waits = 0;
        let first = poll_output(&mut reader, &mut output, || {
            waits += 1;
            Ok(Some(ExitStatus::from_raw(0)))
        });
        assert!(
            matches!(first, Ok(None)),
            "valid unread reply must remain pending after observed exit: {first:?}"
        );
        assert!(!output.eof);
        let second = poll_output(&mut reader, &mut output, || {
            waits += 1;
            panic!("reaped child must not be observed or recorded twice")
        });
        assert!(second.unwrap().unwrap().success());
        assert_eq!(waits, 1);
        assert_eq!(output.bytes, expected);
        assert!(output.eof);
    }
}

#[test]
fn reply_bound_is_still_enforced_after_exit_without_dropping_trailing_bytes() {
    let mut reader = Reader(VecDeque::from([
        Step::Block,
        Step::Bytes(vec![b'x'; MAX_REPLY + 1]),
        Step::Eof,
    ]));
    let mut output = OutputState::default();
    assert!(
        poll_output(&mut reader, &mut output, || Ok(Some(ExitStatus::from_raw(
            0
        ))))
        .unwrap()
        .is_none()
    );
    assert!(matches!(
        poll_output(&mut reader, &mut output, || panic!("already reaped")),
        Err(OutputError::Limit)
    ));
    assert!(output.bytes.len() <= MAX_REPLY);
}

#[test]
fn eof_before_child_exit_waits_for_the_actual_exit_status() {
    let mut reader = Reader(VecDeque::from([Step::Bytes(vec![b'x'; 20]), Step::Eof]));
    let mut output = OutputState::default();
    assert!(
        poll_output(&mut reader, &mut output, || Ok(None))
            .unwrap()
            .is_none()
    );
    assert!(output.eof);
    assert_eq!(
        poll_output(&mut reader, &mut output, || Ok(Some(ExitStatus::from_raw(
            2 << 8
        ))))
        .unwrap()
        .unwrap()
        .code(),
        Some(2)
    );
    assert_eq!(output.bytes, vec![b'x'; 20]);
}
