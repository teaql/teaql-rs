use futures_core::Stream;
use std::{
    pin::Pin,
    task::{Context, Poll},
    time::SystemTime,
};
use teaql_data_service::{
    ExecutionMetadata, ExecutionObserver, QueryStream, SqlExecutionOutcome, StreamChunk,
};

/// A terminal diagnostic belongs to a cursor, not to the lifetime of UserContext.
pub(crate) struct DiagnosticStream<'a, E> {
    source: Option<QueryStream<'a, E>>,
    metadata: Option<ExecutionMetadata>,
    observer: ExecutionObserver<'a>,
    delivered: usize,
}

impl<'a, E> DiagnosticStream<'a, E> {
    pub(crate) fn wrap(
        source: QueryStream<'a, E>,
        metadata: ExecutionMetadata,
        observer: ExecutionObserver<'a>,
    ) -> QueryStream<'a, E>
    where
        E: 'a,
    {
        Box::pin(Self {
            source: Some(source),
            metadata: Some(metadata),
            observer,
            delivered: 0,
        })
    }

    fn finish(&mut self, outcome: SqlExecutionOutcome) {
        // Release provider cursor/locks before invoking any runtime sink.
        drop(self.source.take());
        if let Some(mut metadata) = self.metadata.take() {
            metadata.ended_at = SystemTime::now();
            metadata.result_count = Some(self.delivered);
            metadata.sql_log.execution_outcome = Some(outcome);
            // A diagnostic sink panic must not replace the provider error or abort on Drop
            // during an existing unwind. Custom panic hooks remain application-owned.
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                (self.observer)(metadata)
            }));
        }
    }
}

impl<E> Stream for DiagnosticStream<'_, E> {
    type Item = Result<StreamChunk, E>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let Some(source) = this.source.as_mut() else {
            return Poll::Ready(None);
        };
        match source.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                this.delivered += chunk.rows.len();
                if chunk.is_last {
                    this.finish(SqlExecutionOutcome::Success);
                }
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(Some(Err(error))) => {
                this.finish(SqlExecutionOutcome::Failure);
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                this.finish(SqlExecutionOutcome::Success);
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<E> Drop for DiagnosticStream<'_, E> {
    fn drop(&mut self) {
        self.finish(if std::thread::panicking() {
            SqlExecutionOutcome::Failure
        } else {
            SqlExecutionOutcome::Cancelled
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;
    use std::{
        collections::VecDeque,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
    };

    struct Source {
        values: VecDeque<Result<StreamChunk, std::io::Error>>,
        pending: bool,
        released: Arc<AtomicBool>,
    }
    impl Stream for Source {
        type Item = Result<StreamChunk, std::io::Error>;
        fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if self.pending {
                Poll::Pending
            } else {
                Poll::Ready(self.values.pop_front())
            }
        }
    }
    impl Drop for Source {
        fn drop(&mut self) {
            self.released.store(true, Ordering::SeqCst);
        }
    }

    fn chunk(last: bool) -> StreamChunk {
        StreamChunk {
            rows: vec![teaql_core::CompactRow::new(Arc::from([]), vec![]); 2],
            chunk_index: 0,
            is_last: last,
        }
    }

    type Entries = Arc<Mutex<Vec<ExecutionMetadata>>>;
    fn fixture(
        values: Vec<Result<StreamChunk, std::io::Error>>,
        pending: bool,
        panic_sink: bool,
    ) -> (
        QueryStream<'static, std::io::Error>,
        Entries,
        Arc<AtomicBool>,
    ) {
        let entries = Arc::new(Mutex::new(Vec::new()));
        let released = Arc::new(AtomicBool::new(false));
        let observer_entries = entries.clone();
        let observer_released = released.clone();
        let observer = Arc::new(move |metadata| {
            assert!(
                observer_released.load(Ordering::SeqCst),
                "observer ran while provider cursor was held"
            );
            observer_entries.lock().unwrap().push(metadata);
            assert!(!panic_sink, "controlled sink failure");
        });
        let source = Box::pin(Source {
            values: values.into(),
            pending,
            released: released.clone(),
        });
        (
            DiagnosticStream::wrap(source, ExecutionMetadata::unrecorded_query(0), observer),
            entries,
            released,
        )
    }

    fn terminal(entries: &Entries, count: usize, outcome: SqlExecutionOutcome) {
        let entries = entries.lock().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].result_count, Some(count));
        assert_eq!(entries[0].sql_log.execution_outcome, Some(outcome));
        assert!(entries[0].debug_query.is_none());
    }

    #[tokio::test]
    async fn partial_transport_failure_preserves_error_and_drops_cursor_first() {
        let (mut stream, entries, _) = fixture(
            vec![
                Ok(chunk(false)),
                Err(std::io::Error::other("driver canary")),
            ],
            false,
            false,
        );
        stream.next().await.unwrap().unwrap();
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().to_string(),
            "driver canary"
        );
        assert!(stream.next().await.is_none());
        drop(stream);
        terminal(&entries, 2, SqlExecutionOutcome::Failure);
    }
    #[tokio::test]
    async fn terminal_chunk_closes_without_extra_poll() {
        let (mut stream, entries, released) = fixture(vec![Ok(chunk(true))], false, false);
        stream.next().await.unwrap().unwrap();
        assert!(released.load(Ordering::SeqCst));
        drop(stream);
        terminal(&entries, 2, SqlExecutionOutcome::Success);
    }
    #[test]
    fn unpolled_drop_is_cancelled_without_delivery() {
        let (stream, entries, _) = fixture(vec![Ok(chunk(false))], false, false);
        drop(stream);
        terminal(&entries, 0, SqlExecutionOutcome::Cancelled);
    }
    #[test]
    fn pending_drop_is_cancelled_without_delivery() {
        let (mut stream, entries, _) = fixture(vec![], true, false);
        let mut cx = Context::from_waker(futures_util::task::noop_waker_ref());
        assert!(stream.as_mut().poll_next(&mut cx).is_pending());
        drop(stream);
        terminal(&entries, 0, SqlExecutionOutcome::Cancelled);
    }
    #[tokio::test]
    async fn consumer_unwind_is_failure_with_delivered_chunk_count() {
        let (mut stream, entries, _) = fixture(vec![Ok(chunk(false))], false, false);
        stream.next().await.unwrap().unwrap();
        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _owned = stream;
            panic!("controlled consumer failure");
        }));
        assert!(unwind.is_err());
        terminal(&entries, 2, SqlExecutionOutcome::Failure);
    }
    #[tokio::test]
    async fn sink_panic_does_not_replace_provider_error() {
        let (mut stream, entries, _) = fixture(
            vec![Err(std::io::Error::other("original driver error"))],
            false,
            true,
        );
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().to_string(),
            "original driver error"
        );
        drop(stream);
        terminal(&entries, 0, SqlExecutionOutcome::Failure);
    }
    #[test]
    fn sink_panic_on_drop_does_not_escape() {
        let (stream, entries, _) = fixture(vec![], true, true);
        drop(stream);
        terminal(&entries, 0, SqlExecutionOutcome::Cancelled);
    }
}
