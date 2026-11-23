use crate::{FailOpenRuntimeTelemetryScope, RuntimeOperation, UserContext};
use futures_core::Stream;
use std::{
    collections::BTreeMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use teaql_data_service::statement_telemetry::SqlStatementTelemetry;

type EntityStream<'a, T, E> = Pin<Box<dyn Stream<Item = Result<T, E>> + 'a>>;

impl UserContext {
    /// Generated typed boundary counts delivered entities, not buffered chunks.
    /// A never-polled stream does not start telemetry or execute business work.
    #[doc(hidden)]
    pub fn observe_generated_stream<'a, T: 'a, E: 'a>(
        &'a self,
        entity: &'a str,
        source: EntityStream<'a, T, E>,
    ) -> EntityStream<'a, T, E> {
        if self.runtime_telemetry_is_noop() {
            return source;
        }
        Box::pin(GeneratedStream {
            context: self,
            entity,
            source: Some(source),
            scope: None,
            factory: self.sql_statement_factory(),
            delivered: 0,
        })
    }
}

struct GeneratedStream<'a, T, E> {
    context: &'a UserContext,
    entity: &'a str,
    source: Option<EntityStream<'a, T, E>>,
    scope: Option<FailOpenRuntimeTelemetryScope>,
    factory: Option<Arc<dyn SqlStatementTelemetry>>,
    delivered: usize,
}

impl<T, E> GeneratedStream<'_, T, E> {
    fn finish(&mut self, error_type: Option<&str>) {
        // Release the physical cursor inside its carried request context before
        // ending the logical root. SQL diagnostics already carry actual IDs.
        if let Some(scope) = &self.scope {
            crate::telemetry_sql::with_generated_execution_context(
                self.context,
                scope,
                self.factory.as_ref(),
                || drop(self.source.take()),
            );
        } else {
            drop(self.source.take());
        }
        if let Some(scope) = self.scope.take() {
            let attributes =
                BTreeMap::from([("teaql.result.cardinality".into(), self.delivered.into())]);
            match error_type {
                Some(error_type) => scope.failure_with_attributes(error_type, attributes),
                None => scope.success(attributes),
            }
        }
    }
}

#[cfg(test)]
mod tests;

impl<T, E> Stream for GeneratedStream<'_, T, E> {
    type Item = Result<T, E>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let Some(source) = this.source.as_mut() else {
            return Poll::Ready(None);
        };
        let scope = this.scope.get_or_insert_with(|| {
            this.context.start_runtime_operation(
                RuntimeOperation::new("query", format!("{}.stream", this.entity))
                    .attribute("teaql.entity.type", this.entity),
            )
        });
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            crate::telemetry_sql::with_generated_execution_context(
                this.context,
                scope,
                this.factory.as_ref(),
                || source.as_mut().poll_next(cx),
            )
        }))
        .unwrap_or_else(|payload| {
            this.finish(Some("stream_query_error"));
            std::panic::resume_unwind(payload)
        });
        match &result {
            Poll::Ready(Some(Ok(_))) => this.delivered = this.delivered.saturating_add(1),
            Poll::Ready(Some(Err(_))) => this.finish(Some("stream_query_error")),
            Poll::Ready(None) => this.finish(None),
            Poll::Pending => {}
        }
        result
    }
}

impl<T, E> Drop for GeneratedStream<'_, T, E> {
    fn drop(&mut self) {
        self.finish(Some(if std::thread::panicking() {
            "stream_query_error"
        } else {
            "stream_cancelled"
        }));
    }
}
