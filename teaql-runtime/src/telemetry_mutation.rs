use crate::{RuntimeOperation, UserContext};
use std::{collections::BTreeMap, future::Future};

struct RequestScope(crate::FailOpenRuntimeTelemetryScope);
impl Drop for RequestScope {
    fn drop(&mut self) {
        self.0.failure(if std::thread::panicking() {
            "mutation_request_error"
        } else {
            "mutation_request_cancelled"
        });
    }
}

impl UserContext {
    /// One audited-save invocation owns its validation, physical mutations,
    /// authoritative readbacks and owned transaction/audit completion. This
    /// trace root is not a mutation ledger and never merges business state.
    pub(crate) fn observe_mutation_request<'a, T: 'a, E: 'a, F, N>(
        &'a self,
        entity_name: N,
        work: F,
    ) -> impl Future<Output = Result<T, E>> + 'a
    where
        F: Future<Output = Result<T, E>> + 'a,
        N: FnOnce() -> String + 'a,
    {
        if self.runtime_telemetry_is_noop() {
            return futures_util::future::Either::Left(work);
        }
        let work = Box::pin(work);
        futures_util::future::Either::Right(Box::pin(async move {
            let entity = entity_name();
            let scope = RequestScope(
                self.start_runtime_operation(
                    RuntimeOperation::new("mutation", format!("{entity}.saveGraph"))
                        .attribute("teaql.entity.type", entity),
                ),
            );
            let result = scope.0.run(self.observe_sql_statements(work)).await;
            match &result {
                Ok(_) => scope.0.success(BTreeMap::new()),
                Err(_) => scope.0.failure("mutation_request_error"),
            }
            result
        }))
    }
}

#[cfg(test)]
mod tests;
