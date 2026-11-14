use std::sync::{Arc, Mutex};
use teaql_runtime::{
    RawAuditEventKind, RuntimeError, SafeAuditEvent, SafeAuditEventSink, UserContext,
};

#[derive(Clone, Default)]
pub struct RecordedEvents(pub Arc<Mutex<Vec<SafeAuditEvent>>>);

impl SafeAuditEventSink for RecordedEvents {
    fn on_safe_event(&self, _: &UserContext, event: &SafeAuditEvent) -> Result<(), RuntimeError> {
        self.0
            .lock()
            .map_err(|_| RuntimeError::Event("example event recorder poisoned".into()))?
            .push(event.clone());
        Ok(())
    }
}

impl RecordedEvents {
    pub fn len(&self) -> usize {
        self.0.lock().unwrap().len()
    }

    pub fn verify_recoveries(&self, id: u64) {
        let events = self.0.lock().unwrap();
        let recovered = events
            .iter()
            .filter(|event| {
                event.entity == "Academy"
                    && event.entity_id == Some(id)
                    && event.kind == RawAuditEventKind::Recovered
            })
            .collect::<Vec<_>>();
        assert_eq!(
            recovered.len(),
            2,
            "each committed recovery must have exactly one audit event"
        );
        for event in &recovered {
            assert!(
                event.trace_chain.iter().any(|node| node.kind
                    == inheritance_matching_service_core::teaql_core::TraceKind::AuditReason
                    && node.entity_type == "Academy"
                    && !node.comment.trim().is_empty()),
                "recovery retains its typed audit lineage"
            );
        }
        assert!(
            recovered[1]
                .fields
                .iter()
                .any(|field| field.name == "campus_code"),
            "recovery audit must include corrected child fields in KSML vocabulary"
        );
        assert!(recovered[1].fields.iter().any(|field| field.name == "name"));
    }
}
