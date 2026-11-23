
use teaql_runtime::{CheckObjectStatus, CheckResults, ObjectLocation, TypedChecker, UserContext};

pub trait EntryCheckerLogic: Send + Sync {
    /// Trusted application-owned declarations, never accepted from mutation JSON.
    fn custom_context_fields(&self) -> &'static [&'static str] { &[] }
    fn check_and_fix_entry(
        &self,
        _context: &UserContext,
        _entity: &mut crate::Entry,
        _status: CheckObjectStatus,
        _location: &ObjectLocation,
        _results: &mut CheckResults,
    ) {
    }

    /// Context/state defaults run only for create or an independent business mutation.
    fn fix_entry(
        &self, context: &UserContext, entity: &mut crate::Entry,
        status: CheckObjectStatus, location: &ObjectLocation, results: &mut CheckResults,
    ) {
        self.check_and_fix_entry(context, entity, status, location, results);
    }

    /// Readonly validation is independent of automatic Context eligibility.
    fn check_entry(
        &self, _context: &UserContext, _entity: &crate::Entry,
        _status: CheckObjectStatus, _location: &ObjectLocation, _results: &mut CheckResults,
    ) {}

    fn required(
        &self,
        value: bool,
        field: &str,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        if !value {
            results.push(teaql_runtime::CheckResult::required(location.clone().member(field)));
        }
    }

    fn required_option<V>(
        &self,
        value: Option<&V>,
        field: &str,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        if value.is_none() {
            results.push(teaql_runtime::CheckResult::required(location.clone().member(field)));
        }
    }

    fn required_text(
        &self,
        value: &str,
        field: &str,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        if value.trim().is_empty() {
            results.push(teaql_runtime::CheckResult::required(location.clone().member(field)));
        }
    }

    fn min_string_length(
        &self,
        value: &str,
        field: &str,
        min_len: usize,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        if value.chars().count() < min_len {
            results.push(teaql_runtime::CheckResult::min_str(
                location.clone().member(field),
                min_len as u64,
                value.to_owned(),
            ));
        }
    }

    fn max_string_length(
        &self,
        value: &str,
        field: &str,
        max_len: usize,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        if value.chars().count() > max_len {
            results.push(teaql_runtime::CheckResult::max_str(
                location.clone().member(field),
                max_len as u64,
                value.to_owned(),
            ));
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct NoopEntryChecker;

impl EntryCheckerLogic for NoopEntryChecker {}

#[derive(Clone, Debug)]
pub struct EntryChecker<L = NoopEntryChecker> {
    logic: L,
}

impl Default for EntryChecker<NoopEntryChecker> {
    fn default() -> Self {
        Self {
            logic: NoopEntryChecker,
        }
    }
}

impl<L> EntryChecker<L>
where
    L: EntryCheckerLogic,
{
    pub fn new(logic: L) -> Self {
        Self { logic }
    }
}

impl<L> TypedChecker<crate::Entry> for EntryChecker<L>
where
    L: EntryCheckerLogic,
{
    fn separate_fix_and_check(&self) -> bool { true }

    fn managed_context_fields(&self) -> &'static [&'static str] {
        &["born_at", "touched_at", "guarded_token", ]
    }

    fn custom_context_fields(&self) -> &'static [&'static str] { self.logic.custom_context_fields() }

    fn fix_typed(
        &self,
        context: &UserContext,
        entity: &mut crate::Entry,
        status: CheckObjectStatus,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        if status.is_create() {
            if let Some(value) = context.context_fix_value::<teaql_core::time::Timestamp>(
                "Entry", "born_at", "now",
                &[], status, location, results) {
                entity.update_born_at(value);
                context.record_fix_evidence(teaql_runtime::FixEvidence::new("Entry", "born_at", teaql_runtime::FixEvidenceSource::Clock, "contextFix:now"));
            }
        }

        if status.is_create() {
            if let Some(value) = context.context_fix_value::<teaql_core::time::Timestamp>(
                "Entry", "touched_at", "now",
                &[], status, location, results) {
                entity.update_touched_at(value);
                context.record_fix_evidence(teaql_runtime::FixEvidence::new("Entry", "touched_at", teaql_runtime::FixEvidenceSource::Clock, "contextFix:now"));
            }
        }
        if status.is_update() {
            if let Some(value) = context.context_fix_value::<teaql_core::time::Timestamp>(
                "Entry", "touched_at", "now",
                &[], status, location, results) {
                entity.update_touched_at(value);
                context.record_fix_evidence(teaql_runtime::FixEvidence::new("Entry", "touched_at", teaql_runtime::FixEvidenceSource::Clock, "contextFix:now"));
            }
        }





        if status.is_create() {
            if let Some(value) = context.context_fix_value::<String>(
                "Entry", "guarded_token", "uniStr",
                &[String::from("")], status, location, results) {
                entity.update_guarded_token(value);
                context.record_fix_evidence(teaql_runtime::FixEvidence::new("Entry", "guarded_token", teaql_runtime::FixEvidenceSource::Context, "contextFix:uniStr"));
            }
        }


        self.logic.fix_entry(context, entity, status, location, results);
    }

    fn check_typed(
        &self, context: &UserContext, entity: &crate::Entry,
        status: CheckObjectStatus, location: &ObjectLocation, results: &mut CheckResults, has_mutation: bool,
    ) {
        if has_mutation {
        if status.is_update() && !entity.is_loaded("id") {
            results.push(
                teaql_runtime::CheckResult::new(
                    teaql_runtime::CheckRule::InvalidType,
                    location.clone().member("id"),
                )
                .with_message("Mutation requires a fully loaded entity"),
            );
        }

        if status.is_update() && !entity.is_loaded("catalog_id") {
            results.push(
                teaql_runtime::CheckResult::new(
                    teaql_runtime::CheckRule::InvalidType,
                    location.clone().member("catalog"),
                )
                .with_message("Mutation requires a fully loaded entity"),
            );
        }

        if status.is_update() && !entity.is_loaded("name") {
            results.push(
                teaql_runtime::CheckResult::new(
                    teaql_runtime::CheckRule::InvalidType,
                    location.clone().member("name"),
                )
                .with_message("Mutation requires a fully loaded entity"),
            );
        }

        if status.is_update() && !entity.is_loaded("born_at") {
            results.push(
                teaql_runtime::CheckResult::new(
                    teaql_runtime::CheckRule::InvalidType,
                    location.clone().member("born_at"),
                )
                .with_message("Mutation requires a fully loaded entity"),
            );
        }

        if status.is_update() && !entity.is_loaded("touched_at") {
            results.push(
                teaql_runtime::CheckResult::new(
                    teaql_runtime::CheckRule::InvalidType,
                    location.clone().member("touched_at"),
                )
                .with_message("Mutation requires a fully loaded entity"),
            );
        }

        if status.is_update() && !entity.is_loaded("source_ip") {
            results.push(
                teaql_runtime::CheckResult::new(
                    teaql_runtime::CheckRule::InvalidType,
                    location.clone().member("source_ip"),
                )
                .with_message("Mutation requires a fully loaded entity"),
            );
        }

        if status.is_update() && !entity.is_loaded("last_ip") {
            results.push(
                teaql_runtime::CheckResult::new(
                    teaql_runtime::CheckRule::InvalidType,
                    location.clone().member("last_ip"),
                )
                .with_message("Mutation requires a fully loaded entity"),
            );
        }

        if status.is_update() && !entity.is_loaded("opened_by") {
            results.push(
                teaql_runtime::CheckResult::new(
                    teaql_runtime::CheckRule::InvalidType,
                    location.clone().member("opened_by"),
                )
                .with_message("Mutation requires a fully loaded entity"),
            );
        }

        if status.is_update() && !entity.is_loaded("changed_by") {
            results.push(
                teaql_runtime::CheckResult::new(
                    teaql_runtime::CheckRule::InvalidType,
                    location.clone().member("changed_by"),
                )
                .with_message("Mutation requires a fully loaded entity"),
            );
        }

        if status.is_update() && !entity.is_loaded("guarded_token") {
            results.push(
                teaql_runtime::CheckResult::new(
                    teaql_runtime::CheckRule::InvalidType,
                    location.clone().member("guarded_token"),
                )
                .with_message("Mutation requires a fully loaded entity"),
            );
        }

        if status.is_update() && !entity.is_loaded("version") {
            results.push(
                teaql_runtime::CheckResult::new(
                    teaql_runtime::CheckRule::InvalidType,
                    location.clone().member("version"),
                )
                .with_message("Mutation requires a fully loaded entity"),
            );
        }

        }
        self.logic.check_entry(context, entity, status, location, results);
    }

    fn check_and_fix_typed(
        &self, context: &UserContext, entity: &mut crate::Entry,
        status: CheckObjectStatus, location: &ObjectLocation, results: &mut CheckResults,
    ) {
        let eligible = status.is_create() || teaql_core::Entity::dirty_fields(entity).is_some_and(|fields|
            fields.iter().any(|field| !field.starts_with('_') && field != "id" && field != "version"
                && !self.managed_context_fields().contains(&field.as_str())));
        if eligible { self.fix_typed(context, entity, status, location, results); }
        self.check_typed(context, entity, status, location, results, eligible);
    }

    fn check_fixed_values(
        &self,
        _context: &UserContext,
        values: &teaql_runtime::EntityValues,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        if let Some(teaql_core::Value::Text(value)) = values.get("name") {
            let length = value.chars().count() as u64;
            if length > 100_u64 {
                results.push(teaql_runtime::CheckResult::max_str(location.clone().member("name"), 100_u64, value.clone()));
            }
        }



        if let Some(teaql_core::Value::Text(value)) = values.get("source_ip") {
            let length = value.chars().count() as u64;
            if length > 100_u64 {
                results.push(teaql_runtime::CheckResult::max_str(location.clone().member("source_ip"), 100_u64, value.clone()));
            }
        }

        if let Some(teaql_core::Value::Text(value)) = values.get("last_ip") {
            let length = value.chars().count() as u64;
            if length > 100_u64 {
                results.push(teaql_runtime::CheckResult::max_str(location.clone().member("last_ip"), 100_u64, value.clone()));
            }
        }

        if let Some(teaql_core::Value::Text(value)) = values.get("opened_by") {
            let length = value.chars().count() as u64;
            if length > 100_u64 {
                results.push(teaql_runtime::CheckResult::max_str(location.clone().member("opened_by"), 100_u64, value.clone()));
            }
        }

        if let Some(teaql_core::Value::Text(value)) = values.get("changed_by") {
            let length = value.chars().count() as u64;
            if length > 100_u64 {
                results.push(teaql_runtime::CheckResult::max_str(location.clone().member("changed_by"), 100_u64, value.clone()));
            }
        }

        if let Some(teaql_core::Value::Text(value)) = values.get("guarded_token") {
            let length = value.chars().count() as u64;
            if length > 100_u64 {
                results.push(teaql_runtime::CheckResult::max_str(location.clone().member("guarded_token"), 100_u64, value.clone()));
            }
        }


    }
}