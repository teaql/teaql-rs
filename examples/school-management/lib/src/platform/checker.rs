
use teaql_runtime::{CheckObjectStatus, CheckResults, ObjectLocation, TypedChecker, UserContext};

pub trait PlatformCheckerLogic: Send + Sync {
    /// Trusted application-owned declarations, never accepted from mutation JSON.
    fn custom_context_fields(&self) -> &'static [&'static str] { &[] }
    fn check_and_fix_platform(
        &self,
        _context: &UserContext,
        _entity: &mut crate::Platform,
        _status: CheckObjectStatus,
        _location: &ObjectLocation,
        _results: &mut CheckResults,
    ) {
    }

    /// Context/state defaults run only for create or an independent business mutation.
    fn fix_platform(
        &self, context: &UserContext, entity: &mut crate::Platform,
        status: CheckObjectStatus, location: &ObjectLocation, results: &mut CheckResults,
    ) {
        self.check_and_fix_platform(context, entity, status, location, results);
    }

    /// Readonly validation is independent of automatic Context eligibility.
    fn check_platform(
        &self, _context: &UserContext, _entity: &crate::Platform,
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
pub struct NoopPlatformChecker;

impl PlatformCheckerLogic for NoopPlatformChecker {}

#[derive(Clone, Debug)]
pub struct PlatformChecker<L = NoopPlatformChecker> {
    logic: L,
}

impl Default for PlatformChecker<NoopPlatformChecker> {
    fn default() -> Self {
        Self {
            logic: NoopPlatformChecker,
        }
    }
}

impl<L> PlatformChecker<L>
where
    L: PlatformCheckerLogic,
{
    pub fn new(logic: L) -> Self {
        Self { logic }
    }
}

impl<L> TypedChecker<crate::Platform> for PlatformChecker<L>
where
    L: PlatformCheckerLogic,
{
    fn separate_fix_and_check(&self) -> bool { true }

    fn managed_context_fields(&self) -> &'static [&'static str] {
        &["create_time", "update_time", ]
    }

    fn custom_context_fields(&self) -> &'static [&'static str] { self.logic.custom_context_fields() }

    fn fix_typed(
        &self,
        context: &UserContext,
        entity: &mut crate::Platform,
        status: CheckObjectStatus,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        if status.is_create() {
            if let Some(value) = context.context_fix_value::<teaql_core::time::Timestamp>(
                "Platform", "create_time", "now",
                &[], status, location, results) {
                entity.update_create_time(value);
                context.record_fix_evidence(teaql_runtime::FixEvidence::new("Platform", "create_time", teaql_runtime::FixEvidenceSource::Clock, "contextFix:now"));
            }
        }

        if status.is_create() {
            if let Some(value) = context.context_fix_value::<teaql_core::time::Timestamp>(
                "Platform", "update_time", "now",
                &[], status, location, results) {
                entity.update_update_time(value);
                context.record_fix_evidence(teaql_runtime::FixEvidence::new("Platform", "update_time", teaql_runtime::FixEvidenceSource::Clock, "contextFix:now"));
            }
        }
        if status.is_update() {
            if let Some(value) = context.context_fix_value::<teaql_core::time::Timestamp>(
                "Platform", "update_time", "now",
                &[], status, location, results) {
                entity.update_update_time(value);
                context.record_fix_evidence(teaql_runtime::FixEvidence::new("Platform", "update_time", teaql_runtime::FixEvidenceSource::Clock, "contextFix:now"));
            }
        }


        self.logic.fix_platform(context, entity, status, location, results);
    }

    fn check_typed(
        &self, context: &UserContext, entity: &crate::Platform,
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

        if status.is_update() && !entity.is_loaded("name") {
            results.push(
                teaql_runtime::CheckResult::new(
                    teaql_runtime::CheckRule::InvalidType,
                    location.clone().member("name"),
                )
                .with_message("Mutation requires a fully loaded entity"),
            );
        }

        if status.is_update() && !entity.is_loaded("base_url") {
            results.push(
                teaql_runtime::CheckResult::new(
                    teaql_runtime::CheckRule::InvalidType,
                    location.clone().member("base_url"),
                )
                .with_message("Mutation requires a fully loaded entity"),
            );
        }

        if status.is_update() && !entity.is_loaded("create_time") {
            results.push(
                teaql_runtime::CheckResult::new(
                    teaql_runtime::CheckRule::InvalidType,
                    location.clone().member("create_time"),
                )
                .with_message("Mutation requires a fully loaded entity"),
            );
        }

        if status.is_update() && !entity.is_loaded("update_time") {
            results.push(
                teaql_runtime::CheckResult::new(
                    teaql_runtime::CheckRule::InvalidType,
                    location.clone().member("update_time"),
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
        self.logic.check_platform(context, entity, status, location, results);
    }

    fn check_and_fix_typed(
        &self, context: &UserContext, entity: &mut crate::Platform,
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

        if let Some(teaql_core::Value::Text(value)) = values.get("base_url") {
            let length = value.chars().count() as u64;
            if length > 100_u64 {
                results.push(teaql_runtime::CheckResult::max_str(location.clone().member("base_url"), 100_u64, value.clone()));
            }
        }




    }
}