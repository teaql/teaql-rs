
use teaql_runtime::{CheckObjectStatus, CheckResults, ObjectLocation, TypedChecker, UserContext};

pub trait DispatchOrderCheckerLogic: Send + Sync {
    /// Trusted application-owned declarations, never accepted from mutation JSON.
    fn custom_context_fields(&self) -> &'static [&'static str] { &[] }
    fn check_and_fix_dispatch_order(
        &self,
        _context: &UserContext,
        _entity: &mut crate::DispatchOrder,
        _status: CheckObjectStatus,
        _location: &ObjectLocation,
        _results: &mut CheckResults,
    ) {
    }

    /// Context/state defaults run only for create or an independent business mutation.
    fn fix_dispatch_order(
        &self, context: &UserContext, entity: &mut crate::DispatchOrder,
        status: CheckObjectStatus, location: &ObjectLocation, results: &mut CheckResults,
    ) {
        self.check_and_fix_dispatch_order(context, entity, status, location, results);
    }

    /// Readonly validation is independent of automatic Context eligibility.
    fn check_dispatch_order(
        &self, _context: &UserContext, _entity: &crate::DispatchOrder,
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
pub struct NoopDispatchOrderChecker;

impl DispatchOrderCheckerLogic for NoopDispatchOrderChecker {}

#[derive(Clone, Debug)]
pub struct DispatchOrderChecker<L = NoopDispatchOrderChecker> {
    logic: L,
}

impl Default for DispatchOrderChecker<NoopDispatchOrderChecker> {
    fn default() -> Self {
        Self {
            logic: NoopDispatchOrderChecker,
        }
    }
}

impl<L> DispatchOrderChecker<L>
where
    L: DispatchOrderCheckerLogic,
{
    pub fn new(logic: L) -> Self {
        Self { logic }
    }
}

impl<L> TypedChecker<crate::DispatchOrder> for DispatchOrderChecker<L>
where
    L: DispatchOrderCheckerLogic,
{
    fn separate_fix_and_check(&self) -> bool { true }

    fn managed_context_fields(&self) -> &'static [&'static str] {
        &[]
    }

    fn custom_context_fields(&self) -> &'static [&'static str] { self.logic.custom_context_fields() }

    fn fix_typed(
        &self,
        context: &UserContext,
        entity: &mut crate::DispatchOrder,
        status: CheckObjectStatus,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        self.logic.fix_dispatch_order(context, entity, status, location, results);
    }

    fn check_typed(
        &self, context: &UserContext, entity: &crate::DispatchOrder,
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

        if status.is_update() && !entity.is_loaded("distribution_center_id") {
            results.push(
                teaql_runtime::CheckResult::new(
                    teaql_runtime::CheckRule::InvalidType,
                    location.clone().member("distribution_center"),
                )
                .with_message("Mutation requires a fully loaded entity"),
            );
        }

        if status.is_update() && !entity.is_loaded("order_reference") {
            results.push(
                teaql_runtime::CheckResult::new(
                    teaql_runtime::CheckRule::InvalidType,
                    location.clone().member("order_reference"),
                )
                .with_message("Mutation requires a fully loaded entity"),
            );
        }

        if status.is_update() && !entity.is_loaded("handling_note") {
            results.push(
                teaql_runtime::CheckResult::new(
                    teaql_runtime::CheckRule::InvalidType,
                    location.clone().member("handling_note"),
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
        self.logic.check_dispatch_order(context, entity, status, location, results);
    }

    fn check_and_fix_typed(
        &self, context: &UserContext, entity: &mut crate::DispatchOrder,
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
        if let Some(teaql_core::Value::Text(value)) = values.get("order_reference") {
            let length = value.chars().count() as u64;
            if length > 100_u64 {
                results.push(teaql_runtime::CheckResult::max_str(location.clone().member("order_reference"), 100_u64, value.clone()));
            }
        }

        if let Some(teaql_core::Value::Text(value)) = values.get("handling_note") {
            let length = value.chars().count() as u64;
            if length > 100_u64 {
                results.push(teaql_runtime::CheckResult::max_str(location.clone().member("handling_note"), 100_u64, value.clone()));
            }
        }


    }
}