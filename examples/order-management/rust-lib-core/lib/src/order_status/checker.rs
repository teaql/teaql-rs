
use teaql_runtime::{CheckObjectStatus, CheckResults, ObjectLocation, TypedChecker, UserContext};

pub trait OrderStatusCheckerLogic: Send + Sync {
    fn check_and_fix_order_status(
        &self,
        _context: &UserContext,
        _entity: &mut crate::OrderStatus,
        _status: CheckObjectStatus,
        _location: &ObjectLocation,
        _results: &mut CheckResults,
    ) {
    }

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
pub struct NoopOrderStatusChecker;

impl OrderStatusCheckerLogic for NoopOrderStatusChecker {}

#[derive(Clone, Debug)]
pub struct OrderStatusChecker<L = NoopOrderStatusChecker> {
    logic: L,
}

impl Default for OrderStatusChecker<NoopOrderStatusChecker> {
    fn default() -> Self {
        Self {
            logic: NoopOrderStatusChecker,
        }
    }
}

impl<L> OrderStatusChecker<L>
where
    L: OrderStatusCheckerLogic,
{
    pub fn new(logic: L) -> Self {
        Self { logic }
    }
}

impl<L> TypedChecker<crate::OrderStatus> for OrderStatusChecker<L>
where
    L: OrderStatusCheckerLogic,
{
    fn check_and_fix_typed(
        &self,
        context: &UserContext,
        entity: &mut crate::OrderStatus,
        status: CheckObjectStatus,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
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
        if status.is_update() && !entity.is_loaded("code") {
            results.push(
                teaql_runtime::CheckResult::new(
                    teaql_runtime::CheckRule::InvalidType,
                    location.clone().member("code"),
                )
                .with_message("Mutation requires a fully loaded entity"),
            );
        }
        if status.is_update() && !entity.is_loaded("color") {
            results.push(
                teaql_runtime::CheckResult::new(
                    teaql_runtime::CheckRule::InvalidType,
                    location.clone().member("color"),
                )
                .with_message("Mutation requires a fully loaded entity"),
            );
        }
        if status.is_update() && !entity.is_loaded("display_order") {
            results.push(
                teaql_runtime::CheckResult::new(
                    teaql_runtime::CheckRule::InvalidType,
                    location.clone().member("display_order"),
                )
                .with_message("Mutation requires a fully loaded entity"),
            );
        }
        if status.is_update() && !entity.is_loaded("commerce_platform_id") {
            results.push(
                teaql_runtime::CheckResult::new(
                    teaql_runtime::CheckRule::InvalidType,
                    location.clone().member("commerce_platform"),
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
        self.logic
            .check_and_fix_order_status(context, entity, status, location, results);
    }
}