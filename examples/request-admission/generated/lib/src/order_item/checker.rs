
use teaql_runtime::{CheckObjectStatus, CheckResults, ObjectLocation, TypedChecker, UserContext};

pub trait OrderItemCheckerLogic: Send + Sync {
    fn check_and_fix_order_item(
        &self,
        _context: &UserContext,
        _entity: &mut crate::OrderItem,
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
pub struct NoopOrderItemChecker;

impl OrderItemCheckerLogic for NoopOrderItemChecker {}

#[derive(Clone, Debug)]
pub struct OrderItemChecker<L = NoopOrderItemChecker> {
    logic: L,
}

impl Default for OrderItemChecker<NoopOrderItemChecker> {
    fn default() -> Self {
        Self {
            logic: NoopOrderItemChecker,
        }
    }
}

impl<L> OrderItemChecker<L>
where
    L: OrderItemCheckerLogic,
{
    pub fn new(logic: L) -> Self {
        Self { logic }
    }
}

impl<L> TypedChecker<crate::OrderItem> for OrderItemChecker<L>
where
    L: OrderItemCheckerLogic,
{
    fn check_and_fix_typed(
        &self,
        context: &UserContext,
        entity: &mut crate::OrderItem,
        status: CheckObjectStatus,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        if status.is_create() {
            match context.require_active_root("Tenant") {
                Ok(root) if entity.tenant_id() == 0 => {
                    entity.update_tenant_id(root.id);
                    context.record_fix_evidence(teaql_runtime::FixEvidence::new("OrderItem", "tenant", teaql_runtime::FixEvidenceSource::Context, "activeRoot:Tenant"));
                }
                Ok(root) if entity.tenant_id() != root.id => results.push(teaql_runtime::CheckResult::new(teaql_runtime::CheckRule::ContextRootMismatch, location.clone().member("tenant"))),
                Ok(_) => {}
                Err(error) => results.push(teaql_runtime::CheckResult::new(teaql_runtime::CheckRule::ContextRootMissing, location.clone().member("tenant")).with_message(error.to_string())),
            }
        } else if status.is_update() && entity.tenant_id() != 0 {
            match context.require_active_root("Tenant") {
                Ok(root) if entity.tenant_id() != root.id => results.push(teaql_runtime::CheckResult::new(teaql_runtime::CheckRule::ContextRootMismatch, location.clone().member("tenant"))),
                Ok(_) => {}
                Err(error) => results.push(teaql_runtime::CheckResult::new(teaql_runtime::CheckRule::ContextRootMissing, location.clone().member("tenant")).with_message(error.to_string())),
            }
        }



        if status.is_create() {
            entity.update_create_time(context.fix_time());
            context.record_fix_evidence(teaql_runtime::FixEvidence::new("OrderItem", "create_time", teaql_runtime::FixEvidenceSource::Clock, "graphClock"));
        }

        if status.is_create() {
            entity.update_update_time(context.fix_time());
            context.record_fix_evidence(teaql_runtime::FixEvidence::new("OrderItem", "update_time", teaql_runtime::FixEvidenceSource::Clock, "graphClock"));
        }
        if status.is_create() || status.is_update() {
            entity.update_update_time(context.fix_time());
            context.record_fix_evidence(teaql_runtime::FixEvidence::new("OrderItem", "update_time", teaql_runtime::FixEvidenceSource::Clock, "graphClock"));
        }


        if status.is_update() && !entity.is_loaded("id") {
            results.push(
                teaql_runtime::CheckResult::new(
                    teaql_runtime::CheckRule::InvalidType,
                    location.clone().member("id"),
                )
                .with_message("Mutation requires a fully loaded entity"),
            );
        }

        if status.is_update() && !entity.is_loaded("purchase_order_id") {
            results.push(
                teaql_runtime::CheckResult::new(
                    teaql_runtime::CheckRule::InvalidType,
                    location.clone().member("purchase_order"),
                )
                .with_message("Mutation requires a fully loaded entity"),
            );
        }

        if status.is_update() && !entity.is_loaded("tenant_id") {
            results.push(
                teaql_runtime::CheckResult::new(
                    teaql_runtime::CheckRule::InvalidType,
                    location.clone().member("tenant"),
                )
                .with_message("Mutation requires a fully loaded entity"),
            );
        }

        if status.is_update() && !entity.is_loaded("product_name") {
            results.push(
                teaql_runtime::CheckResult::new(
                    teaql_runtime::CheckRule::InvalidType,
                    location.clone().member("product_name"),
                )
                .with_message("Mutation requires a fully loaded entity"),
            );
        }

        if status.is_update() && !entity.is_loaded("unit_price") {
            results.push(
                teaql_runtime::CheckResult::new(
                    teaql_runtime::CheckRule::InvalidType,
                    location.clone().member("unit_price"),
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

        self.logic
            .check_and_fix_order_item(context, entity, status, location, results);
    }

    fn check_fixed_values(
        &self,
        _context: &UserContext,
        values: &teaql_runtime::EntityValues,
        location: &ObjectLocation,
        results: &mut CheckResults,
    ) {
        if let Some(teaql_core::Value::Text(value)) = values.get("product_name") {
            let length = value.chars().count() as u64;
            if length < 1_u64 {
                results.push(teaql_runtime::CheckResult::min_str(location.clone().member("product_name"), 1_u64, value.clone()));
            }
            if length > 80_u64 {
                results.push(teaql_runtime::CheckResult::max_str(location.clone().member("product_name"), 80_u64, value.clone()));
            }
        }

        if let Some(value) = values.get("unit_price").and_then(teaql_core::Value::try_decimal) {
            let minimum = rust_decimal::Decimal::from_parts(1, 0, 0, false, 2);
            if value < minimum {
                results.push(teaql_runtime::CheckResult::min(location.clone().member("unit_price"), minimum, value));
            }
            let maximum = rust_decimal::Decimal::from_parts(1000000, 0, 0, false, 0);
            if value > maximum {
                results.push(teaql_runtime::CheckResult::max(location.clone().member("unit_price"), maximum, value));
            }
        }




    }
}