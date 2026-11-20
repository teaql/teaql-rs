#[derive(Clone)]
pub struct CustomerOrderExpression<'a> {
    result: teaql_core::eval::EvalResult<&'a crate::CustomerOrder>,
    root_desc: std::sync::Arc<String>,
    path: teaql_core::eval_location::ExpressionPath,
}

impl<'a> CustomerOrderExpression<'a> {
    pub fn new(result: teaql_core::eval::EvalResult<&'a crate::CustomerOrder>, root_desc: std::sync::Arc<String>) -> Self {
        Self { result, root_desc, path: Default::default() }
    }

    pub(crate) fn with_path(mut self, mut path: teaql_core::eval_location::ExpressionPath) -> Self { if matches!(&self.result, teaql_core::eval::EvalResult::NotLoaded { .. }) { path.mark_missing(); } self.path = path; self }

    fn resolve(&self, terminal: &'static str) -> Option<&'a crate::CustomerOrder> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(v) => Some(*v),
            teaql_core::eval::EvalResult::Null => None,
            teaql_core::eval::EvalResult::NotLoaded { failed_node, attempted_path } => {
                crate::trigger_logic_bug_panic(&self.root_desc, &failed_node, &attempted_path, &self.path, terminal)
            }
        }
    }

    pub fn try_eval(&self) -> Result<Option<&'a crate::CustomerOrder>, teaql_core::eval_location::NotLoadedDiagnostic> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(value) => Ok(Some(*value)),
            teaql_core::eval::EvalResult::Null => Ok(None),
            teaql_core::eval::EvalResult::NotLoaded { failed_node, .. } => Err(teaql_core::eval_location::NotLoadedDiagnostic::from_path(&self.path, failed_node, "try_eval()").with_profile(teaql_core::object_location::JsonFieldNamingProfile::from_model_value("camelCase").expect("validated model JSON naming"))),
        }
    }

    pub fn eval(&self) -> Option<&'a crate::CustomerOrder> {
        self.resolve("eval()")
    }

    pub fn unwrap(&self) -> &'a crate::CustomerOrder {
        self.resolve("unwrap()").expect("Relation was legitimately null in database!")
    }

    pub fn get_id(self) -> crate::ValueExpression<'a, u64> {
        let next = self.result.and_then("id", |entity| entity.eval_id());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("id", "get_id"))
    }

    pub fn get_order_number(self) -> crate::ValueExpression<'a, String> {
        let next = self.result.and_then("order_number", |entity| entity.eval_order_number());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("order_number", "get_order_number"))
    }

    pub fn get_order_date(self) -> crate::ValueExpression<'a, chrono::NaiveDate> {
        let next = self.result.and_then("order_date", |entity| entity.eval_order_date());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("order_date", "get_order_date"))
    }

    pub fn get_total_amount(self) -> crate::ValueExpression<'a, rust_decimal::Decimal> {
        let next = self.result.and_then("total_amount", |entity| entity.eval_total_amount());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("total_amount", "get_total_amount"))
    }

    pub fn get_create_time(self) -> crate::ValueExpression<'a, teaql_core::time::Timestamp> {
        let next = self.result.and_then("create_time", |entity| entity.eval_create_time());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("create_time", "get_create_time"))
    }

    pub fn get_update_time(self) -> crate::ValueExpression<'a, teaql_core::time::Timestamp> {
        let next = self.result.and_then("update_time", |entity| entity.eval_update_time());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("update_time", "get_update_time"))
    }

    pub fn get_version(self) -> crate::ValueExpression<'a, i64> {
        let next = self.result.and_then("version", |entity| entity.eval_version());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("version", "get_version"))
    }
    pub fn get_status_id(self) -> crate::ValueExpression<'a, u64> {
        let next = self.result.and_then("status_id", |entity| entity.eval_status_id());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("status", "get_status_id"))
    }

    pub fn get_customer_id(self) -> crate::ValueExpression<'a, u64> {
        let next = self.result.and_then("customer_id", |entity| entity.eval_customer_id());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("customer", "get_customer_id"))
    }

    pub fn get_commerce_platform_id(self) -> crate::ValueExpression<'a, u64> {
        let next = self.result.and_then("commerce_platform_id", |entity| entity.eval_commerce_platform_id());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("commerce_platform", "get_commerce_platform_id"))
    }
    pub fn get_status(self) -> crate::OrderStatusExpression<'a> {
        let next = self.result.and_then("status", |entity| entity.eval_status());
        crate::OrderStatusExpression::new(next, self.root_desc.clone()).with_path(self.path.member("status"))
    }

    pub fn get_customer(self) -> crate::CustomerExpression<'a> {
        let next = self.result.and_then("customer", |entity| entity.eval_customer());
        crate::CustomerExpression::new(next, self.root_desc.clone()).with_path(self.path.member("customer"))
    }

    pub fn get_commerce_platform(self) -> crate::CommercePlatformExpression<'a> {
        let next = self.result.and_then("commerce_platform", |entity| entity.eval_commerce_platform());
        crate::CommercePlatformExpression::new(next, self.root_desc.clone()).with_path(self.path.member("commerce_platform"))
    }
    pub fn status_is_pending(self) -> crate::ValueExpression<'a, bool> {
        let next = self.result.and_then("status_id", |entity| {
            if !entity.is_loaded("status_id") {
                teaql_core::eval::EvalResult::NotLoaded { failed_node: "status_id".to_string(), attempted_path: "status_id".to_string() }
            } else {
                teaql_core::eval::EvalResult::Value(entity.status_is_pending())
            }
        });
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("status", "status_is_pending"))
    }

    pub fn status_is_confirmed(self) -> crate::ValueExpression<'a, bool> {
        let next = self.result.and_then("status_id", |entity| {
            if !entity.is_loaded("status_id") {
                teaql_core::eval::EvalResult::NotLoaded { failed_node: "status_id".to_string(), attempted_path: "status_id".to_string() }
            } else {
                teaql_core::eval::EvalResult::Value(entity.status_is_confirmed())
            }
        });
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("status", "status_is_confirmed"))
    }
    pub fn get_order_line_list(self) -> crate::OrderLineListExpression<'a> {
        let next = self.result.and_then("order_line_list", |entity| entity.eval_order_line_list());
        crate::OrderLineListExpression::new(next, self.root_desc.clone()).with_path(self.path.member("order_line_list"))
    }
}

#[derive(Clone)]
pub struct CustomerOrderListExpression<'a> {
    result: teaql_core::eval::EvalResult<&'a teaql_core::SmartList<crate::CustomerOrder>>,
    root_desc: std::sync::Arc<String>,
    path: teaql_core::eval_location::ExpressionPath,
}

impl<'a> CustomerOrderListExpression<'a> {
    pub fn new(result: teaql_core::eval::EvalResult<&'a teaql_core::SmartList<crate::CustomerOrder>>, root_desc: std::sync::Arc<String>) -> Self {
        Self { result, root_desc, path: Default::default() }
    }

    pub(crate) fn with_path(mut self, mut path: teaql_core::eval_location::ExpressionPath) -> Self { if matches!(&self.result, teaql_core::eval::EvalResult::NotLoaded { .. }) { path.mark_missing(); } self.path = path; self }

    fn resolve(&self, terminal: &'static str) -> Option<&'a teaql_core::SmartList<crate::CustomerOrder>> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(v) => Some(*v),
            teaql_core::eval::EvalResult::Null => None,
            teaql_core::eval::EvalResult::NotLoaded { failed_node, attempted_path } => {
                crate::trigger_logic_bug_panic(&self.root_desc, &failed_node, &attempted_path, &self.path, terminal)
            }
        }
    }

    pub fn try_eval(&self) -> Result<Option<&'a teaql_core::SmartList<crate::CustomerOrder>>, teaql_core::eval_location::NotLoadedDiagnostic> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(value) => Ok(Some(*value)),
            teaql_core::eval::EvalResult::Null => Ok(None),
            teaql_core::eval::EvalResult::NotLoaded { failed_node, .. } => Err(teaql_core::eval_location::NotLoadedDiagnostic::from_path(&self.path, failed_node, "try_eval()").with_profile(teaql_core::object_location::JsonFieldNamingProfile::from_model_value("camelCase").expect("validated model JSON naming"))),
        }
    }

    pub fn eval(&self) -> Option<&'a teaql_core::SmartList<crate::CustomerOrder>> {
        self.resolve("eval()")
    }

    pub fn unwrap(&self) -> &'a teaql_core::SmartList<crate::CustomerOrder> {
        self.resolve("unwrap()").expect("List relation was legitimately null in database!")
    }

    pub fn size(&self) -> crate::ValueExpression<'a, usize> {
        let next = self.result.clone().and_then("size", |list| teaql_core::eval::EvalResult::Value(list.len()));
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.operation("size()"))
    }

    pub fn first(&self) -> crate::CustomerOrderExpression<'a> {
        let next = self.result.clone().and_then("first", |list| {
            if let Some(item) = list.first() {
                teaql_core::eval::EvalResult::Value(item)
            } else {
                teaql_core::eval::EvalResult::Null
            }
        });
        crate::CustomerOrderExpression::new(next, self.root_desc.clone()).with_path(self.path.first())
    }

    pub fn get(&self, index: usize) -> crate::CustomerOrderExpression<'a> {
        let next = self.result.clone().and_then("get", |list| {
            if let Some(item) = list.get(index) {
                teaql_core::eval::EvalResult::Value(item)
            } else {
                teaql_core::eval::EvalResult::Null
            }
        });
        crate::CustomerOrderExpression::new(next, self.root_desc.clone()).with_path(self.path.element(index))
    }
}