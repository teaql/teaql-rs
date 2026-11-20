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

    pub fn get_fulfillment_status(self) -> crate::ValueExpression<'a, String> {
        let next = self.result.and_then("fulfillment_status", |entity| entity.eval_fulfillment_status());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("fulfillment_status", "get_fulfillment_status"))
    }

    pub fn get_version(self) -> crate::ValueExpression<'a, i64> {
        let next = self.result.and_then("version", |entity| entity.eval_version());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("version", "get_version"))
    }
    pub fn get_merchant_id(self) -> crate::ValueExpression<'a, u64> {
        let next = self.result.and_then("merchant_id", |entity| entity.eval_merchant_id());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("merchant", "get_merchant_id"))
    }

    pub fn get_customer_profile_id(self) -> crate::ValueExpression<'a, u64> {
        let next = self.result.and_then("customer_profile_id", |entity| entity.eval_customer_profile_id());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("customer_profile", "get_customer_profile_id"))
    }
    pub fn get_merchant(self) -> crate::MerchantExpression<'a> {
        let next = self.result.and_then("merchant", |entity| entity.eval_merchant());
        crate::MerchantExpression::new(next, self.root_desc.clone()).with_path(self.path.member("merchant"))
    }

    pub fn get_customer_profile(self) -> crate::CustomerProfileExpression<'a> {
        let next = self.result.and_then("customer_profile", |entity| entity.eval_customer_profile());
        crate::CustomerProfileExpression::new(next, self.root_desc.clone()).with_path(self.path.member("customer_profile"))
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