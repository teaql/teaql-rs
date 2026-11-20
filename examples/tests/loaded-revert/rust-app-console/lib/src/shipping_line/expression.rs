#[derive(Clone)]
pub struct ShippingLineExpression<'a> {
    result: teaql_core::eval::EvalResult<&'a crate::ShippingLine>,
    root_desc: std::sync::Arc<String>,
    path: teaql_core::eval_location::ExpressionPath,
}

impl<'a> ShippingLineExpression<'a> {
    pub fn new(result: teaql_core::eval::EvalResult<&'a crate::ShippingLine>, root_desc: std::sync::Arc<String>) -> Self {
        Self { result, root_desc, path: Default::default() }
    }

    pub(crate) fn with_path(mut self, mut path: teaql_core::eval_location::ExpressionPath) -> Self { if matches!(&self.result, teaql_core::eval::EvalResult::NotLoaded { .. }) { path.mark_missing(); } self.path = path; self }

    fn resolve(&self, terminal: &'static str) -> Option<&'a crate::ShippingLine> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(v) => Some(*v),
            teaql_core::eval::EvalResult::Null => None,
            teaql_core::eval::EvalResult::NotLoaded { failed_node, attempted_path } => {
                crate::trigger_logic_bug_panic(&self.root_desc, &failed_node, &attempted_path, &self.path, terminal)
            }
        }
    }

    pub fn try_eval(&self) -> Result<Option<&'a crate::ShippingLine>, teaql_core::eval_location::NotLoadedDiagnostic> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(value) => Ok(Some(*value)),
            teaql_core::eval::EvalResult::Null => Ok(None),
            teaql_core::eval::EvalResult::NotLoaded { failed_node, .. } => Err(teaql_core::eval_location::NotLoadedDiagnostic::from_path(&self.path, failed_node, "try_eval()").with_profile(teaql_core::object_location::JsonFieldNamingProfile::from_model_value("camelCase").expect("validated model JSON naming"))),
        }
    }

    pub fn eval(&self) -> Option<&'a crate::ShippingLine> {
        self.resolve("eval()")
    }

    pub fn unwrap(&self) -> &'a crate::ShippingLine {
        self.resolve("unwrap()").expect("Relation was legitimately null in database!")
    }

    pub fn get_id(self) -> crate::ValueExpression<'a, u64> {
        let next = self.result.and_then("id", |entity| entity.eval_id());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("id", "get_id"))
    }

    pub fn get_item_code(self) -> crate::ValueExpression<'a, String> {
        let next = self.result.and_then("item_code", |entity| entity.eval_item_code());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("item_code", "get_item_code"))
    }

    pub fn get_quantity(self) -> crate::ValueExpression<'a, i64> {
        let next = self.result.and_then("quantity", |entity| entity.eval_quantity());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("quantity", "get_quantity"))
    }

    pub fn get_delivery_note(self) -> crate::ValueExpression<'a, Option<String>> {
        let next = self.result.and_then("delivery_note", |entity| entity.eval_delivery_note());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("delivery_note", "get_delivery_note"))
    }

    pub fn get_version(self) -> crate::ValueExpression<'a, i64> {
        let next = self.result.and_then("version", |entity| entity.eval_version());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("version", "get_version"))
    }
    pub fn get_shipping_order_id(self) -> crate::ValueExpression<'a, u64> {
        let next = self.result.and_then("shipping_order_id", |entity| entity.eval_shipping_order_id());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("shipping_order", "get_shipping_order_id"))
    }
    pub fn get_shipping_order(self) -> crate::ShippingOrderExpression<'a> {
        let next = self.result.and_then("shipping_order", |entity| entity.eval_shipping_order());
        crate::ShippingOrderExpression::new(next, self.root_desc.clone()).with_path(self.path.member("shipping_order"))
    }
}

#[derive(Clone)]
pub struct ShippingLineListExpression<'a> {
    result: teaql_core::eval::EvalResult<&'a teaql_core::SmartList<crate::ShippingLine>>,
    root_desc: std::sync::Arc<String>,
    path: teaql_core::eval_location::ExpressionPath,
}

impl<'a> ShippingLineListExpression<'a> {
    pub fn new(result: teaql_core::eval::EvalResult<&'a teaql_core::SmartList<crate::ShippingLine>>, root_desc: std::sync::Arc<String>) -> Self {
        Self { result, root_desc, path: Default::default() }
    }

    pub(crate) fn with_path(mut self, mut path: teaql_core::eval_location::ExpressionPath) -> Self { if matches!(&self.result, teaql_core::eval::EvalResult::NotLoaded { .. }) { path.mark_missing(); } self.path = path; self }

    fn resolve(&self, terminal: &'static str) -> Option<&'a teaql_core::SmartList<crate::ShippingLine>> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(v) => Some(*v),
            teaql_core::eval::EvalResult::Null => None,
            teaql_core::eval::EvalResult::NotLoaded { failed_node, attempted_path } => {
                crate::trigger_logic_bug_panic(&self.root_desc, &failed_node, &attempted_path, &self.path, terminal)
            }
        }
    }

    pub fn try_eval(&self) -> Result<Option<&'a teaql_core::SmartList<crate::ShippingLine>>, teaql_core::eval_location::NotLoadedDiagnostic> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(value) => Ok(Some(*value)),
            teaql_core::eval::EvalResult::Null => Ok(None),
            teaql_core::eval::EvalResult::NotLoaded { failed_node, .. } => Err(teaql_core::eval_location::NotLoadedDiagnostic::from_path(&self.path, failed_node, "try_eval()").with_profile(teaql_core::object_location::JsonFieldNamingProfile::from_model_value("camelCase").expect("validated model JSON naming"))),
        }
    }

    pub fn eval(&self) -> Option<&'a teaql_core::SmartList<crate::ShippingLine>> {
        self.resolve("eval()")
    }

    pub fn unwrap(&self) -> &'a teaql_core::SmartList<crate::ShippingLine> {
        self.resolve("unwrap()").expect("List relation was legitimately null in database!")
    }

    pub fn size(&self) -> crate::ValueExpression<'a, usize> {
        let next = self.result.clone().and_then("size", |list| teaql_core::eval::EvalResult::Value(list.len()));
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.operation("size()"))
    }

    pub fn first(&self) -> crate::ShippingLineExpression<'a> {
        let next = self.result.clone().and_then("first", |list| {
            if let Some(item) = list.first() {
                teaql_core::eval::EvalResult::Value(item)
            } else {
                teaql_core::eval::EvalResult::Null
            }
        });
        crate::ShippingLineExpression::new(next, self.root_desc.clone()).with_path(self.path.first())
    }

    pub fn get(&self, index: usize) -> crate::ShippingLineExpression<'a> {
        let next = self.result.clone().and_then("get", |list| {
            if let Some(item) = list.get(index) {
                teaql_core::eval::EvalResult::Value(item)
            } else {
                teaql_core::eval::EvalResult::Null
            }
        });
        crate::ShippingLineExpression::new(next, self.root_desc.clone()).with_path(self.path.element(index))
    }
}