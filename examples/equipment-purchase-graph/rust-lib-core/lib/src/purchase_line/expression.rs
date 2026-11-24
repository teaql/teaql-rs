#[derive(Clone)]
pub struct PurchaseLineExpression<'a> {
    result: teaql_core::eval::EvalResult<&'a crate::PurchaseLine>,
    root_desc: std::sync::Arc<String>,
    path: teaql_core::eval_location::ExpressionPath,
}

impl<'a> PurchaseLineExpression<'a> {
    pub fn new(result: teaql_core::eval::EvalResult<&'a crate::PurchaseLine>, root_desc: std::sync::Arc<String>) -> Self {
        Self { result, root_desc, path: Default::default() }
    }

    pub(crate) fn with_path(mut self, mut path: teaql_core::eval_location::ExpressionPath) -> Self { if matches!(&self.result, teaql_core::eval::EvalResult::NotLoaded { .. }) { path.mark_missing(); } self.path = path; self }

    fn resolve(&self, terminal: &'static str) -> Option<&'a crate::PurchaseLine> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(v) => Some(*v),
            teaql_core::eval::EvalResult::Null => None,
            teaql_core::eval::EvalResult::NotLoaded { failed_node, attempted_path } => {
                crate::trigger_logic_bug_panic(&self.root_desc, &failed_node, &attempted_path, &self.path, terminal)
            }
        }
    }

    pub fn try_eval(&self) -> Result<Option<&'a crate::PurchaseLine>, teaql_core::eval_location::NotLoadedDiagnostic> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(value) => Ok(Some(*value)),
            teaql_core::eval::EvalResult::Null => Ok(None),
            teaql_core::eval::EvalResult::NotLoaded { failed_node, .. } => Err(teaql_core::eval_location::NotLoadedDiagnostic::from_path(&self.path, failed_node, "try_eval()").with_profile(teaql_core::object_location::JsonFieldNamingProfile::from_model_value("camelCase").expect("validated model JSON naming"))),
        }
    }

    pub fn eval(&self) -> Option<&'a crate::PurchaseLine> {
        self.resolve("eval()")
    }

    pub fn unwrap(&self) -> &'a crate::PurchaseLine {
        self.resolve("unwrap()").expect("Relation was legitimately null in database!")
    }

    pub fn get_id(self) -> crate::ValueExpression<'a, u64> {
        let next = self.result.and_then("id", |entity| entity.eval_id());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("id", "get_id"))
    }

    pub fn get_line_code(self) -> crate::ValueExpression<'a, String> {
        let next = self.result.and_then("line_code", |entity| entity.eval_line_code());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("line_code", "get_line_code"))
    }

    pub fn get_quantity(self) -> crate::ValueExpression<'a, i64> {
        let next = self.result.and_then("quantity", |entity| entity.eval_quantity());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("quantity", "get_quantity"))
    }

    pub fn get_version(self) -> crate::ValueExpression<'a, i64> {
        let next = self.result.and_then("version", |entity| entity.eval_version());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("version", "get_version"))
    }
    pub fn get_purchase_order_id(self) -> crate::ValueExpression<'a, u64> {
        let next = self.result.and_then("purchase_order_id", |entity| entity.eval_purchase_order_id());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("purchase_order", "get_purchase_order_id"))
    }

    pub fn get_catalog_product_id(self) -> crate::ValueExpression<'a, u64> {
        let next = self.result.and_then("catalog_product_id", |entity| entity.eval_catalog_product_id());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("catalog_product", "get_catalog_product_id"))
    }
    pub fn get_purchase_order(self) -> crate::PurchaseOrderExpression<'a> {
        let next = self.result.and_then("purchase_order", |entity| entity.eval_purchase_order());
        crate::PurchaseOrderExpression::new(next, self.root_desc.clone()).with_path(self.path.member("purchase_order"))
    }

    pub fn get_catalog_product(self) -> crate::CatalogProductExpression<'a> {
        let next = self.result.and_then("catalog_product", |entity| entity.eval_catalog_product());
        crate::CatalogProductExpression::new(next, self.root_desc.clone()).with_path(self.path.member("catalog_product"))
    }
}

#[derive(Clone)]
pub struct PurchaseLineListExpression<'a> {
    result: teaql_core::eval::EvalResult<&'a teaql_core::SmartList<crate::PurchaseLine>>,
    root_desc: std::sync::Arc<String>,
    path: teaql_core::eval_location::ExpressionPath,
}

impl<'a> PurchaseLineListExpression<'a> {
    pub fn new(result: teaql_core::eval::EvalResult<&'a teaql_core::SmartList<crate::PurchaseLine>>, root_desc: std::sync::Arc<String>) -> Self {
        Self { result, root_desc, path: Default::default() }
    }

    pub(crate) fn with_path(mut self, mut path: teaql_core::eval_location::ExpressionPath) -> Self { if matches!(&self.result, teaql_core::eval::EvalResult::NotLoaded { .. }) { path.mark_missing(); } self.path = path; self }

    fn resolve(&self, terminal: &'static str) -> Option<&'a teaql_core::SmartList<crate::PurchaseLine>> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(v) => Some(*v),
            teaql_core::eval::EvalResult::Null => None,
            teaql_core::eval::EvalResult::NotLoaded { failed_node, attempted_path } => {
                crate::trigger_logic_bug_panic(&self.root_desc, &failed_node, &attempted_path, &self.path, terminal)
            }
        }
    }

    pub fn try_eval(&self) -> Result<Option<&'a teaql_core::SmartList<crate::PurchaseLine>>, teaql_core::eval_location::NotLoadedDiagnostic> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(value) => Ok(Some(*value)),
            teaql_core::eval::EvalResult::Null => Ok(None),
            teaql_core::eval::EvalResult::NotLoaded { failed_node, .. } => Err(teaql_core::eval_location::NotLoadedDiagnostic::from_path(&self.path, failed_node, "try_eval()").with_profile(teaql_core::object_location::JsonFieldNamingProfile::from_model_value("camelCase").expect("validated model JSON naming"))),
        }
    }

    pub fn eval(&self) -> Option<&'a teaql_core::SmartList<crate::PurchaseLine>> {
        self.resolve("eval()")
    }

    pub fn unwrap(&self) -> &'a teaql_core::SmartList<crate::PurchaseLine> {
        self.resolve("unwrap()").expect("List relation was legitimately null in database!")
    }

    pub fn size(&self) -> crate::ValueExpression<'a, usize> {
        let next = self.result.clone().and_then("size", |list| teaql_core::eval::EvalResult::Value(list.len()));
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.operation("size()"))
    }

    pub fn first(&self) -> crate::PurchaseLineExpression<'a> {
        let next = self.result.clone().and_then("first", |list| {
            if let Some(item) = list.first() {
                teaql_core::eval::EvalResult::Value(item)
            } else {
                teaql_core::eval::EvalResult::Null
            }
        });
        crate::PurchaseLineExpression::new(next, self.root_desc.clone()).with_path(self.path.first())
    }

    pub fn get(&self, index: usize) -> crate::PurchaseLineExpression<'a> {
        let next = self.result.clone().and_then("get", |list| {
            if let Some(item) = list.get(index) {
                teaql_core::eval::EvalResult::Value(item)
            } else {
                teaql_core::eval::EvalResult::Null
            }
        });
        crate::PurchaseLineExpression::new(next, self.root_desc.clone()).with_path(self.path.element(index))
    }
}