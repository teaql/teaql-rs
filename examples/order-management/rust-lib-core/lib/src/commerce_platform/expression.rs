#[derive(Clone)]
pub struct CommercePlatformExpression<'a> {
    result: teaql_core::eval::EvalResult<&'a crate::CommercePlatform>,
    root_desc: std::sync::Arc<String>,
}

impl<'a> CommercePlatformExpression<'a> {
    pub fn new(result: teaql_core::eval::EvalResult<&'a crate::CommercePlatform>, root_desc: std::sync::Arc<String>) -> Self {
        Self { result, root_desc }
    }

    fn resolve(&self) -> Option<&'a crate::CommercePlatform> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(v) => Some(*v),
            teaql_core::eval::EvalResult::Null => None,
            teaql_core::eval::EvalResult::NotLoaded { failed_node, attempted_path } => {
                crate::trigger_logic_bug_panic(&self.root_desc, &failed_node, &attempted_path)
            }
        }
    }

    pub fn eval(&self) -> Option<&'a crate::CommercePlatform> {
        self.resolve()
    }

    pub fn unwrap(&self) -> &'a crate::CommercePlatform {
        self.resolve().expect("Relation was legitimately null in database!")
    }

    pub fn get_id(self) -> crate::ValueExpression<'a, u64> {
        let next = self.result.and_then("id", |entity| entity.eval_id());
        crate::ValueExpression::new(next, self.root_desc.clone())
    }

    pub fn get_name(self) -> crate::ValueExpression<'a, String> {
        let next = self.result.and_then("name", |entity| entity.eval_name());
        crate::ValueExpression::new(next, self.root_desc.clone())
    }

    pub fn get_create_time(self) -> crate::ValueExpression<'a, teaql_core::time::Timestamp> {
        let next = self.result.and_then("create_time", |entity| entity.eval_create_time());
        crate::ValueExpression::new(next, self.root_desc.clone())
    }

    pub fn get_update_time(self) -> crate::ValueExpression<'a, teaql_core::time::Timestamp> {
        let next = self.result.and_then("update_time", |entity| entity.eval_update_time());
        crate::ValueExpression::new(next, self.root_desc.clone())
    }

    pub fn get_version(self) -> crate::ValueExpression<'a, i64> {
        let next = self.result.and_then("version", |entity| entity.eval_version());
        crate::ValueExpression::new(next, self.root_desc.clone())
    }
    pub fn get_customer_list(self) -> crate::CustomerListExpression<'a> {
        let next = self.result.and_then("customer_list", |entity| entity.eval_customer_list());
        crate::CustomerListExpression::new(next, self.root_desc.clone())
    }

    pub fn get_order_status_list(self) -> crate::OrderStatusListExpression<'a> {
        let next = self.result.and_then("order_status_list", |entity| entity.eval_order_status_list());
        crate::OrderStatusListExpression::new(next, self.root_desc.clone())
    }

    pub fn get_customer_order_list(self) -> crate::CustomerOrderListExpression<'a> {
        let next = self.result.and_then("customer_order_list", |entity| entity.eval_customer_order_list());
        crate::CustomerOrderListExpression::new(next, self.root_desc.clone())
    }

    pub fn get_product_list(self) -> crate::ProductListExpression<'a> {
        let next = self.result.and_then("product_list", |entity| entity.eval_product_list());
        crate::ProductListExpression::new(next, self.root_desc.clone())
    }

    pub fn get_order_line_list(self) -> crate::OrderLineListExpression<'a> {
        let next = self.result.and_then("order_line_list", |entity| entity.eval_order_line_list());
        crate::OrderLineListExpression::new(next, self.root_desc.clone())
    }

    pub fn get_order_search_preset_list(self) -> crate::OrderSearchPresetListExpression<'a> {
        let next = self.result.and_then("order_search_preset_list", |entity| entity.eval_order_search_preset_list());
        crate::OrderSearchPresetListExpression::new(next, self.root_desc.clone())
    }
}

#[derive(Clone)]
pub struct CommercePlatformListExpression<'a> {
    result: teaql_core::eval::EvalResult<&'a teaql_core::SmartList<crate::CommercePlatform>>,
    root_desc: std::sync::Arc<String>,
}

impl<'a> CommercePlatformListExpression<'a> {
    pub fn new(result: teaql_core::eval::EvalResult<&'a teaql_core::SmartList<crate::CommercePlatform>>, root_desc: std::sync::Arc<String>) -> Self {
        Self { result, root_desc }
    }

    fn resolve(&self) -> Option<&'a teaql_core::SmartList<crate::CommercePlatform>> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(v) => Some(*v),
            teaql_core::eval::EvalResult::Null => None,
            teaql_core::eval::EvalResult::NotLoaded { failed_node, attempted_path } => {
                crate::trigger_logic_bug_panic(&self.root_desc, &failed_node, &attempted_path)
            }
        }
    }

    pub fn eval(&self) -> Option<&'a teaql_core::SmartList<crate::CommercePlatform>> {
        self.resolve()
    }

    pub fn unwrap(&self) -> &'a teaql_core::SmartList<crate::CommercePlatform> {
        self.resolve().expect("List relation was legitimately null in database!")
    }

    pub fn size(&self) -> crate::ValueExpression<'a, usize> {
        let next = self.result.clone().and_then("size", |list| teaql_core::eval::EvalResult::Value(list.len()));
        crate::ValueExpression::new(next, self.root_desc.clone())
    }

    pub fn first(&self) -> crate::CommercePlatformExpression<'a> {
        let next = self.result.clone().and_then("first", |list| {
            if let Some(item) = list.first() {
                teaql_core::eval::EvalResult::Value(item)
            } else {
                teaql_core::eval::EvalResult::Null
            }
        });
        crate::CommercePlatformExpression::new(next, self.root_desc.clone())
    }

    pub fn get(&self, index: usize) -> crate::CommercePlatformExpression<'a> {
        let next = self.result.clone().and_then("get", |list| {
            if let Some(item) = list.get(index) {
                teaql_core::eval::EvalResult::Value(item)
            } else {
                teaql_core::eval::EvalResult::Null
            }
        });
        crate::CommercePlatformExpression::new(next, self.root_desc.clone())
    }
}