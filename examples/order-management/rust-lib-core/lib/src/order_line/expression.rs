#[derive(Clone)]
pub struct OrderLineExpression<'a> {
    result: teaql_core::eval::EvalResult<&'a crate::OrderLine>,
    root_desc: std::sync::Arc<String>,
}

impl<'a> OrderLineExpression<'a> {
    pub fn new(result: teaql_core::eval::EvalResult<&'a crate::OrderLine>, root_desc: std::sync::Arc<String>) -> Self {
        Self { result, root_desc }
    }

    fn resolve(&self) -> Option<&'a crate::OrderLine> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(v) => Some(*v),
            teaql_core::eval::EvalResult::Null => None,
            teaql_core::eval::EvalResult::NotLoaded { failed_node, attempted_path } => {
                crate::trigger_logic_bug_panic(&self.root_desc, &failed_node, &attempted_path)
            }
        }
    }

    pub fn eval(&self) -> Option<&'a crate::OrderLine> {
        self.resolve()
    }

    pub fn unwrap(&self) -> &'a crate::OrderLine {
        self.resolve().expect("Relation was legitimately null in database!")
    }

    pub fn get_id(self) -> crate::ValueExpression<'a, u64> {
        let next = self.result.and_then("id", |entity| entity.eval_id());
        crate::ValueExpression::new(next, self.root_desc.clone())
    }

    pub fn get_product_name(self) -> crate::ValueExpression<'a, String> {
        let next = self.result.and_then("product_name", |entity| entity.eval_product_name());
        crate::ValueExpression::new(next, self.root_desc.clone())
    }

    pub fn get_sku(self) -> crate::ValueExpression<'a, String> {
        let next = self.result.and_then("sku", |entity| entity.eval_sku());
        crate::ValueExpression::new(next, self.root_desc.clone())
    }

    pub fn get_quantity(self) -> crate::ValueExpression<'a, i64> {
        let next = self.result.and_then("quantity", |entity| entity.eval_quantity());
        crate::ValueExpression::new(next, self.root_desc.clone())
    }

    pub fn get_create_time(self) -> crate::ValueExpression<'a, teaql_core::time::Timestamp> {
        let next = self.result.and_then("create_time", |entity| entity.eval_create_time());
        crate::ValueExpression::new(next, self.root_desc.clone())
    }

    pub fn get_version(self) -> crate::ValueExpression<'a, i64> {
        let next = self.result.and_then("version", |entity| entity.eval_version());
        crate::ValueExpression::new(next, self.root_desc.clone())
    }
    pub fn get_customer_order_id(self) -> crate::ValueExpression<'a, u64> {
        let next = self.result.and_then("customer_order_id", |entity| entity.eval_customer_order_id());
        crate::ValueExpression::new(next, self.root_desc.clone())
    }

    pub fn get_product_id(self) -> crate::ValueExpression<'a, u64> {
        let next = self.result.and_then("product_id", |entity| entity.eval_product_id());
        crate::ValueExpression::new(next, self.root_desc.clone())
    }

    pub fn get_commerce_platform_id(self) -> crate::ValueExpression<'a, u64> {
        let next = self.result.and_then("commerce_platform_id", |entity| entity.eval_commerce_platform_id());
        crate::ValueExpression::new(next, self.root_desc.clone())
    }
    pub fn get_customer_order(self) -> crate::CustomerOrderExpression<'a> {
        let next = self.result.and_then("customer_order", |entity| entity.eval_customer_order());
        crate::CustomerOrderExpression::new(next, self.root_desc.clone())
    }

    pub fn get_product(self) -> crate::ProductExpression<'a> {
        let next = self.result.and_then("product", |entity| entity.eval_product());
        crate::ProductExpression::new(next, self.root_desc.clone())
    }

    pub fn get_commerce_platform(self) -> crate::CommercePlatformExpression<'a> {
        let next = self.result.and_then("commerce_platform", |entity| entity.eval_commerce_platform());
        crate::CommercePlatformExpression::new(next, self.root_desc.clone())
    }
}

#[derive(Clone)]
pub struct OrderLineListExpression<'a> {
    result: teaql_core::eval::EvalResult<&'a teaql_core::SmartList<crate::OrderLine>>,
    root_desc: std::sync::Arc<String>,
}

impl<'a> OrderLineListExpression<'a> {
    pub fn new(result: teaql_core::eval::EvalResult<&'a teaql_core::SmartList<crate::OrderLine>>, root_desc: std::sync::Arc<String>) -> Self {
        Self { result, root_desc }
    }

    fn resolve(&self) -> Option<&'a teaql_core::SmartList<crate::OrderLine>> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(v) => Some(*v),
            teaql_core::eval::EvalResult::Null => None,
            teaql_core::eval::EvalResult::NotLoaded { failed_node, attempted_path } => {
                crate::trigger_logic_bug_panic(&self.root_desc, &failed_node, &attempted_path)
            }
        }
    }

    pub fn eval(&self) -> Option<&'a teaql_core::SmartList<crate::OrderLine>> {
        self.resolve()
    }

    pub fn unwrap(&self) -> &'a teaql_core::SmartList<crate::OrderLine> {
        self.resolve().expect("List relation was legitimately null in database!")
    }

    pub fn size(&self) -> crate::ValueExpression<'a, usize> {
        let next = self.result.clone().and_then("size", |list| teaql_core::eval::EvalResult::Value(list.len()));
        crate::ValueExpression::new(next, self.root_desc.clone())
    }

    pub fn first(&self) -> crate::OrderLineExpression<'a> {
        let next = self.result.clone().and_then("first", |list| {
            if let Some(item) = list.first() {
                teaql_core::eval::EvalResult::Value(item)
            } else {
                teaql_core::eval::EvalResult::Null
            }
        });
        crate::OrderLineExpression::new(next, self.root_desc.clone())
    }

    pub fn get(&self, index: usize) -> crate::OrderLineExpression<'a> {
        let next = self.result.clone().and_then("get", |list| {
            if let Some(item) = list.get(index) {
                teaql_core::eval::EvalResult::Value(item)
            } else {
                teaql_core::eval::EvalResult::Null
            }
        });
        crate::OrderLineExpression::new(next, self.root_desc.clone())
    }
}