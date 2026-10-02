#[derive(Clone)]
pub struct CustomerOrderExpression<'a> {
    result: teaql_core::eval::EvalResult<&'a crate::CustomerOrder>,
    root_desc: std::sync::Arc<String>,
}

impl<'a> CustomerOrderExpression<'a> {
    pub fn new(result: teaql_core::eval::EvalResult<&'a crate::CustomerOrder>, root_desc: std::sync::Arc<String>) -> Self {
        Self { result, root_desc }
    }

    fn resolve(&self) -> Option<&'a crate::CustomerOrder> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(v) => Some(*v),
            teaql_core::eval::EvalResult::Null => None,
            teaql_core::eval::EvalResult::NotLoaded { failed_node, attempted_path } => {
                crate::trigger_logic_bug_panic(&self.root_desc, &failed_node, &attempted_path)
            }
        }
    }

    pub fn eval(&self) -> Option<&'a crate::CustomerOrder> {
        self.resolve()
    }

    pub fn unwrap(&self) -> &'a crate::CustomerOrder {
        self.resolve().expect("Relation was legitimately null in database!")
    }

    pub fn get_id(self) -> crate::ValueExpression<'a, u64> {
        let next = self.result.and_then("id", |entity| entity.eval_id());
        crate::ValueExpression::new(next, self.root_desc.clone())
    }

    pub fn get_order_number(self) -> crate::ValueExpression<'a, String> {
        let next = self.result.and_then("order_number", |entity| entity.eval_order_number());
        crate::ValueExpression::new(next, self.root_desc.clone())
    }

    pub fn get_description(self) -> crate::ValueExpression<'a, String> {
        let next = self.result.and_then("description", |entity| entity.eval_description());
        crate::ValueExpression::new(next, self.root_desc.clone())
    }

    pub fn get_version(self) -> crate::ValueExpression<'a, i64> {
        let next = self.result.and_then("version", |entity| entity.eval_version());
        crate::ValueExpression::new(next, self.root_desc.clone())
    }
    pub fn get_platform_id(self) -> crate::ValueExpression<'a, u64> {
        let next = self.result.and_then("platform_id", |entity| entity.eval_platform_id());
        crate::ValueExpression::new(next, self.root_desc.clone())
    }
    pub fn get_platform(self) -> crate::PlatformExpression<'a> {
        let next = self.result.and_then("platform", |entity| entity.eval_platform());
        crate::PlatformExpression::new(next, self.root_desc.clone())
    }
    pub fn get_order_item_list(self) -> crate::OrderItemListExpression<'a> {
        let next = self.result.and_then("order_item_list", |entity| entity.eval_order_item_list());
        crate::OrderItemListExpression::new(next, self.root_desc.clone())
    }

    pub fn get_payment_list(self) -> crate::PaymentListExpression<'a> {
        let next = self.result.and_then("payment_list", |entity| entity.eval_payment_list());
        crate::PaymentListExpression::new(next, self.root_desc.clone())
    }

    pub fn get_shipment_list(self) -> crate::ShipmentListExpression<'a> {
        let next = self.result.and_then("shipment_list", |entity| entity.eval_shipment_list());
        crate::ShipmentListExpression::new(next, self.root_desc.clone())
    }
}

#[derive(Clone)]
pub struct CustomerOrderListExpression<'a> {
    result: teaql_core::eval::EvalResult<&'a teaql_core::SmartList<crate::CustomerOrder>>,
    root_desc: std::sync::Arc<String>,
}

impl<'a> CustomerOrderListExpression<'a> {
    pub fn new(result: teaql_core::eval::EvalResult<&'a teaql_core::SmartList<crate::CustomerOrder>>, root_desc: std::sync::Arc<String>) -> Self {
        Self { result, root_desc }
    }

    fn resolve(&self) -> Option<&'a teaql_core::SmartList<crate::CustomerOrder>> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(v) => Some(*v),
            teaql_core::eval::EvalResult::Null => None,
            teaql_core::eval::EvalResult::NotLoaded { failed_node, attempted_path } => {
                crate::trigger_logic_bug_panic(&self.root_desc, &failed_node, &attempted_path)
            }
        }
    }

    pub fn eval(&self) -> Option<&'a teaql_core::SmartList<crate::CustomerOrder>> {
        self.resolve()
    }

    pub fn unwrap(&self) -> &'a teaql_core::SmartList<crate::CustomerOrder> {
        self.resolve().expect("List relation was legitimately null in database!")
    }

    pub fn size(&self) -> crate::ValueExpression<'a, usize> {
        let next = self.result.clone().and_then("size", |list| teaql_core::eval::EvalResult::Value(list.len()));
        crate::ValueExpression::new(next, self.root_desc.clone())
    }

    pub fn first(&self) -> crate::CustomerOrderExpression<'a> {
        let next = self.result.clone().and_then("first", |list| {
            if let Some(item) = list.first() {
                teaql_core::eval::EvalResult::Value(item)
            } else {
                teaql_core::eval::EvalResult::Null
            }
        });
        crate::CustomerOrderExpression::new(next, self.root_desc.clone())
    }

    pub fn get(&self, index: usize) -> crate::CustomerOrderExpression<'a> {
        let next = self.result.clone().and_then("get", |list| {
            if let Some(item) = list.get(index) {
                teaql_core::eval::EvalResult::Value(item)
            } else {
                teaql_core::eval::EvalResult::Null
            }
        });
        crate::CustomerOrderExpression::new(next, self.root_desc.clone())
    }
}