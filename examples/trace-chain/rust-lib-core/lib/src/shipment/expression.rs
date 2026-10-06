#[derive(Clone)]
pub struct ShipmentExpression<'a> {
    result: teaql_core::eval::EvalResult<&'a crate::Shipment>,
    root_desc: std::sync::Arc<String>,
}

impl<'a> ShipmentExpression<'a> {
    pub fn new(result: teaql_core::eval::EvalResult<&'a crate::Shipment>, root_desc: std::sync::Arc<String>) -> Self {
        Self { result, root_desc }
    }

    fn resolve(&self) -> Option<&'a crate::Shipment> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(v) => Some(*v),
            teaql_core::eval::EvalResult::Null => None,
            teaql_core::eval::EvalResult::NotLoaded { failed_node, attempted_path } => {
                crate::trigger_logic_bug_panic(&self.root_desc, &failed_node, &attempted_path)
            }
        }
    }

    pub fn eval(&self) -> Option<&'a crate::Shipment> {
        self.resolve()
    }

    pub fn unwrap(&self) -> &'a crate::Shipment {
        self.resolve().expect("Relation was legitimately null in database!")
    }

    pub fn get_id(self) -> crate::ValueExpression<'a, u64> {
        let next = self.result.and_then("id", |entity| entity.eval_id());
        crate::ValueExpression::new(next, self.root_desc.clone())
    }

    pub fn get_reference_code(self) -> crate::ValueExpression<'a, String> {
        let next = self.result.and_then("reference_code", |entity| entity.eval_reference_code());
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
    pub fn get_customer_order(self) -> crate::CustomerOrderExpression<'a> {
        let next = self.result.and_then("customer_order", |entity| entity.eval_customer_order());
        crate::CustomerOrderExpression::new(next, self.root_desc.clone())
    }
}

#[derive(Clone)]
pub struct ShipmentListExpression<'a> {
    result: teaql_core::eval::EvalResult<&'a teaql_core::SmartList<crate::Shipment>>,
    root_desc: std::sync::Arc<String>,
}

impl<'a> ShipmentListExpression<'a> {
    pub fn new(result: teaql_core::eval::EvalResult<&'a teaql_core::SmartList<crate::Shipment>>, root_desc: std::sync::Arc<String>) -> Self {
        Self { result, root_desc }
    }

    fn resolve(&self) -> Option<&'a teaql_core::SmartList<crate::Shipment>> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(v) => Some(*v),
            teaql_core::eval::EvalResult::Null => None,
            teaql_core::eval::EvalResult::NotLoaded { failed_node, attempted_path } => {
                crate::trigger_logic_bug_panic(&self.root_desc, &failed_node, &attempted_path)
            }
        }
    }

    pub fn eval(&self) -> Option<&'a teaql_core::SmartList<crate::Shipment>> {
        self.resolve()
    }

    pub fn unwrap(&self) -> &'a teaql_core::SmartList<crate::Shipment> {
        self.resolve().expect("List relation was legitimately null in database!")
    }

    pub fn size(&self) -> crate::ValueExpression<'a, usize> {
        let next = self.result.clone().and_then("size", |list| teaql_core::eval::EvalResult::Value(list.len()));
        crate::ValueExpression::new(next, self.root_desc.clone())
    }

    pub fn first(&self) -> crate::ShipmentExpression<'a> {
        let next = self.result.clone().and_then("first", |list| {
            if let Some(item) = list.first() {
                teaql_core::eval::EvalResult::Value(item)
            } else {
                teaql_core::eval::EvalResult::Null
            }
        });
        crate::ShipmentExpression::new(next, self.root_desc.clone())
    }

    pub fn get(&self, index: usize) -> crate::ShipmentExpression<'a> {
        let next = self.result.clone().and_then("get", |list| {
            if let Some(item) = list.get(index) {
                teaql_core::eval::EvalResult::Value(item)
            } else {
                teaql_core::eval::EvalResult::Null
            }
        });
        crate::ShipmentExpression::new(next, self.root_desc.clone())
    }
}