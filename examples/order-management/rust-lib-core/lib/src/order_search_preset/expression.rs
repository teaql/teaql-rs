#[derive(Clone)]
pub struct OrderSearchPresetExpression<'a> {
    result: teaql_core::eval::EvalResult<&'a crate::OrderSearchPreset>,
    root_desc: std::sync::Arc<String>,
}

impl<'a> OrderSearchPresetExpression<'a> {
    pub fn new(result: teaql_core::eval::EvalResult<&'a crate::OrderSearchPreset>, root_desc: std::sync::Arc<String>) -> Self {
        Self { result, root_desc }
    }

    fn resolve(&self) -> Option<&'a crate::OrderSearchPreset> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(v) => Some(*v),
            teaql_core::eval::EvalResult::Null => None,
            teaql_core::eval::EvalResult::NotLoaded { failed_node, attempted_path } => {
                crate::trigger_logic_bug_panic(&self.root_desc, &failed_node, &attempted_path)
            }
        }
    }

    pub fn eval(&self) -> Option<&'a crate::OrderSearchPreset> {
        self.resolve()
    }

    pub fn unwrap(&self) -> &'a crate::OrderSearchPreset {
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

    pub fn get_filter_json(self) -> crate::ValueExpression<'a, String> {
        let next = self.result.and_then("filter_json", |entity| entity.eval_filter_json());
        crate::ValueExpression::new(next, self.root_desc.clone())
    }

    pub fn get_request_id(self) -> crate::ValueExpression<'a, String> {
        let next = self.result.and_then("request_id", |entity| entity.eval_request_id());
        crate::ValueExpression::new(next, self.root_desc.clone())
    }

    pub fn get_owner_user_id(self) -> crate::ValueExpression<'a, String> {
        let next = self.result.and_then("owner_user_id", |entity| entity.eval_owner_user_id());
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
    pub fn get_commerce_platform_id(self) -> crate::ValueExpression<'a, u64> {
        let next = self.result.and_then("commerce_platform_id", |entity| entity.eval_commerce_platform_id());
        crate::ValueExpression::new(next, self.root_desc.clone())
    }
    pub fn get_commerce_platform(self) -> crate::CommercePlatformExpression<'a> {
        let next = self.result.and_then("commerce_platform", |entity| entity.eval_commerce_platform());
        crate::CommercePlatformExpression::new(next, self.root_desc.clone())
    }
}

#[derive(Clone)]
pub struct OrderSearchPresetListExpression<'a> {
    result: teaql_core::eval::EvalResult<&'a teaql_core::SmartList<crate::OrderSearchPreset>>,
    root_desc: std::sync::Arc<String>,
}

impl<'a> OrderSearchPresetListExpression<'a> {
    pub fn new(result: teaql_core::eval::EvalResult<&'a teaql_core::SmartList<crate::OrderSearchPreset>>, root_desc: std::sync::Arc<String>) -> Self {
        Self { result, root_desc }
    }

    fn resolve(&self) -> Option<&'a teaql_core::SmartList<crate::OrderSearchPreset>> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(v) => Some(*v),
            teaql_core::eval::EvalResult::Null => None,
            teaql_core::eval::EvalResult::NotLoaded { failed_node, attempted_path } => {
                crate::trigger_logic_bug_panic(&self.root_desc, &failed_node, &attempted_path)
            }
        }
    }

    pub fn eval(&self) -> Option<&'a teaql_core::SmartList<crate::OrderSearchPreset>> {
        self.resolve()
    }

    pub fn unwrap(&self) -> &'a teaql_core::SmartList<crate::OrderSearchPreset> {
        self.resolve().expect("List relation was legitimately null in database!")
    }

    pub fn size(&self) -> crate::ValueExpression<'a, usize> {
        let next = self.result.clone().and_then("size", |list| teaql_core::eval::EvalResult::Value(list.len()));
        crate::ValueExpression::new(next, self.root_desc.clone())
    }

    pub fn first(&self) -> crate::OrderSearchPresetExpression<'a> {
        let next = self.result.clone().and_then("first", |list| {
            if let Some(item) = list.first() {
                teaql_core::eval::EvalResult::Value(item)
            } else {
                teaql_core::eval::EvalResult::Null
            }
        });
        crate::OrderSearchPresetExpression::new(next, self.root_desc.clone())
    }

    pub fn get(&self, index: usize) -> crate::OrderSearchPresetExpression<'a> {
        let next = self.result.clone().and_then("get", |list| {
            if let Some(item) = list.get(index) {
                teaql_core::eval::EvalResult::Value(item)
            } else {
                teaql_core::eval::EvalResult::Null
            }
        });
        crate::OrderSearchPresetExpression::new(next, self.root_desc.clone())
    }
}