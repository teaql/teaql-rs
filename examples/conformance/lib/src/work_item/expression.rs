#[derive(Clone)]
pub struct WorkItemExpression<'a> {
    result: teaql_core::eval::EvalResult<&'a crate::WorkItem>,
    root_desc: std::sync::Arc<String>,
}

impl<'a> WorkItemExpression<'a> {
    pub fn new(result: teaql_core::eval::EvalResult<&'a crate::WorkItem>, root_desc: std::sync::Arc<String>) -> Self {
        Self { result, root_desc }
    }

    fn resolve(&self) -> Option<&'a crate::WorkItem> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(v) => Some(*v),
            teaql_core::eval::EvalResult::Null => None,
            teaql_core::eval::EvalResult::NotLoaded { failed_node, attempted_path } => {
                crate::trigger_logic_bug_panic(&self.root_desc, &failed_node, &attempted_path)
            }
        }
    }

    pub fn eval(&self) -> Option<&'a crate::WorkItem> {
        self.resolve()
    }

    pub fn unwrap(&self) -> &'a crate::WorkItem {
        self.resolve().expect("Relation was legitimately null in database!")
    }

    pub fn get_id(self) -> crate::ValueExpression<'a, u64> {
        let next = self.result.and_then("id", |entity| entity.eval_id());
        crate::ValueExpression::new(next, self.root_desc.clone())
    }

    pub fn get_title(self) -> crate::ValueExpression<'a, String> {
        let next = self.result.and_then("title", |entity| entity.eval_title());
        crate::ValueExpression::new(next, self.root_desc.clone())
    }

    pub fn get_description(self) -> crate::ValueExpression<'a, Option<String>> {
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
}

#[derive(Clone)]
pub struct WorkItemListExpression<'a> {
    result: teaql_core::eval::EvalResult<&'a teaql_core::SmartList<crate::WorkItem>>,
    root_desc: std::sync::Arc<String>,
}

impl<'a> WorkItemListExpression<'a> {
    pub fn new(result: teaql_core::eval::EvalResult<&'a teaql_core::SmartList<crate::WorkItem>>, root_desc: std::sync::Arc<String>) -> Self {
        Self { result, root_desc }
    }

    fn resolve(&self) -> Option<&'a teaql_core::SmartList<crate::WorkItem>> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(v) => Some(*v),
            teaql_core::eval::EvalResult::Null => None,
            teaql_core::eval::EvalResult::NotLoaded { failed_node, attempted_path } => {
                crate::trigger_logic_bug_panic(&self.root_desc, &failed_node, &attempted_path)
            }
        }
    }

    pub fn eval(&self) -> Option<&'a teaql_core::SmartList<crate::WorkItem>> {
        self.resolve()
    }

    pub fn unwrap(&self) -> &'a teaql_core::SmartList<crate::WorkItem> {
        self.resolve().expect("List relation was legitimately null in database!")
    }

    pub fn size(&self) -> crate::ValueExpression<'a, usize> {
        let next = self.result.clone().and_then("size", |list| teaql_core::eval::EvalResult::Value(list.len()));
        crate::ValueExpression::new(next, self.root_desc.clone())
    }

    pub fn first(&self) -> crate::WorkItemExpression<'a> {
        let next = self.result.clone().and_then("first", |list| {
            if let Some(item) = list.first() {
                teaql_core::eval::EvalResult::Value(item)
            } else {
                teaql_core::eval::EvalResult::Null
            }
        });
        crate::WorkItemExpression::new(next, self.root_desc.clone())
    }

    pub fn get(&self, index: usize) -> crate::WorkItemExpression<'a> {
        let next = self.result.clone().and_then("get", |list| {
            if let Some(item) = list.get(index) {
                teaql_core::eval::EvalResult::Value(item)
            } else {
                teaql_core::eval::EvalResult::Null
            }
        });
        crate::WorkItemExpression::new(next, self.root_desc.clone())
    }
}