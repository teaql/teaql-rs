#[derive(Clone)]
pub struct EntryExpression<'a> {
    result: teaql_core::eval::EvalResult<&'a crate::Entry>,
    root_desc: std::sync::Arc<String>,
    path: teaql_core::eval_location::ExpressionPath,
}

impl<'a> EntryExpression<'a> {
    pub fn new(result: teaql_core::eval::EvalResult<&'a crate::Entry>, root_desc: std::sync::Arc<String>) -> Self {
        Self { result, root_desc, path: Default::default() }
    }

    pub(crate) fn with_path(mut self, mut path: teaql_core::eval_location::ExpressionPath) -> Self { if matches!(&self.result, teaql_core::eval::EvalResult::NotLoaded { .. }) { path.mark_missing(); } self.path = path; self }

    fn resolve(&self, terminal: &'static str) -> Option<&'a crate::Entry> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(v) => Some(*v),
            teaql_core::eval::EvalResult::Null => None,
            teaql_core::eval::EvalResult::NotLoaded { failed_node, attempted_path } => {
                crate::trigger_logic_bug_panic(&self.root_desc, &failed_node, &attempted_path, &self.path, terminal)
            }
        }
    }

    pub fn try_eval(&self) -> Result<Option<&'a crate::Entry>, teaql_core::eval_location::NotLoadedDiagnostic> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(value) => Ok(Some(*value)),
            teaql_core::eval::EvalResult::Null => Ok(None),
            teaql_core::eval::EvalResult::NotLoaded { failed_node, .. } => Err(teaql_core::eval_location::NotLoadedDiagnostic::from_path(&self.path, failed_node, "try_eval()").with_profile(teaql_core::object_location::JsonFieldNamingProfile::from_model_value("camelCase").expect("validated model JSON naming"))),
        }
    }

    pub fn eval(&self) -> Option<&'a crate::Entry> {
        self.resolve("eval()")
    }

    pub fn unwrap(&self) -> &'a crate::Entry {
        self.resolve("unwrap()").expect("Relation was legitimately null in database!")
    }

    pub fn get_id(self) -> crate::ValueExpression<'a, u64> {
        let next = self.result.and_then("id", |entity| entity.eval_id());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("id", "get_id"))
    }

    pub fn get_name(self) -> crate::ValueExpression<'a, String> {
        let next = self.result.and_then("name", |entity| entity.eval_name());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("name", "get_name"))
    }

    pub fn get_born_at(self) -> crate::ValueExpression<'a, teaql_core::time::Timestamp> {
        let next = self.result.and_then("born_at", |entity| entity.eval_born_at());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("born_at", "get_born_at"))
    }

    pub fn get_touched_at(self) -> crate::ValueExpression<'a, teaql_core::time::Timestamp> {
        let next = self.result.and_then("touched_at", |entity| entity.eval_touched_at());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("touched_at", "get_touched_at"))
    }

    pub fn get_source_ip(self) -> crate::ValueExpression<'a, String> {
        let next = self.result.and_then("source_ip", |entity| entity.eval_source_ip());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("source_ip", "get_source_ip"))
    }

    pub fn get_last_ip(self) -> crate::ValueExpression<'a, String> {
        let next = self.result.and_then("last_ip", |entity| entity.eval_last_ip());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("last_ip", "get_last_ip"))
    }

    pub fn get_opened_by(self) -> crate::ValueExpression<'a, String> {
        let next = self.result.and_then("opened_by", |entity| entity.eval_opened_by());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("opened_by", "get_opened_by"))
    }

    pub fn get_changed_by(self) -> crate::ValueExpression<'a, String> {
        let next = self.result.and_then("changed_by", |entity| entity.eval_changed_by());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("changed_by", "get_changed_by"))
    }

    pub fn get_guarded_token(self) -> crate::ValueExpression<'a, String> {
        let next = self.result.and_then("guarded_token", |entity| entity.eval_guarded_token());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("guarded_token", "get_guarded_token"))
    }

    pub fn get_version(self) -> crate::ValueExpression<'a, i64> {
        let next = self.result.and_then("version", |entity| entity.eval_version());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("version", "get_version"))
    }
    pub fn get_catalog_id(self) -> crate::ValueExpression<'a, u64> {
        let next = self.result.and_then("catalog_id", |entity| entity.eval_catalog_id());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("catalog", "get_catalog_id"))
    }
    pub fn get_catalog(self) -> crate::CatalogExpression<'a> {
        let next = self.result.and_then("catalog", |entity| entity.eval_catalog());
        crate::CatalogExpression::new(next, self.root_desc.clone()).with_path(self.path.member("catalog"))
    }
}

#[derive(Clone)]
pub struct EntryListExpression<'a> {
    result: teaql_core::eval::EvalResult<&'a teaql_core::SmartList<crate::Entry>>,
    root_desc: std::sync::Arc<String>,
    path: teaql_core::eval_location::ExpressionPath,
}

impl<'a> EntryListExpression<'a> {
    pub fn new(result: teaql_core::eval::EvalResult<&'a teaql_core::SmartList<crate::Entry>>, root_desc: std::sync::Arc<String>) -> Self {
        Self { result, root_desc, path: Default::default() }
    }

    pub(crate) fn with_path(mut self, mut path: teaql_core::eval_location::ExpressionPath) -> Self { if matches!(&self.result, teaql_core::eval::EvalResult::NotLoaded { .. }) { path.mark_missing(); } self.path = path; self }

    fn resolve(&self, terminal: &'static str) -> Option<&'a teaql_core::SmartList<crate::Entry>> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(v) => Some(*v),
            teaql_core::eval::EvalResult::Null => None,
            teaql_core::eval::EvalResult::NotLoaded { failed_node, attempted_path } => {
                crate::trigger_logic_bug_panic(&self.root_desc, &failed_node, &attempted_path, &self.path, terminal)
            }
        }
    }

    pub fn try_eval(&self) -> Result<Option<&'a teaql_core::SmartList<crate::Entry>>, teaql_core::eval_location::NotLoadedDiagnostic> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(value) => Ok(Some(*value)),
            teaql_core::eval::EvalResult::Null => Ok(None),
            teaql_core::eval::EvalResult::NotLoaded { failed_node, .. } => Err(teaql_core::eval_location::NotLoadedDiagnostic::from_path(&self.path, failed_node, "try_eval()").with_profile(teaql_core::object_location::JsonFieldNamingProfile::from_model_value("camelCase").expect("validated model JSON naming"))),
        }
    }

    pub fn eval(&self) -> Option<&'a teaql_core::SmartList<crate::Entry>> {
        self.resolve("eval()")
    }

    pub fn unwrap(&self) -> &'a teaql_core::SmartList<crate::Entry> {
        self.resolve("unwrap()").expect("List relation was legitimately null in database!")
    }

    pub fn size(&self) -> crate::ValueExpression<'a, usize> {
        let next = self.result.clone().and_then("size", |list| teaql_core::eval::EvalResult::Value(list.len()));
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.operation("size()"))
    }

    pub fn first(&self) -> crate::EntryExpression<'a> {
        let next = self.result.clone().and_then("first", |list| {
            if let Some(item) = list.first() {
                teaql_core::eval::EvalResult::Value(item)
            } else {
                teaql_core::eval::EvalResult::Null
            }
        });
        crate::EntryExpression::new(next, self.root_desc.clone()).with_path(self.path.first())
    }

    pub fn get(&self, index: usize) -> crate::EntryExpression<'a> {
        let next = self.result.clone().and_then("get", |list| {
            if let Some(item) = list.get(index) {
                teaql_core::eval::EvalResult::Value(item)
            } else {
                teaql_core::eval::EvalResult::Null
            }
        });
        crate::EntryExpression::new(next, self.root_desc.clone()).with_path(self.path.element(index))
    }
}