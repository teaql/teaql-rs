#[derive(Clone)]
pub struct SchoolCapacitySummaryExpression<'a> {
    result: teaql_core::eval::EvalResult<&'a crate::SchoolCapacitySummary>,
    root_desc: std::sync::Arc<String>,
    path: teaql_core::eval_location::ExpressionPath,
}

impl<'a> SchoolCapacitySummaryExpression<'a> {
    pub fn new(result: teaql_core::eval::EvalResult<&'a crate::SchoolCapacitySummary>, root_desc: std::sync::Arc<String>) -> Self {
        Self { result, root_desc, path: Default::default() }
    }

    pub(crate) fn with_path(mut self, mut path: teaql_core::eval_location::ExpressionPath) -> Self { if matches!(&self.result, teaql_core::eval::EvalResult::NotLoaded { .. }) { path.mark_missing(); } self.path = path; self }

    fn resolve(&self) -> Option<&'a crate::SchoolCapacitySummary> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(v) => Some(*v),
            teaql_core::eval::EvalResult::Null => None,
            teaql_core::eval::EvalResult::NotLoaded { failed_node, attempted_path } => {
                crate::trigger_logic_bug_panic(&self.root_desc, &failed_node, &attempted_path, &self.path)
            }
        }
    }

    pub fn try_eval(&self) -> Result<Option<&'a crate::SchoolCapacitySummary>, teaql_core::eval_location::NotLoadedDiagnostic> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(value) => Ok(Some(*value)),
            teaql_core::eval::EvalResult::Null => Ok(None),
            teaql_core::eval::EvalResult::NotLoaded { failed_node, .. } => Err(teaql_core::eval_location::NotLoadedDiagnostic::new(self.path.failure_location(), self.path.missing_member().unwrap_or(failed_node.as_str())).with_profile(teaql_core::object_location::JsonFieldNamingProfile::from_model_value("").expect("validated model JSON naming"))),
        }
    }

    pub fn eval(&self) -> Option<&'a crate::SchoolCapacitySummary> {
        self.resolve()
    }

    pub fn unwrap(&self) -> &'a crate::SchoolCapacitySummary {
        self.resolve().expect("Relation was legitimately null in database!")
    }

    pub fn get_id(self) -> crate::ValueExpression<'a, u64> {
        let next = self.result.and_then("id", |entity| entity.eval_id());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member("id"))
    }

    pub fn get_name(self) -> crate::ValueExpression<'a, String> {
        let next = self.result.and_then("name", |entity| entity.eval_name());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member("name"))
    }

    pub fn get_total_capacity(self) -> crate::ValueExpression<'a, i64> {
        let next = self.result.and_then("total_capacity", |entity| entity.eval_total_capacity());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member("total_capacity"))
    }

    pub fn get_school_count(self) -> crate::ValueExpression<'a, i64> {
        let next = self.result.and_then("school_count", |entity| entity.eval_school_count());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member("school_count"))
    }

    pub fn get_version(self) -> crate::ValueExpression<'a, i64> {
        let next = self.result.and_then("version", |entity| entity.eval_version());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member("version"))
    }
    pub fn get_platform_id(self) -> crate::ValueExpression<'a, u64> {
        let next = self.result.and_then("platform_id", |entity| entity.eval_platform_id());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member("platform"))
    }
    pub fn get_platform(self) -> crate::PlatformExpression<'a> {
        let next = self.result.and_then("platform", |entity| entity.eval_platform());
        crate::PlatformExpression::new(next, self.root_desc.clone()).with_path(self.path.member("platform"))
    }
}

#[derive(Clone)]
pub struct SchoolCapacitySummaryListExpression<'a> {
    result: teaql_core::eval::EvalResult<&'a teaql_core::SmartList<crate::SchoolCapacitySummary>>,
    root_desc: std::sync::Arc<String>,
    path: teaql_core::eval_location::ExpressionPath,
}

impl<'a> SchoolCapacitySummaryListExpression<'a> {
    pub fn new(result: teaql_core::eval::EvalResult<&'a teaql_core::SmartList<crate::SchoolCapacitySummary>>, root_desc: std::sync::Arc<String>) -> Self {
        Self { result, root_desc, path: Default::default() }
    }

    pub(crate) fn with_path(mut self, mut path: teaql_core::eval_location::ExpressionPath) -> Self { if matches!(&self.result, teaql_core::eval::EvalResult::NotLoaded { .. }) { path.mark_missing(); } self.path = path; self }

    fn resolve(&self) -> Option<&'a teaql_core::SmartList<crate::SchoolCapacitySummary>> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(v) => Some(*v),
            teaql_core::eval::EvalResult::Null => None,
            teaql_core::eval::EvalResult::NotLoaded { failed_node, attempted_path } => {
                crate::trigger_logic_bug_panic(&self.root_desc, &failed_node, &attempted_path, &self.path)
            }
        }
    }

    pub fn try_eval(&self) -> Result<Option<&'a teaql_core::SmartList<crate::SchoolCapacitySummary>>, teaql_core::eval_location::NotLoadedDiagnostic> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(value) => Ok(Some(*value)),
            teaql_core::eval::EvalResult::Null => Ok(None),
            teaql_core::eval::EvalResult::NotLoaded { failed_node, .. } => Err(teaql_core::eval_location::NotLoadedDiagnostic::new(self.path.failure_location(), self.path.missing_member().unwrap_or(failed_node.as_str())).with_profile(teaql_core::object_location::JsonFieldNamingProfile::from_model_value("").expect("validated model JSON naming"))),
        }
    }

    pub fn eval(&self) -> Option<&'a teaql_core::SmartList<crate::SchoolCapacitySummary>> {
        self.resolve()
    }

    pub fn unwrap(&self) -> &'a teaql_core::SmartList<crate::SchoolCapacitySummary> {
        self.resolve().expect("List relation was legitimately null in database!")
    }

    pub fn size(&self) -> crate::ValueExpression<'a, usize> {
        let next = self.result.clone().and_then("size", |list| teaql_core::eval::EvalResult::Value(list.len()));
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.clone())
    }

    pub fn first(&self) -> crate::SchoolCapacitySummaryExpression<'a> {
        let next = self.result.clone().and_then("first", |list| {
            if let Some(item) = list.first() {
                teaql_core::eval::EvalResult::Value(item)
            } else {
                teaql_core::eval::EvalResult::Null
            }
        });
        crate::SchoolCapacitySummaryExpression::new(next, self.root_desc.clone()).with_path(self.path.element(0))
    }

    pub fn get(&self, index: usize) -> crate::SchoolCapacitySummaryExpression<'a> {
        let next = self.result.clone().and_then("get", |list| {
            if let Some(item) = list.get(index) {
                teaql_core::eval::EvalResult::Value(item)
            } else {
                teaql_core::eval::EvalResult::Null
            }
        });
        crate::SchoolCapacitySummaryExpression::new(next, self.root_desc.clone()).with_path(self.path.element(index))
    }
}