#[derive(Clone)]
pub struct TypePlatformExpression<'a> {
    result: teaql_core::eval::EvalResult<&'a crate::TypePlatform>,
    root_desc: std::sync::Arc<String>,
    path: teaql_core::eval_location::ExpressionPath,
}

impl<'a> TypePlatformExpression<'a> {
    pub fn new(result: teaql_core::eval::EvalResult<&'a crate::TypePlatform>, root_desc: std::sync::Arc<String>) -> Self {
        Self { result, root_desc, path: Default::default() }
    }

    pub(crate) fn with_path(mut self, mut path: teaql_core::eval_location::ExpressionPath) -> Self { if matches!(&self.result, teaql_core::eval::EvalResult::NotLoaded { .. }) { path.mark_missing(); } self.path = path; self }

    fn resolve(&self, terminal: &'static str) -> Option<&'a crate::TypePlatform> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(v) => Some(*v),
            teaql_core::eval::EvalResult::Null => None,
            teaql_core::eval::EvalResult::NotLoaded { failed_node, attempted_path } => {
                crate::trigger_logic_bug_panic(&self.root_desc, &failed_node, &attempted_path, &self.path, terminal)
            }
        }
    }

    pub fn try_eval(&self) -> Result<Option<&'a crate::TypePlatform>, teaql_core::eval_location::NotLoadedDiagnostic> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(value) => Ok(Some(*value)),
            teaql_core::eval::EvalResult::Null => Ok(None),
            teaql_core::eval::EvalResult::NotLoaded { failed_node, .. } => Err(teaql_core::eval_location::NotLoadedDiagnostic::from_path(&self.path, failed_node, "try_eval()").with_profile(teaql_core::object_location::JsonFieldNamingProfile::from_model_value("PascalCase").expect("validated model JSON naming"))),
        }
    }

    pub fn eval(&self) -> Option<&'a crate::TypePlatform> {
        self.resolve("eval()")
    }

    pub fn unwrap(&self) -> &'a crate::TypePlatform {
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

    pub fn get_version(self) -> crate::ValueExpression<'a, i64> {
        let next = self.result.and_then("version", |entity| entity.eval_version());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("version", "get_version"))
    }
    pub fn get_scalar_type_probe_list(self) -> crate::ScalarTypeProbeListExpression<'a> {
        let next = self.result.and_then("scalar_type_probe_list", |entity| entity.eval_scalar_type_probe_list());
        crate::ScalarTypeProbeListExpression::new(next, self.root_desc.clone()).with_path(self.path.member("scalar_type_probe_list"))
    }
}

#[derive(Clone)]
pub struct TypePlatformListExpression<'a> {
    result: teaql_core::eval::EvalResult<&'a teaql_core::SmartList<crate::TypePlatform>>,
    root_desc: std::sync::Arc<String>,
    path: teaql_core::eval_location::ExpressionPath,
}

impl<'a> TypePlatformListExpression<'a> {
    pub fn new(result: teaql_core::eval::EvalResult<&'a teaql_core::SmartList<crate::TypePlatform>>, root_desc: std::sync::Arc<String>) -> Self {
        Self { result, root_desc, path: Default::default() }
    }

    pub(crate) fn with_path(mut self, mut path: teaql_core::eval_location::ExpressionPath) -> Self { if matches!(&self.result, teaql_core::eval::EvalResult::NotLoaded { .. }) { path.mark_missing(); } self.path = path; self }

    fn resolve(&self, terminal: &'static str) -> Option<&'a teaql_core::SmartList<crate::TypePlatform>> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(v) => Some(*v),
            teaql_core::eval::EvalResult::Null => None,
            teaql_core::eval::EvalResult::NotLoaded { failed_node, attempted_path } => {
                crate::trigger_logic_bug_panic(&self.root_desc, &failed_node, &attempted_path, &self.path, terminal)
            }
        }
    }

    pub fn try_eval(&self) -> Result<Option<&'a teaql_core::SmartList<crate::TypePlatform>>, teaql_core::eval_location::NotLoadedDiagnostic> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(value) => Ok(Some(*value)),
            teaql_core::eval::EvalResult::Null => Ok(None),
            teaql_core::eval::EvalResult::NotLoaded { failed_node, .. } => Err(teaql_core::eval_location::NotLoadedDiagnostic::from_path(&self.path, failed_node, "try_eval()").with_profile(teaql_core::object_location::JsonFieldNamingProfile::from_model_value("PascalCase").expect("validated model JSON naming"))),
        }
    }

    pub fn eval(&self) -> Option<&'a teaql_core::SmartList<crate::TypePlatform>> {
        self.resolve("eval()")
    }

    pub fn unwrap(&self) -> &'a teaql_core::SmartList<crate::TypePlatform> {
        self.resolve("unwrap()").expect("List relation was legitimately null in database!")
    }

    pub fn size(&self) -> crate::ValueExpression<'a, usize> {
        let next = self.result.clone().and_then("size", |list| teaql_core::eval::EvalResult::Value(list.len()));
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.operation("size()"))
    }

    pub fn first(&self) -> crate::TypePlatformExpression<'a> {
        let next = self.result.clone().and_then("first", |list| {
            if let Some(item) = list.first() {
                teaql_core::eval::EvalResult::Value(item)
            } else {
                teaql_core::eval::EvalResult::Null
            }
        });
        crate::TypePlatformExpression::new(next, self.root_desc.clone()).with_path(self.path.first())
    }

    pub fn get(&self, index: usize) -> crate::TypePlatformExpression<'a> {
        let next = self.result.clone().and_then("get", |list| {
            if let Some(item) = list.get(index) {
                teaql_core::eval::EvalResult::Value(item)
            } else {
                teaql_core::eval::EvalResult::Null
            }
        });
        crate::TypePlatformExpression::new(next, self.root_desc.clone()).with_path(self.path.element(index))
    }
}