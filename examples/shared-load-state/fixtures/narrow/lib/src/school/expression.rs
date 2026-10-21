#[derive(Clone)]
pub struct SchoolExpression<'a> {
    result: teaql_core::eval::EvalResult<&'a crate::School>,
    root_desc: std::sync::Arc<String>,
    path: teaql_core::eval_location::ExpressionPath,
}

impl<'a> SchoolExpression<'a> {
    pub fn new(result: teaql_core::eval::EvalResult<&'a crate::School>, root_desc: std::sync::Arc<String>) -> Self {
        Self { result, root_desc, path: Default::default() }
    }

    pub(crate) fn with_path(mut self, mut path: teaql_core::eval_location::ExpressionPath) -> Self { if matches!(&self.result, teaql_core::eval::EvalResult::NotLoaded { .. }) { path.mark_missing(); } self.path = path; self }

    fn resolve(&self) -> Option<&'a crate::School> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(v) => Some(*v),
            teaql_core::eval::EvalResult::Null => None,
            teaql_core::eval::EvalResult::NotLoaded { failed_node, attempted_path } => {
                crate::trigger_logic_bug_panic(&self.root_desc, &failed_node, &attempted_path, &self.path)
            }
        }
    }

    pub fn try_eval(&self) -> Result<Option<&'a crate::School>, teaql_core::eval_location::NotLoadedDiagnostic> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(value) => Ok(Some(*value)),
            teaql_core::eval::EvalResult::Null => Ok(None),
            teaql_core::eval::EvalResult::NotLoaded { failed_node, .. } => Err(teaql_core::eval_location::NotLoadedDiagnostic::new(self.path.failure_location(), self.path.missing_member().unwrap_or(failed_node.as_str())).with_profile(teaql_core::object_location::JsonFieldNamingProfile::from_model_value("camelCase").expect("validated model JSON naming"))),
        }
    }

    pub fn eval(&self) -> Option<&'a crate::School> {
        self.resolve()
    }

    pub fn unwrap(&self) -> &'a crate::School {
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

    pub fn get_address(self) -> crate::ValueExpression<'a, String> {
        let next = self.result.and_then("address", |entity| entity.eval_address());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member("address"))
    }

    pub fn get_established_date(self) -> crate::ValueExpression<'a, chrono::NaiveDate> {
        let next = self.result.and_then("established_date", |entity| entity.eval_established_date());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member("established_date"))
    }

    pub fn get_student_capacity(self) -> crate::ValueExpression<'a, i64> {
        let next = self.result.and_then("student_capacity", |entity| entity.eval_student_capacity());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member("student_capacity"))
    }

    pub fn get_active(self) -> crate::ValueExpression<'a, bool> {
        let next = self.result.and_then("active", |entity| entity.eval_active());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member("active"))
    }

    pub fn get_create_time(self) -> crate::ValueExpression<'a, teaql_core::time::Timestamp> {
        let next = self.result.and_then("create_time", |entity| entity.eval_create_time());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member("create_time"))
    }

    pub fn get_update_time(self) -> crate::ValueExpression<'a, teaql_core::time::Timestamp> {
        let next = self.result.and_then("update_time", |entity| entity.eval_update_time());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member("update_time"))
    }

    pub fn get_version(self) -> crate::ValueExpression<'a, i64> {
        let next = self.result.and_then("version", |entity| entity.eval_version());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member("version"))
    }
    pub fn get_platform_id(self) -> crate::ValueExpression<'a, u64> {
        let next = self.result.and_then("platform_id", |entity| entity.eval_platform_id());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member("platform"))
    }

    pub fn get_school_type_id(self) -> crate::ValueExpression<'a, u64> {
        let next = self.result.and_then("school_type_id", |entity| entity.eval_school_type_id());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member("school_type"))
    }
    pub fn get_platform(self) -> crate::PlatformExpression<'a> {
        let next = self.result.and_then("platform", |entity| entity.eval_platform());
        crate::PlatformExpression::new(next, self.root_desc.clone()).with_path(self.path.member("platform"))
    }

    pub fn get_school_type(self) -> crate::SchoolTypeExpression<'a> {
        let next = self.result.and_then("school_type", |entity| entity.eval_school_type());
        crate::SchoolTypeExpression::new(next, self.root_desc.clone()).with_path(self.path.member("school_type"))
    }
    pub fn school_type_is_primary(self) -> crate::ValueExpression<'a, bool> {
        let next = self.result.and_then("school_type_id", |entity| {
            if !entity.is_loaded("school_type_id") {
                teaql_core::eval::EvalResult::NotLoaded { failed_node: "school_type_id".to_string(), attempted_path: "school_type_id".to_string() }
            } else {
                teaql_core::eval::EvalResult::Value(entity.school_type_is_primary())
            }
        });
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member("school_type"))
    }

    pub fn school_type_is_secondary(self) -> crate::ValueExpression<'a, bool> {
        let next = self.result.and_then("school_type_id", |entity| {
            if !entity.is_loaded("school_type_id") {
                teaql_core::eval::EvalResult::NotLoaded { failed_node: "school_type_id".to_string(), attempted_path: "school_type_id".to_string() }
            } else {
                teaql_core::eval::EvalResult::Value(entity.school_type_is_secondary())
            }
        });
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member("school_type"))
    }
}

#[derive(Clone)]
pub struct SchoolListExpression<'a> {
    result: teaql_core::eval::EvalResult<&'a teaql_core::SmartList<crate::School>>,
    root_desc: std::sync::Arc<String>,
    path: teaql_core::eval_location::ExpressionPath,
}

impl<'a> SchoolListExpression<'a> {
    pub fn new(result: teaql_core::eval::EvalResult<&'a teaql_core::SmartList<crate::School>>, root_desc: std::sync::Arc<String>) -> Self {
        Self { result, root_desc, path: Default::default() }
    }

    pub(crate) fn with_path(mut self, mut path: teaql_core::eval_location::ExpressionPath) -> Self { if matches!(&self.result, teaql_core::eval::EvalResult::NotLoaded { .. }) { path.mark_missing(); } self.path = path; self }

    fn resolve(&self) -> Option<&'a teaql_core::SmartList<crate::School>> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(v) => Some(*v),
            teaql_core::eval::EvalResult::Null => None,
            teaql_core::eval::EvalResult::NotLoaded { failed_node, attempted_path } => {
                crate::trigger_logic_bug_panic(&self.root_desc, &failed_node, &attempted_path, &self.path)
            }
        }
    }

    pub fn try_eval(&self) -> Result<Option<&'a teaql_core::SmartList<crate::School>>, teaql_core::eval_location::NotLoadedDiagnostic> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(value) => Ok(Some(*value)),
            teaql_core::eval::EvalResult::Null => Ok(None),
            teaql_core::eval::EvalResult::NotLoaded { failed_node, .. } => Err(teaql_core::eval_location::NotLoadedDiagnostic::new(self.path.failure_location(), self.path.missing_member().unwrap_or(failed_node.as_str())).with_profile(teaql_core::object_location::JsonFieldNamingProfile::from_model_value("camelCase").expect("validated model JSON naming"))),
        }
    }

    pub fn eval(&self) -> Option<&'a teaql_core::SmartList<crate::School>> {
        self.resolve()
    }

    pub fn unwrap(&self) -> &'a teaql_core::SmartList<crate::School> {
        self.resolve().expect("List relation was legitimately null in database!")
    }

    pub fn size(&self) -> crate::ValueExpression<'a, usize> {
        let next = self.result.clone().and_then("size", |list| teaql_core::eval::EvalResult::Value(list.len()));
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.clone())
    }

    pub fn first(&self) -> crate::SchoolExpression<'a> {
        let next = self.result.clone().and_then("first", |list| {
            if let Some(item) = list.first() {
                teaql_core::eval::EvalResult::Value(item)
            } else {
                teaql_core::eval::EvalResult::Null
            }
        });
        crate::SchoolExpression::new(next, self.root_desc.clone()).with_path(self.path.element(0))
    }

    pub fn get(&self, index: usize) -> crate::SchoolExpression<'a> {
        let next = self.result.clone().and_then("get", |list| {
            if let Some(item) = list.get(index) {
                teaql_core::eval::EvalResult::Value(item)
            } else {
                teaql_core::eval::EvalResult::Null
            }
        });
        crate::SchoolExpression::new(next, self.root_desc.clone()).with_path(self.path.element(index))
    }
}