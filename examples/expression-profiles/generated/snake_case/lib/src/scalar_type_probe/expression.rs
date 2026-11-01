#[derive(Clone)]
pub struct ScalarTypeProbeExpression<'a> {
    result: teaql_core::eval::EvalResult<&'a crate::ScalarTypeProbe>,
    root_desc: std::sync::Arc<String>,
    path: teaql_core::eval_location::ExpressionPath,
}

impl<'a> ScalarTypeProbeExpression<'a> {
    pub fn new(result: teaql_core::eval::EvalResult<&'a crate::ScalarTypeProbe>, root_desc: std::sync::Arc<String>) -> Self {
        Self { result, root_desc, path: Default::default() }
    }

    pub(crate) fn with_path(mut self, mut path: teaql_core::eval_location::ExpressionPath) -> Self { if matches!(&self.result, teaql_core::eval::EvalResult::NotLoaded { .. }) { path.mark_missing(); } self.path = path; self }

    fn resolve(&self, terminal: &'static str) -> Option<&'a crate::ScalarTypeProbe> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(v) => Some(*v),
            teaql_core::eval::EvalResult::Null => None,
            teaql_core::eval::EvalResult::NotLoaded { failed_node, attempted_path } => {
                crate::trigger_logic_bug_panic(&self.root_desc, &failed_node, &attempted_path, &self.path, terminal)
            }
        }
    }

    pub fn try_eval(&self) -> Result<Option<&'a crate::ScalarTypeProbe>, teaql_core::eval_location::NotLoadedDiagnostic> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(value) => Ok(Some(*value)),
            teaql_core::eval::EvalResult::Null => Ok(None),
            teaql_core::eval::EvalResult::NotLoaded { failed_node, .. } => Err(teaql_core::eval_location::NotLoadedDiagnostic::from_path(&self.path, failed_node, "try_eval()").with_profile(teaql_core::object_location::JsonFieldNamingProfile::from_model_value("snake_case").expect("validated model JSON naming"))),
        }
    }

    pub fn eval(&self) -> Option<&'a crate::ScalarTypeProbe> {
        self.resolve("eval()")
    }

    pub fn unwrap(&self) -> &'a crate::ScalarTypeProbe {
        self.resolve("unwrap()").expect("Relation was legitimately null in database!")
    }

    pub fn get_id(self) -> crate::ValueExpression<'a, u64> {
        let next = self.result.and_then("id", |entity| entity.eval_id());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("id", "get_id"))
    }

    pub fn get_required_string(self) -> crate::ValueExpression<'a, String> {
        let next = self.result.and_then("required_string", |entity| entity.eval_required_string());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("required_string", "get_required_string"))
    }

    pub fn get_optional_string(self) -> crate::ValueExpression<'a, Option<String>> {
        let next = self.result.and_then("optional_string", |entity| entity.eval_optional_string());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("optional_string", "get_optional_string"))
    }

    pub fn get_large_text(self) -> crate::ValueExpression<'a, Option<String>> {
        let next = self.result.and_then("large_text", |entity| entity.eval_large_text());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("large_text", "get_large_text"))
    }

    pub fn get_required_boolean(self) -> crate::ValueExpression<'a, bool> {
        let next = self.result.and_then("required_boolean", |entity| entity.eval_required_boolean());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("required_boolean", "get_required_boolean"))
    }

    pub fn get_optional_boolean(self) -> crate::ValueExpression<'a, Option<bool>> {
        let next = self.result.and_then("optional_boolean", |entity| entity.eval_optional_boolean());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("optional_boolean", "get_optional_boolean"))
    }

    pub fn get_required_integer(self) -> crate::ValueExpression<'a, i64> {
        let next = self.result.and_then("required_integer", |entity| entity.eval_required_integer());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("required_integer", "get_required_integer"))
    }

    pub fn get_optional_integer(self) -> crate::ValueExpression<'a, Option<i64>> {
        let next = self.result.and_then("optional_integer", |entity| entity.eval_optional_integer());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("optional_integer", "get_optional_integer"))
    }

    pub fn get_required_long(self) -> crate::ValueExpression<'a, i64> {
        let next = self.result.and_then("required_long", |entity| entity.eval_required_long());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("required_long", "get_required_long"))
    }

    pub fn get_optional_long(self) -> crate::ValueExpression<'a, Option<i64>> {
        let next = self.result.and_then("optional_long", |entity| entity.eval_optional_long());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("optional_long", "get_optional_long"))
    }

    pub fn get_required_decimal(self) -> crate::ValueExpression<'a, rust_decimal::Decimal> {
        let next = self.result.and_then("required_decimal", |entity| entity.eval_required_decimal());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("required_decimal", "get_required_decimal"))
    }

    pub fn get_optional_decimal(self) -> crate::ValueExpression<'a, Option<rust_decimal::Decimal>> {
        let next = self.result.and_then("optional_decimal", |entity| entity.eval_optional_decimal());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("optional_decimal", "get_optional_decimal"))
    }

    pub fn get_required_float(self) -> crate::ValueExpression<'a, f32> {
        let next = self.result.and_then("required_float", |entity| entity.eval_required_float());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("required_float", "get_required_float"))
    }

    pub fn get_optional_float(self) -> crate::ValueExpression<'a, Option<f32>> {
        let next = self.result.and_then("optional_float", |entity| entity.eval_optional_float());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("optional_float", "get_optional_float"))
    }

    pub fn get_required_double(self) -> crate::ValueExpression<'a, f64> {
        let next = self.result.and_then("required_double", |entity| entity.eval_required_double());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("required_double", "get_required_double"))
    }

    pub fn get_optional_double(self) -> crate::ValueExpression<'a, Option<f64>> {
        let next = self.result.and_then("optional_double", |entity| entity.eval_optional_double());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("optional_double", "get_optional_double"))
    }

    pub fn get_required_date(self) -> crate::ValueExpression<'a, chrono::NaiveDate> {
        let next = self.result.and_then("required_date", |entity| entity.eval_required_date());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("required_date", "get_required_date"))
    }

    pub fn get_optional_date(self) -> crate::ValueExpression<'a, Option<chrono::NaiveDate>> {
        let next = self.result.and_then("optional_date", |entity| entity.eval_optional_date());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("optional_date", "get_optional_date"))
    }

    pub fn get_required_time(self) -> crate::ValueExpression<'a, String> {
        let next = self.result.and_then("required_time", |entity| entity.eval_required_time());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("required_time", "get_required_time"))
    }

    pub fn get_optional_time(self) -> crate::ValueExpression<'a, Option<String>> {
        let next = self.result.and_then("optional_time", |entity| entity.eval_optional_time());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("optional_time", "get_optional_time"))
    }

    pub fn get_required_datetime(self) -> crate::ValueExpression<'a, teaql_core::time::Timestamp> {
        let next = self.result.and_then("required_datetime", |entity| entity.eval_required_datetime());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("required_datetime", "get_required_datetime"))
    }

    pub fn get_optional_datetime(self) -> crate::ValueExpression<'a, Option<teaql_core::time::Timestamp>> {
        let next = self.result.and_then("optional_datetime", |entity| entity.eval_optional_datetime());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("optional_datetime", "get_optional_datetime"))
    }

    pub fn get_create_time(self) -> crate::ValueExpression<'a, teaql_core::time::Timestamp> {
        let next = self.result.and_then("create_time", |entity| entity.eval_create_time());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("create_time", "get_create_time"))
    }

    pub fn get_update_time(self) -> crate::ValueExpression<'a, teaql_core::time::Timestamp> {
        let next = self.result.and_then("update_time", |entity| entity.eval_update_time());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("update_time", "get_update_time"))
    }

    pub fn get_version(self) -> crate::ValueExpression<'a, i64> {
        let next = self.result.and_then("version", |entity| entity.eval_version());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("version", "get_version"))
    }
    pub fn get_type_platform_id(self) -> crate::ValueExpression<'a, u64> {
        let next = self.result.and_then("type_platform_id", |entity| entity.eval_type_platform_id());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("type_platform", "get_type_platform_id"))
    }
    pub fn get_type_platform(self) -> crate::TypePlatformExpression<'a> {
        let next = self.result.and_then("type_platform", |entity| entity.eval_type_platform());
        crate::TypePlatformExpression::new(next, self.root_desc.clone()).with_path(self.path.member("type_platform"))
    }
}

#[derive(Clone)]
pub struct ScalarTypeProbeListExpression<'a> {
    result: teaql_core::eval::EvalResult<&'a teaql_core::SmartList<crate::ScalarTypeProbe>>,
    root_desc: std::sync::Arc<String>,
    path: teaql_core::eval_location::ExpressionPath,
}

impl<'a> ScalarTypeProbeListExpression<'a> {
    pub fn new(result: teaql_core::eval::EvalResult<&'a teaql_core::SmartList<crate::ScalarTypeProbe>>, root_desc: std::sync::Arc<String>) -> Self {
        Self { result, root_desc, path: Default::default() }
    }

    pub(crate) fn with_path(mut self, mut path: teaql_core::eval_location::ExpressionPath) -> Self { if matches!(&self.result, teaql_core::eval::EvalResult::NotLoaded { .. }) { path.mark_missing(); } self.path = path; self }

    fn resolve(&self, terminal: &'static str) -> Option<&'a teaql_core::SmartList<crate::ScalarTypeProbe>> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(v) => Some(*v),
            teaql_core::eval::EvalResult::Null => None,
            teaql_core::eval::EvalResult::NotLoaded { failed_node, attempted_path } => {
                crate::trigger_logic_bug_panic(&self.root_desc, &failed_node, &attempted_path, &self.path, terminal)
            }
        }
    }

    pub fn try_eval(&self) -> Result<Option<&'a teaql_core::SmartList<crate::ScalarTypeProbe>>, teaql_core::eval_location::NotLoadedDiagnostic> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(value) => Ok(Some(*value)),
            teaql_core::eval::EvalResult::Null => Ok(None),
            teaql_core::eval::EvalResult::NotLoaded { failed_node, .. } => Err(teaql_core::eval_location::NotLoadedDiagnostic::from_path(&self.path, failed_node, "try_eval()").with_profile(teaql_core::object_location::JsonFieldNamingProfile::from_model_value("snake_case").expect("validated model JSON naming"))),
        }
    }

    pub fn eval(&self) -> Option<&'a teaql_core::SmartList<crate::ScalarTypeProbe>> {
        self.resolve("eval()")
    }

    pub fn unwrap(&self) -> &'a teaql_core::SmartList<crate::ScalarTypeProbe> {
        self.resolve("unwrap()").expect("List relation was legitimately null in database!")
    }

    pub fn size(&self) -> crate::ValueExpression<'a, usize> {
        let next = self.result.clone().and_then("size", |list| teaql_core::eval::EvalResult::Value(list.len()));
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.operation("size()"))
    }

    pub fn first(&self) -> crate::ScalarTypeProbeExpression<'a> {
        let next = self.result.clone().and_then("first", |list| {
            if let Some(item) = list.first() {
                teaql_core::eval::EvalResult::Value(item)
            } else {
                teaql_core::eval::EvalResult::Null
            }
        });
        crate::ScalarTypeProbeExpression::new(next, self.root_desc.clone()).with_path(self.path.first())
    }

    pub fn get(&self, index: usize) -> crate::ScalarTypeProbeExpression<'a> {
        let next = self.result.clone().and_then("get", |list| {
            if let Some(item) = list.get(index) {
                teaql_core::eval::EvalResult::Value(item)
            } else {
                teaql_core::eval::EvalResult::Null
            }
        });
        crate::ScalarTypeProbeExpression::new(next, self.root_desc.clone()).with_path(self.path.element(index))
    }
}