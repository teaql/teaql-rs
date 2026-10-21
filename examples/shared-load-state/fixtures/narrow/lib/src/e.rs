
// The `E` expression wrapper provides zero-cost AST traversal
// and will automatically panic if it encounters a NotLoaded error.
pub struct E;

impl E {
    pub fn platform<'a>(value: &'a crate::Platform) -> crate::PlatformExpression<'a> {
        let root_desc = std::sync::Arc::new(format!("Platform(id={})", value.id()));
        crate::PlatformExpression::new(teaql_core::eval::EvalResult::Value(value), root_desc)
    }

    pub fn school_type<'a>(value: &'a crate::SchoolType) -> crate::SchoolTypeExpression<'a> {
        let root_desc = std::sync::Arc::new(format!("SchoolType(id={})", value.id()));
        crate::SchoolTypeExpression::new(teaql_core::eval::EvalResult::Value(value), root_desc)
    }

    pub fn school<'a>(value: &'a crate::School) -> crate::SchoolExpression<'a> {
        let root_desc = std::sync::Arc::new(format!("School(id={})", value.id()));
        crate::SchoolExpression::new(teaql_core::eval::EvalResult::Value(value), root_desc)
    }

    pub fn school_capacity_summary<'a>(value: &'a crate::SchoolCapacitySummary) -> crate::SchoolCapacitySummaryExpression<'a> {
        let root_desc = std::sync::Arc::new(format!("SchoolCapacitySummary(id={})", value.id()));
        crate::SchoolCapacitySummaryExpression::new(teaql_core::eval::EvalResult::Value(value), root_desc)
    }
}


pub fn trigger_logic_bug_panic(root_desc: &str, failed_node: &str, attempted_path: &str, path: &teaql_core::eval_location::ExpressionPath) -> ! {
    let diagnostic = teaql_core::eval_location::NotLoadedDiagnostic::new(path.failure_location(), path.missing_member().unwrap_or(failed_node))
        .with_profile(teaql_core::object_location::JsonFieldNamingProfile::from_model_value("camelCase").expect("validated model JSON naming"));
    let wire = teaql_core::serde_json::to_string(&diagnostic.to_wire()).expect("serializable diagnostic");
    // Keep legacy preload hints as display only. The canonical location above
    // comes from typed traversal segments, never from parsing these hints.
    let parts: Vec<&str> = attempted_path.split('.').collect();
    let break_idx = parts.iter().position(|&p| p == failed_node).unwrap_or(0);

    let mut nested_fix = String::new();
    if break_idx < parts.len() - 1 {
        nested_fix.push_str(&format!("\"select_{}(", failed_node));
        let mut close_parens = 1;
        for i in (break_idx + 1)..parts.len() {
            let sub_field = parts[i];
            let prev_field = parts[i-1];
            let is_last = i == parts.len() - 1;
            if is_last {
                nested_fix.push_str(&format!("<generated Q entry for {}>.select_{}()", prev_field, sub_field));
            } else {
                nested_fix.push_str(&format!("<generated Q entry for {}>.select_{}(", prev_field, sub_field));
                close_parens += 1;
            }
        }
        for _ in 0..close_parens {
            nested_fix.push(')');
        }
        nested_fix.push('"');
    } else {
        nested_fix = "null".to_string();
    }

    let suggested_fix = format!("\"select_{}()\"", failed_node);

    let access_path_json = format!("[{}]", parts.iter().map(|s| format!("\"{}\"", s)).collect::<Vec<_>>().join(", "));
    let missing_preload_json = format!("[\"{}\"]", failed_node);

    let human_nested = if nested_fix != "null" { format!(" 或完整嵌套加载 {}", nested_fix) } else { String::new() };
    let root_name = root_desc.split('(').next().unwrap_or("Unknown");

    let mut root_snake = String::new();
    for (i, c) in root_name.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 {
                root_snake.push('_');
            }
            root_snake.push(c.to_ascii_lowercase());
        } else {
            root_snake.push(c);
        }
    }
    let id_part = root_desc.split('(').nth(1).unwrap_or(")");
    let mut original_expr = format!("E::{}({}", root_snake, id_part);
    for p in &parts {
        original_expr.push_str(&format!(".get_{}()", p));
        if *p == failed_node {
            original_expr.push_str("<broken>");
        }
    }

    let human_message = format!("\"访问 {}.{} 时缺少预加载。请在查询中加入 {}{}\"", root_name, attempted_path, suggested_fix, human_nested);

    panic!("\n\n💥 [Coding Logic Bug]\n\ndiagnostic: {}\noriginal_expr_with_broken_point: \"{}\"\nroot: {}\naccess_path: {}\nbreak_point: \"{}\"\nmissing_preload: {}\nsuggested_fix: {}\nnested_fix: {}\nseverity: \"error\"\nhuman_message: {}\n",
        wire, original_expr, root_desc, access_path_json, failed_node, missing_preload_json, suggested_fix, nested_fix, human_message);
}

#[derive(Clone)]
pub struct ValueExpression<'a, T> {
    result: teaql_core::eval::EvalResult<T>,
    root_desc: std::sync::Arc<String>,
    path: teaql_core::eval_location::ExpressionPath,
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl<'a, T: Clone> ValueExpression<'a, T> {
    pub fn new(result: teaql_core::eval::EvalResult<T>, root_desc: std::sync::Arc<String>) -> Self {
        Self { result, root_desc, path: Default::default(), _phantom: std::marker::PhantomData }
    }

    pub(crate) fn with_path(mut self, mut path: teaql_core::eval_location::ExpressionPath) -> Self { if matches!(&self.result, teaql_core::eval::EvalResult::NotLoaded { .. }) { path.mark_missing(); } self.path = path; self }

    fn resolve(self) -> Option<T> {
        match self.result {
            teaql_core::eval::EvalResult::Value(v) => Some(v),
            teaql_core::eval::EvalResult::Null => None,
            teaql_core::eval::EvalResult::NotLoaded { failed_node, attempted_path } => {
                crate::trigger_logic_bug_panic(&self.root_desc, &failed_node, &attempted_path, &self.path)
            }
        }
    }

    pub fn try_eval(self) -> Result<Option<T>, teaql_core::eval_location::NotLoadedDiagnostic> {
        match self.result {
            teaql_core::eval::EvalResult::Value(value) => Ok(Some(value)),
            teaql_core::eval::EvalResult::Null => Ok(None),
            teaql_core::eval::EvalResult::NotLoaded { failed_node, .. } => Err(teaql_core::eval_location::NotLoadedDiagnostic::new(self.path.failure_location(), self.path.missing_member().unwrap_or(failed_node.as_str())).with_profile(teaql_core::object_location::JsonFieldNamingProfile::from_model_value("camelCase").expect("validated model JSON naming"))),
        }
    }

    pub fn eval(self) -> Option<T> {
        self.resolve()
    }

    pub fn unwrap(self) -> T {
        self.resolve().expect("Value was legitimately null in database!")
    }

    /// Returns `default_value` only when the expression is loaded and null.
    ///
    /// # Panics
    ///
    /// Panics when a required field or relation is `NotLoaded`.
    pub fn or_if_null(self, default_value: T) -> T {
        self.eval().unwrap_or(default_value)
    }

    /// Lazily computes a fallback only when the expression is loaded and null.
    ///
    /// # Panics
    ///
    /// Panics when a required field or relation is `NotLoaded`.
    pub fn or_else_if_null(self, default_fn: impl FnOnce() -> T) -> T {
        self.eval().unwrap_or_else(default_fn)
    }

    /// Returns `T::default()` only when the expression is loaded and null.
    ///
    /// # Panics
    ///
    /// Panics when a required field or relation is `NotLoaded`.
    pub fn or_default_if_null(self) -> T where T: Default {
        self.eval().unwrap_or_default()
    }
}

