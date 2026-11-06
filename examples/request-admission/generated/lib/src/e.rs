
// The `E` expression wrapper provides zero-cost AST traversal
// and will automatically panic if it encounters a NotLoaded error.
pub struct E;

impl E {
    pub fn tenant<'a>(value: &'a crate::Tenant) -> crate::TenantExpression<'a> {
        let root_desc = std::sync::Arc::new(format!("Tenant(id={})", value.id()));
        crate::TenantExpression::new(teaql_core::eval::EvalResult::Value(value), root_desc).with_path(teaql_core::eval_location::ExpressionPath::root("E::tenant(<root>)"))
    }

    pub fn purchase_order<'a>(value: &'a crate::PurchaseOrder) -> crate::PurchaseOrderExpression<'a> {
        let root_desc = std::sync::Arc::new(format!("PurchaseOrder(id={})", value.id()));
        crate::PurchaseOrderExpression::new(teaql_core::eval::EvalResult::Value(value), root_desc).with_path(teaql_core::eval_location::ExpressionPath::root("E::purchase_order(<root>)"))
    }

    pub fn order_item<'a>(value: &'a crate::OrderItem) -> crate::OrderItemExpression<'a> {
        let root_desc = std::sync::Arc::new(format!("OrderItem(id={})", value.id()));
        crate::OrderItemExpression::new(teaql_core::eval::EvalResult::Value(value), root_desc).with_path(teaql_core::eval_location::ExpressionPath::root("E::order_item(<root>)"))
    }
}


pub fn trigger_logic_bug_panic(_root_desc: &str, failed_node: &str, _attempted_path: &str, path: &teaql_core::eval_location::ExpressionPath, terminal: &'static str) -> ! {
    let diagnostic = teaql_core::eval_location::NotLoadedDiagnostic::from_path(path, failed_node, terminal)
        .with_profile(teaql_core::object_location::JsonFieldNamingProfile::from_model_value("camelCase").expect("validated model JSON naming"));
    // Render from generated native-call metadata, not the lossy EvalResult
    // display path. No guessed Q methods or entity values enter the diagnostic.
    // Structured callers use try_eval().map_err(|error| error.to_wire()).
    // A native panic is the human view: never embed JSON or repeat a breakpoint marker.
    panic!("\n💥 [Coding Logic Bug]\n{}\n", diagnostic);
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

    fn resolve(self, terminal: &'static str) -> Option<T> {
        match self.result {
            teaql_core::eval::EvalResult::Value(v) => Some(v),
            teaql_core::eval::EvalResult::Null => None,
            teaql_core::eval::EvalResult::NotLoaded { failed_node, attempted_path } => {
                crate::trigger_logic_bug_panic(&self.root_desc, &failed_node, &attempted_path, &self.path, terminal)
            }
        }
    }

    pub fn try_eval(self) -> Result<Option<T>, teaql_core::eval_location::NotLoadedDiagnostic> {
        match self.result {
            teaql_core::eval::EvalResult::Value(value) => Ok(Some(value)),
            teaql_core::eval::EvalResult::Null => Ok(None),
            teaql_core::eval::EvalResult::NotLoaded { failed_node, .. } => Err(teaql_core::eval_location::NotLoadedDiagnostic::from_path(&self.path, &failed_node, "try_eval()").with_profile(teaql_core::object_location::JsonFieldNamingProfile::from_model_value("camelCase").expect("validated model JSON naming"))),
        }
    }

    pub fn eval(self) -> Option<T> {
        self.resolve("eval()")
    }

    pub fn unwrap(self) -> T {
        self.resolve("unwrap()").expect("Value was legitimately null in database!")
    }

    /// Returns `default_value` only when the expression is loaded and null.
    ///
    /// # Panics
    ///
    /// Panics when a required field or relation is `NotLoaded`.
    pub fn or_if_null(self, default_value: T) -> T {
        self.resolve("or_if_null(<fallback>)").unwrap_or(default_value)
    }

    /// Lazily computes a fallback only when the expression is loaded and null.
    ///
    /// # Panics
    ///
    /// Panics when a required field or relation is `NotLoaded`.
    pub fn or_else_if_null(self, default_fn: impl FnOnce() -> T) -> T {
        self.resolve("or_else_if_null(<fallback>)").unwrap_or_else(default_fn)
    }

    /// Returns `T::default()` only when the expression is loaded and null.
    ///
    /// # Panics
    ///
    /// Panics when a required field or relation is `NotLoaded`.
    pub fn or_default_if_null(self) -> T where T: Default {
        self.resolve("or_default_if_null()").unwrap_or_default()
    }
}

