#[derive(Clone)]
pub struct OrderAttachmentExpression<'a> {
    result: teaql_core::eval::EvalResult<&'a crate::OrderAttachment>,
    root_desc: std::sync::Arc<String>,
    path: teaql_core::eval_location::ExpressionPath,
}

impl<'a> OrderAttachmentExpression<'a> {
    pub fn new(result: teaql_core::eval::EvalResult<&'a crate::OrderAttachment>, root_desc: std::sync::Arc<String>) -> Self {
        Self { result, root_desc, path: Default::default() }
    }

    pub(crate) fn with_path(mut self, mut path: teaql_core::eval_location::ExpressionPath) -> Self { if matches!(&self.result, teaql_core::eval::EvalResult::NotLoaded { .. }) { path.mark_missing(); } self.path = path; self }

    fn resolve(&self, terminal: &'static str) -> Option<&'a crate::OrderAttachment> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(v) => Some(*v),
            teaql_core::eval::EvalResult::Null => None,
            teaql_core::eval::EvalResult::NotLoaded { failed_node, attempted_path } => {
                crate::trigger_logic_bug_panic(&self.root_desc, &failed_node, &attempted_path, &self.path, terminal)
            }
        }
    }

    pub fn try_eval(&self) -> Result<Option<&'a crate::OrderAttachment>, teaql_core::eval_location::NotLoadedDiagnostic> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(value) => Ok(Some(*value)),
            teaql_core::eval::EvalResult::Null => Ok(None),
            teaql_core::eval::EvalResult::NotLoaded { failed_node, .. } => Err(teaql_core::eval_location::NotLoadedDiagnostic::from_path(&self.path, failed_node, "try_eval()").with_profile(teaql_core::object_location::JsonFieldNamingProfile::from_model_value("camelCase").expect("validated model JSON naming"))),
        }
    }

    pub fn eval(&self) -> Option<&'a crate::OrderAttachment> {
        self.resolve("eval()")
    }

    pub fn unwrap(&self) -> &'a crate::OrderAttachment {
        self.resolve("unwrap()").expect("Relation was legitimately null in database!")
    }

    pub fn get_id(self) -> crate::ValueExpression<'a, u64> {
        let next = self.result.and_then("id", |entity| entity.eval_id());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("id", "get_id"))
    }

    pub fn get_file_name(self) -> crate::ValueExpression<'a, String> {
        let next = self.result.and_then("file_name", |entity| entity.eval_file_name());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("file_name", "get_file_name"))
    }

    pub fn get_content_type(self) -> crate::ValueExpression<'a, String> {
        let next = self.result.and_then("content_type", |entity| entity.eval_content_type());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("content_type", "get_content_type"))
    }

    pub fn get_content_length(self) -> crate::ValueExpression<'a, i64> {
        let next = self.result.and_then("content_length", |entity| entity.eval_content_length());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("content_length", "get_content_length"))
    }

    pub fn get_content_sha256(self) -> crate::ValueExpression<'a, String> {
        let next = self.result.and_then("content_sha256", |entity| entity.eval_content_sha256());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("content_sha256", "get_content_sha256"))
    }

    pub fn get_object_key(self) -> crate::ValueExpression<'a, String> {
        let next = self.result.and_then("object_key", |entity| entity.eval_object_key());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("object_key", "get_object_key"))
    }

    pub fn get_version(self) -> crate::ValueExpression<'a, i64> {
        let next = self.result.and_then("version", |entity| entity.eval_version());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("version", "get_version"))
    }
    pub fn get_customer_order_id(self) -> crate::ValueExpression<'a, u64> {
        let next = self.result.and_then("customer_order_id", |entity| entity.eval_customer_order_id());
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.member_as("customer_order", "get_customer_order_id"))
    }
    pub fn get_customer_order(self) -> crate::CustomerOrderExpression<'a> {
        let next = self.result.and_then("customer_order", |entity| entity.eval_customer_order());
        crate::CustomerOrderExpression::new(next, self.root_desc.clone()).with_path(self.path.member("customer_order"))
    }
}

#[derive(Clone)]
pub struct OrderAttachmentListExpression<'a> {
    result: teaql_core::eval::EvalResult<&'a teaql_core::SmartList<crate::OrderAttachment>>,
    root_desc: std::sync::Arc<String>,
    path: teaql_core::eval_location::ExpressionPath,
}

impl<'a> OrderAttachmentListExpression<'a> {
    pub fn new(result: teaql_core::eval::EvalResult<&'a teaql_core::SmartList<crate::OrderAttachment>>, root_desc: std::sync::Arc<String>) -> Self {
        Self { result, root_desc, path: Default::default() }
    }

    pub(crate) fn with_path(mut self, mut path: teaql_core::eval_location::ExpressionPath) -> Self { if matches!(&self.result, teaql_core::eval::EvalResult::NotLoaded { .. }) { path.mark_missing(); } self.path = path; self }

    fn resolve(&self, terminal: &'static str) -> Option<&'a teaql_core::SmartList<crate::OrderAttachment>> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(v) => Some(*v),
            teaql_core::eval::EvalResult::Null => None,
            teaql_core::eval::EvalResult::NotLoaded { failed_node, attempted_path } => {
                crate::trigger_logic_bug_panic(&self.root_desc, &failed_node, &attempted_path, &self.path, terminal)
            }
        }
    }

    pub fn try_eval(&self) -> Result<Option<&'a teaql_core::SmartList<crate::OrderAttachment>>, teaql_core::eval_location::NotLoadedDiagnostic> {
        match &self.result {
            teaql_core::eval::EvalResult::Value(value) => Ok(Some(*value)),
            teaql_core::eval::EvalResult::Null => Ok(None),
            teaql_core::eval::EvalResult::NotLoaded { failed_node, .. } => Err(teaql_core::eval_location::NotLoadedDiagnostic::from_path(&self.path, failed_node, "try_eval()").with_profile(teaql_core::object_location::JsonFieldNamingProfile::from_model_value("camelCase").expect("validated model JSON naming"))),
        }
    }

    pub fn eval(&self) -> Option<&'a teaql_core::SmartList<crate::OrderAttachment>> {
        self.resolve("eval()")
    }

    pub fn unwrap(&self) -> &'a teaql_core::SmartList<crate::OrderAttachment> {
        self.resolve("unwrap()").expect("List relation was legitimately null in database!")
    }

    pub fn size(&self) -> crate::ValueExpression<'a, usize> {
        let next = self.result.clone().and_then("size", |list| teaql_core::eval::EvalResult::Value(list.len()));
        crate::ValueExpression::new(next, self.root_desc.clone()).with_path(self.path.operation("size()"))
    }

    pub fn first(&self) -> crate::OrderAttachmentExpression<'a> {
        let next = self.result.clone().and_then("first", |list| {
            if let Some(item) = list.first() {
                teaql_core::eval::EvalResult::Value(item)
            } else {
                teaql_core::eval::EvalResult::Null
            }
        });
        crate::OrderAttachmentExpression::new(next, self.root_desc.clone()).with_path(self.path.first())
    }

    pub fn get(&self, index: usize) -> crate::OrderAttachmentExpression<'a> {
        let next = self.result.clone().and_then("get", |list| {
            if let Some(item) = list.get(index) {
                teaql_core::eval::EvalResult::Value(item)
            } else {
                teaql_core::eval::EvalResult::Null
            }
        });
        crate::OrderAttachmentExpression::new(next, self.root_desc.clone()).with_path(self.path.element(index))
    }
}