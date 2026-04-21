use std::sync::Arc;

use crate::{BaseEntity, SmartList, Value};

pub trait TeaqlEmpty {
    fn teaql_is_empty(&self) -> bool;
}

impl TeaqlEmpty for String {
    fn teaql_is_empty(&self) -> bool {
        self.is_empty()
    }
}

impl TeaqlEmpty for &str {
    fn teaql_is_empty(&self) -> bool {
        self.is_empty()
    }
}

impl<T> TeaqlEmpty for Vec<T> {
    fn teaql_is_empty(&self) -> bool {
        self.is_empty()
    }
}

impl<T> TeaqlEmpty for SmartList<T> {
    fn teaql_is_empty(&self) -> bool {
        self.is_empty()
    }
}

impl<T> TeaqlEmpty for Option<T> {
    fn teaql_is_empty(&self) -> bool {
        self.is_none()
    }
}

impl TeaqlEmpty for Value {
    fn teaql_is_empty(&self) -> bool {
        match self {
            Self::Null => true,
            Self::Text(value) => value.is_empty(),
            Self::List(values) => values.is_empty(),
            Self::Object(values) => values.is_empty(),
            Self::Json(serde_json::Value::Null) => true,
            Self::Json(serde_json::Value::String(value)) => value.is_empty(),
            Self::Json(serde_json::Value::Array(values)) => values.is_empty(),
            Self::Json(serde_json::Value::Object(values)) => values.is_empty(),
            _ => false,
        }
    }
}

#[derive(Clone)]
pub struct SafeExpression<R, T> {
    root: Arc<R>,
    evaluator: Arc<dyn Fn(&R) -> Option<T> + Send + Sync>,
}

impl<R, T> SafeExpression<R, T>
where
    R: Send + Sync + 'static,
    T: 'static,
{
    pub fn new(root: R, evaluator: impl Fn(&R) -> Option<T> + Send + Sync + 'static) -> Self {
        Self {
            root: Arc::new(root),
            evaluator: Arc::new(evaluator),
        }
    }

    pub fn eval(&self) -> Option<T> {
        (self.evaluator)(&self.root)
    }

    pub fn eval_with(&self, root: &R) -> Option<T> {
        (self.evaluator)(root)
    }

    pub fn apply<U>(self, mapper: impl Fn(T) -> U + Send + Sync + 'static) -> SafeExpression<R, U>
    where
        U: 'static,
    {
        self.apply_optional(move |value| Some(mapper(value)))
    }

    pub fn apply_optional<U>(
        self,
        mapper: impl Fn(T) -> Option<U> + Send + Sync + 'static,
    ) -> SafeExpression<R, U>
    where
        U: 'static,
    {
        let root = Arc::clone(&self.root);
        let evaluator = Arc::clone(&self.evaluator);
        SafeExpression {
            root,
            evaluator: Arc::new(move |root| evaluator(root).and_then(&mapper)),
        }
    }

    pub fn or_else(&self, default_value: T) -> T {
        self.eval().unwrap_or(default_value)
    }

    pub fn or_else_with(&self, default_value: impl FnOnce() -> T) -> T {
        self.eval().unwrap_or_else(default_value)
    }

    pub fn or_else_throw<E>(&self, error: impl FnOnce() -> E) -> Result<T, E> {
        self.eval().ok_or_else(error)
    }

    pub fn is_null(&self) -> bool {
        self.eval().is_none()
    }

    pub fn is_not_null(&self) -> bool {
        self.eval().is_some()
    }

    pub fn is_empty(&self) -> bool
    where
        T: TeaqlEmpty,
    {
        self.eval()
            .map(|value| value.teaql_is_empty())
            .unwrap_or(true)
    }

    pub fn is_not_empty(&self) -> bool
    where
        T: TeaqlEmpty,
    {
        !self.is_empty()
    }

    pub fn when_is_null(&self, function: impl FnOnce()) {
        if self.is_null() {
            function();
        }
    }

    pub fn when_is_not_null(&self, consumer: impl FnOnce(T)) {
        if let Some(value) = self.eval() {
            consumer(value);
        }
    }

    pub fn when_is_empty(&self, function: impl FnOnce())
    where
        T: TeaqlEmpty,
    {
        if self.is_empty() {
            function();
        }
    }

    pub fn when_not_empty(&self, consumer: impl FnOnce(T))
    where
        T: TeaqlEmpty,
    {
        if let Some(value) = self.eval().filter(|value| !value.teaql_is_empty()) {
            consumer(value);
        }
    }
}

impl<R> SafeExpression<R, R>
where
    R: Clone + Send + Sync + 'static,
{
    pub fn value(root: R) -> Self {
        Self::new(root, |root| Some(root.clone()))
    }

    pub fn root(&self) -> &R {
        &self.root
    }
}

impl<R, E> SafeExpression<R, E>
where
    R: Send + Sync + 'static,
    E: BaseEntity + Clone + 'static,
{
    pub fn entity_id(self) -> SafeExpression<R, u64> {
        self.apply_optional(|entity| entity.id())
    }

    pub fn entity_version(self) -> SafeExpression<R, i64> {
        self.apply(|entity| entity.version_value())
    }

    pub fn update_entity_id(self, id: u64) -> SafeExpression<R, E> {
        self.apply(move |mut entity| {
            entity.set_id(id);
            entity
        })
    }
}

impl<R, T> SafeExpression<R, SmartList<T>>
where
    R: Send + Sync + 'static,
    T: Clone + 'static,
{
    pub fn size(self) -> SafeExpression<R, usize> {
        self.apply(|list| list.len())
    }

    pub fn first(self) -> SafeExpression<R, T> {
        self.apply_optional(|list| list.first().cloned())
    }

    pub fn get(self, index: usize) -> SafeExpression<R, T> {
        self.apply_optional(move |list| list.get(index).cloned())
    }
}
