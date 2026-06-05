use serde::{Deserialize, Serialize};

/// The load state metadata hidden inside an entity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LoadState {
    NotLoaded,
    Partial(std::collections::HashSet<String>),
    FullyLoaded,
}

impl Default for LoadState {
    fn default() -> Self {
        LoadState::NotLoaded
    }
}

impl LoadState {
    pub fn is_loaded(&self, field_or_relation: &str) -> bool {
        match self {
            LoadState::NotLoaded => false,
            LoadState::FullyLoaded => true,
            LoadState::Partial(set) => set.contains(field_or_relation),
        }
    }
}

/// A wrapper type for Expression API evaluation results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EvalResult<T> {
    /// Value is successfully loaded and present.
    Value(T),
    /// Value is loaded but it is legitimately Null.
    Null,
    /// Value is not loaded, trapping the evaluation path.
    NotLoaded { missing_path: String },
}

impl<T> EvalResult<T> {
    pub fn and_then<U, F: FnOnce(T) -> EvalResult<U>>(self, field_name: &str, f: F) -> EvalResult<U> {
        match self {
            EvalResult::Value(val) => match f(val) {
                EvalResult::NotLoaded { missing_path } => EvalResult::NotLoaded {
                    missing_path: format!("{}.{}", field_name, missing_path),
                },
                other => other,
            },
            EvalResult::Null => EvalResult::Null,
            EvalResult::NotLoaded { missing_path } => EvalResult::NotLoaded { missing_path },
        }
    }

    pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> EvalResult<U> {
        match self {
            EvalResult::Value(val) => EvalResult::Value(f(val)),
            EvalResult::Null => EvalResult::Null,
            EvalResult::NotLoaded { missing_path } => EvalResult::NotLoaded { missing_path },
        }
    }
}
