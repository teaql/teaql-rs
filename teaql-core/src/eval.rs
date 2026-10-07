use serde::{Deserialize, Serialize};

/// The load state metadata hidden inside an entity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum LoadState {
    #[default]
    NotLoaded,
    Partial(std::collections::HashSet<String>),
    /// Compact generated-entity representation. Known fields borrow their generated static
    /// names; only genuinely dynamic projection aliases need to own a string.
    PartialCompact(smallvec::SmallVec<[std::borrow::Cow<'static, str>; 8]>),
    /// Column layout shared by every row in one database result set.
    SharedColumns(std::sync::Arc<[String]>),
    /// Runtime-only validated fixed layout. Never deserialize internal masks from a client.
    #[serde(skip)]
    Indexed(std::sync::Arc<crate::LoadedSnapshot>),
    FullyLoaded,
}

impl LoadState {
    pub fn into_indexed(self, layout: std::sync::Arc<crate::FieldLayout>) -> Result<Self, String> {
        let state = match self {
            Self::Indexed(state) => {
                if !std::sync::Arc::ptr_eq(state.layout(), &layout) {
                    return Err("incompatible loaded-state type or layout revision".to_owned());
                }
                return Ok(Self::Indexed(state));
            }
            Self::FullyLoaded => crate::LoadedSnapshot::fully_loaded(layout),
            Self::NotLoaded => crate::LoadedSnapshot::projection(layout, std::iter::empty()),
            Self::Partial(fields) => {
                crate::LoadedSnapshot::projection(layout, fields.iter().map(String::as_str))
            }
            Self::PartialCompact(fields) => {
                crate::LoadedSnapshot::projection(layout, fields.iter().map(|field| field.as_ref()))
            }
            Self::SharedColumns(fields) => {
                crate::LoadedSnapshot::projection(layout, fields.iter().map(String::as_str))
            }
        };
        Ok(Self::Indexed(state.into_shared()))
    }

    pub fn is_loaded(&self, field_or_relation: &str) -> bool {
        match self {
            LoadState::NotLoaded => false,
            LoadState::FullyLoaded => true,
            LoadState::Partial(set) => set.contains(field_or_relation),
            LoadState::PartialCompact(fields) => fields
                .iter()
                .any(|field| field.as_ref() == field_or_relation),
            LoadState::SharedColumns(columns) => {
                columns.iter().any(|column| column == field_or_relation)
            }
            LoadState::Indexed(state) => state.is_loaded(field_or_relation),
        }
    }

    pub fn mark_loaded(&mut self, field: &str) -> Result<(), String> {
        match self {
            Self::Indexed(state) => {
                *state = crate::LoadedSnapshot::with_loaded(state, field, true)?
            }
            Self::NotLoaded => {
                *self = Self::PartialCompact(smallvec::smallvec![field.to_owned().into()])
            }
            Self::Partial(fields) => {
                fields.insert(field.to_owned());
            }
            Self::PartialCompact(fields) => {
                if !fields.iter().any(|loaded| loaded.as_ref() == field) {
                    fields.push(field.to_owned().into());
                }
            }
            Self::SharedColumns(columns) => {
                if !columns.iter().any(|loaded| loaded == field) {
                    let mut next = columns.to_vec();
                    next.push(field.to_owned());
                    *columns = next.into();
                }
            }
            Self::FullyLoaded => {}
        }
        Ok(())
    }

    pub fn mark_unloaded(&mut self, field: &str) -> Result<(), String> {
        match self {
            Self::Indexed(state) => {
                *state = crate::LoadedSnapshot::with_loaded(state, field, false)?
            }
            Self::Partial(fields) => {
                fields.remove(field);
            }
            Self::PartialCompact(fields) => fields.retain(|loaded| loaded.as_ref() != field),
            Self::SharedColumns(columns) => {
                *columns = columns
                    .iter()
                    .filter(|name| name.as_str() != field)
                    .cloned()
                    .collect::<Vec<_>>()
                    .into()
            }
            Self::NotLoaded => {}
            Self::FullyLoaded => {
                return Err("clearing availability requires a generated layout".to_owned());
            }
        }
        Ok(())
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
    NotLoaded {
        failed_node: String,
        attempted_path: String,
    },
}

impl<T> EvalResult<T> {
    pub fn and_then<U, F: FnOnce(T) -> EvalResult<U>>(
        self,
        field_name: &str,
        f: F,
    ) -> EvalResult<U> {
        match self {
            EvalResult::Value(val) => match f(val) {
                EvalResult::NotLoaded {
                    failed_node,
                    attempted_path,
                } => {
                    let new_path = match (attempted_path == field_name, attempted_path.is_empty()) {
                        (true, _) => attempted_path,
                        (_, true) => field_name.to_string(),
                        _ => format!("{}.{}", field_name, attempted_path),
                    };
                    EvalResult::NotLoaded {
                        failed_node,
                        attempted_path: new_path,
                    }
                }
                other => other,
            },
            EvalResult::Null => EvalResult::Null,
            EvalResult::NotLoaded {
                failed_node,
                attempted_path,
            } => EvalResult::NotLoaded {
                failed_node,
                attempted_path,
            },
        }
    }

    pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> EvalResult<U> {
        match self {
            EvalResult::Value(val) => EvalResult::Value(f(val)),
            EvalResult::Null => EvalResult::Null,
            EvalResult::NotLoaded {
                failed_node,
                attempted_path,
            } => EvalResult::NotLoaded {
                failed_node,
                attempted_path,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Company {
        pub name: Option<String>,
        pub __load_state: LoadState,
    }

    impl Company {
        fn eval_name(&self) -> EvalResult<&str> {
            if !self.__load_state.is_loaded("name") {
                return EvalResult::NotLoaded {
                    failed_node: "name".to_string(),
                    attempted_path: "name".to_string(),
                };
            }
            match &self.name {
                Some(n) => EvalResult::Value(n.as_str()),
                None => EvalResult::Null,
            }
        }
    }

    struct Platform {
        pub company: Option<Box<Company>>,
        pub __load_state: LoadState,
    }

    impl Platform {
        fn eval_company(&self) -> EvalResult<&Company> {
            if !self.__load_state.is_loaded("company") {
                return EvalResult::NotLoaded {
                    failed_node: "company".to_string(),
                    attempted_path: "company".to_string(),
                };
            }
            match &self.company {
                Some(c) => EvalResult::Value(c.as_ref()),
                None => EvalResult::Null,
            }
        }
    }

    struct User {
        pub platform: Option<Box<Platform>>,
        pub __load_state: LoadState,
    }

    impl User {
        fn eval_platform(&self) -> EvalResult<&Platform> {
            if !self.__load_state.is_loaded("platform") {
                return EvalResult::NotLoaded {
                    failed_node: "platform".to_string(),
                    attempted_path: "platform".to_string(),
                };
            }
            match &self.platform {
                Some(p) => EvalResult::Value(p.as_ref()),
                None => EvalResult::Null,
            }
        }
    }

    #[test]
    fn test_eval_tracking_chain_perfect_path() {
        // Build the mocked entity graph:
        // User -> Platform -> Company
        // But we simulate a logic bug: Company is NOT fully loaded, its "name" is missing!

        let company = Company {
            name: None,
            // Company only partially loaded (doesn't include "name")
            __load_state: LoadState::NotLoaded,
        };

        let platform = Platform {
            company: Some(Box::new(company)),
            // Platform is fully loaded
            __load_state: LoadState::FullyLoaded,
        };

        let user = User {
            platform: Some(Box::new(platform)),
            // User is fully loaded
            __load_state: LoadState::FullyLoaded,
        };

        // Let's evaluate the expression: user.platform.company.name
        let result = user.eval_platform().and_then("platform", |p| {
            p.eval_company().and_then("company", |c| c.eval_name())
        });

        // We expect it to fail exactly at "name" and bubble up the path!
        match &result {
            EvalResult::NotLoaded { attempted_path, .. } => {
                assert_eq!(attempted_path, "platform.company.name");
                println!("\n\n>>> 【系统捕获到未加载异常】 <<<\n{:#?}\n\n", result);
            }
            _ => panic!("Expected NotLoaded but got {:?}", result),
        }
    }

    #[test]
    fn test_eval_tracking_chain_middle_break() {
        // If the platform exists, but company itself wasn't loaded
        let platform = Platform {
            company: None,                      // No data
            __load_state: LoadState::NotLoaded, // Missing loaded state for company
        };

        let user = User {
            platform: Some(Box::new(platform)),
            __load_state: LoadState::FullyLoaded,
        };

        let result = user.eval_platform().and_then("platform", |p| {
            p.eval_company().and_then("company", |c| c.eval_name())
        });

        match result {
            EvalResult::NotLoaded { attempted_path, .. } => {
                assert_eq!(attempted_path, "platform.company");
                println!(
                    "Success! Intercepted middle missing path: {}",
                    attempted_path
                );
            }
            _ => panic!("Expected NotLoaded"),
        }
    }

    #[test]
    fn test_eval_tracking_chain_normal_null() {
        // If the platform exists, company is fully loaded, but its name is truly empty (NULL in DB)
        let company = Company {
            name: None, // Real database null
            __load_state: LoadState::FullyLoaded,
        };

        let platform = Platform {
            company: Some(Box::new(company)),
            __load_state: LoadState::FullyLoaded,
        };

        let user = User {
            platform: Some(Box::new(platform)),
            __load_state: LoadState::FullyLoaded,
        };

        let result = user.eval_platform().and_then("platform", |p| {
            p.eval_company().and_then("company", |c| c.eval_name())
        });

        match result {
            EvalResult::Null => {
                println!("Success! Legitimately empty (Null), not an error.");
            }
            _ => panic!("Expected Null"),
        }
    }
}
