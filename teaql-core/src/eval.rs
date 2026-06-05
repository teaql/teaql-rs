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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    struct Company {
        pub name: Option<String>,
        pub __load_state: LoadState,
    }

    impl Company {
        fn eval_name(&self) -> EvalResult<&str> {
            if !self.__load_state.is_loaded("name") {
                EvalResult::NotLoaded { missing_path: "name".to_string() }
            } else {
                match &self.name {
                    Some(n) => EvalResult::Value(n.as_str()),
                    None => EvalResult::Null,
                }
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
                EvalResult::NotLoaded { missing_path: "company".to_string() }
            } else {
                match &self.company {
                    Some(c) => EvalResult::Value(c.as_ref()),
                    None => EvalResult::Null,
                }
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
                EvalResult::NotLoaded { missing_path: "platform".to_string() }
            } else {
                match &self.platform {
                    Some(p) => EvalResult::Value(p.as_ref()),
                    None => EvalResult::Null,
                }
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
        let result = user.eval_platform()
            .and_then("platform", |p| p.eval_company().and_then("company", |c| c.eval_name()));

        // We expect it to fail exactly at "name" and bubble up the path!
        match result {
            EvalResult::NotLoaded { missing_path } => {
                assert_eq!(missing_path, "platform.company.name");
                println!("Success! Intercepted missing path: {}", missing_path);
            }
            _ => panic!("Expected NotLoaded but got {:?}", result),
        }
    }

    #[test]
    fn test_eval_tracking_chain_middle_break() {
        // If the platform exists, but company itself wasn't loaded
        let platform = Platform {
            company: None, // No data
            __load_state: LoadState::NotLoaded, // Missing loaded state for company
        };

        let user = User {
            platform: Some(Box::new(platform)),
            __load_state: LoadState::FullyLoaded,
        };

        let result = user.eval_platform()
            .and_then("platform", |p| p.eval_company().and_then("company", |c| c.eval_name()));

        match result {
            EvalResult::NotLoaded { missing_path } => {
                assert_eq!(missing_path, "platform.company");
                println!("Success! Intercepted middle missing path: {}", missing_path);
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

        let result = user.eval_platform()
            .and_then("platform", |p| p.eval_company().and_then("company", |c| c.eval_name()));

        match result {
            EvalResult::Null => {
                println!("Success! Legitimately empty (Null), not an error.");
            }
            _ => panic!("Expected Null"),
        }
    }
}
