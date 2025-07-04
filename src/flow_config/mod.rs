use std::{fmt::Debug, str::FromStr};

pub mod environment;
pub mod mode;

pub fn env_with_default<T: FromStr + Debug>(key: &str, default: T) -> T {
    match std::env::var(key) {
        Ok(string) => match T::from_str(&string) {
            Ok(value) => {
                log::info!("Found env: {} with value: {:?}", key, &value);
                value
            }
            Err(_) => {
                log::warn!("Failed to parse env: {} with value: {:?}", key, &string);
                default
            }
        },
        Err(_) => {
            log::warn!("Failed to find env: {}", key);
            default
        }
    }
}

pub fn load_env_file() {
    match dotenv::dotenv() {
        Ok(path) => log::info!("Found Env. file at {:?} ", path),
        Err(e) => log::error!("Failed to load .env file. Reason: {:?}", e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::env;

    #[test]
    #[serial]
    fn test_env_with_default_string_exists() {
        let key = "TEST_STRING_VAR";
        let expected = "test_value";

        unsafe {
            env::set_var(key, expected);
        }

        let result = env_with_default(key, "default".to_string());
        assert_eq!(result, expected);

        unsafe {
            env::remove_var(key);
        }
    }

    #[test]
    #[serial]
    fn test_env_with_default_string_missing() {
        let key = "TEST_MISSING_STRING_VAR";
        unsafe {
            env::remove_var(key);
        }

        let default = "default_value".to_string();
        let result = env_with_default(key, default.clone());
        assert_eq!(result, default);
    }

    #[test]
    #[serial]
    fn test_env_with_default_integer_exists() {
        let key = "TEST_INT_VAR";
        let expected = 42;
        unsafe {
            env::set_var(key, expected.to_string());
        }

        let result = env_with_default(key, 0i32);
        assert_eq!(result, expected);

        unsafe {
            env::remove_var(key);
        }
    }

    #[test]
    #[serial]
    fn test_env_with_default_integer_missing() {
        let key = "TEST_MISSING_INT_VAR";
        unsafe {
            env::remove_var(key);
        }

        let default = 123i32;
        let result = env_with_default(key, default);
        assert_eq!(result, default);
    }

    #[test]
    #[serial]
    fn test_env_with_default_boolean_exists_true() {
        let key = "TEST_BOOL_TRUE_VAR";
        unsafe {
            env::set_var(key, "true");
        }

        let result = env_with_default(key, false);
        assert_eq!(result, true);

        unsafe {
            env::remove_var(key);
        }
    }

    #[test]
    #[serial]
    fn test_env_with_default_boolean_exists_false() {
        let key = "TEST_BOOL_FALSE_VAR";
        unsafe {
            env::set_var(key, "false");
        }

        let result = env_with_default(key, true);
        assert_eq!(result, false);

        unsafe {
            env::remove_var(key);
        }
    }

    #[test]
    #[serial]
    fn test_env_with_default_boolean_missing() {
        let key = "TEST_MISSING_BOOL_VAR";
        unsafe {
            env::remove_var(key);
        }

        let default = true;
        let result = env_with_default(key, default);
        assert_eq!(result, default);
    }

    #[test]
    #[serial]
    fn test_env_with_default_boolean_invalid() {
        let key = "TEST_INVALID_BOOL_VAR";
        unsafe {
            env::set_var(key, "maybe");
        }

        let result = env_with_default(key, false);
        assert_eq!(result, false);

        unsafe {
            env::remove_var(key);
        }
    }

    #[test]
    #[serial]
    fn test_env_with_default_u32_exists() {
        let key = "TEST_U32_VAR";
        let expected = 42u32;
        unsafe {
            env::set_var(key, expected.to_string());
        }

        let result = env_with_default(key, 0u32);
        assert_eq!(result, expected);

        unsafe {
            env::remove_var(key);
        }
    }

    #[test]
    #[serial]
    fn test_env_with_default_u32_negative_invalid() {
        let key = "TEST_U32_NEGATIVE_VAR";
        unsafe {
            env::set_var(key, "-42");
        }

        let result = env_with_default(key, 0u32);
        assert_eq!(result, 0u32);

        unsafe {
            env::remove_var(key);
        }
    }

    #[test]
    #[serial]
    fn test_env_with_default_empty_string() {
        let key = "TEST_EMPTY_STRING_VAR";
        unsafe {
            env::set_var(key, "");
        }

        let result = env_with_default(key, "default".to_string());
        assert_eq!(result, "");

        unsafe {
            env::remove_var(key);
        }
    }

    #[test]
    #[serial]
    fn test_env_with_default_whitespace_string() {
        let key = "TEST_WHITESPACE_VAR";
        unsafe {
            env::set_var(key, "  whitespace  ");
        }

        let result = env_with_default(key, "default".to_string());
        assert_eq!(result, "  whitespace  ");

        unsafe {
            env::remove_var(key);
        }
    }

    #[test]
    #[serial]
    fn test_env_with_environment() {
        let key = "TEST_ENVIRONMENT";
        unsafe {
            env::set_var(key, "DEVELOPMENT");
        }

        let result = env_with_default(key, environment::Environment::Development);
        assert_eq!(result, environment::Environment::Development);

        unsafe {
            env::remove_var(key);
        }
    }

    #[test]
    #[serial]
    fn test_env_with_mode() {
        let key = "TEST_MODE";
        unsafe {
            env::set_var(key, "STATIC");
        }

        let result = env_with_default(key, mode::Mode::STATIC);
        assert_eq!(result, mode::Mode::STATIC);

        unsafe {
            env::remove_var(key);
        }
    }
}