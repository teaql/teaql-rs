use serde::{Deserialize, Serialize};

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Timestamp(pub i64);

impl Timestamp {
    pub fn now() -> Self {
        Self(chrono::Utc::now().timestamp_millis())
    }

    pub fn as_millis(&self) -> i64 {
        self.0
    }

    pub fn to_datetime(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::from_timestamp_millis(self.0)
            .unwrap_or_default()
            .with_timezone(&chrono::Utc)
    }
}

impl From<i64> for Timestamp {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl From<u64> for Timestamp {
    fn from(value: u64) -> Self {
        Self(value as i64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_conversions() {
        let ts = Timestamp::from(1000i64);
        assert_eq!(ts.as_millis(), 1000);
        let dt = ts.to_datetime();
        assert_eq!(dt.timestamp_millis(), 1000);

        let ts2 = Timestamp::from(2000u64);
        assert_eq!(ts2.as_millis(), 2000);
    }

    #[test]
    fn test_timestamp_now() {
        let before = chrono::Utc::now().timestamp_millis();
        let ts = Timestamp::now();
        let after = chrono::Utc::now().timestamp_millis();
        assert!(ts.as_millis() >= before);
        assert!(ts.as_millis() <= after);
    }
}
