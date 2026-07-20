use axum::Json;
use axum::extract::State;

use crate::ActuatorState;

/// `GET /actuator/info`
///
/// Returns service build and runtime information.
pub async fn info_handler(State(state): State<ActuatorState>) -> Json<serde_json::Value> {
    Json(serde_json::to_value(&state.info).unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ServiceInfo;
    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::get;
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_info_endpoint() {
        let state = ActuatorState {
            indicators: vec![],
            collectors: vec![],
            info: ServiceInfo {
                name: "test-service".to_string(),
                version: "1.2.3".to_string(),
                git_commit: Some("deadbeef".to_string()),
                build_time: Some("2026-07-20T12:00:00Z".to_string()),
                rust_version: Some("1.87.0".to_string()),
                profile: "release".to_string(),
            },
        };

        let app = Router::new()
            .route("/actuator/info", get(info_handler))
            .with_state(state);

        let response = app
            .oneshot(Request::get("/actuator/info").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["name"], "test-service");
        assert_eq!(json["version"], "1.2.3");
        assert_eq!(json["git_commit"], "deadbeef");
        assert_eq!(json["profile"], "release");
    }

    #[tokio::test]
    async fn test_info_endpoint_minimal() {
        let state = ActuatorState {
            indicators: vec![],
            collectors: vec![],
            info: ServiceInfo {
                name: "minimal".to_string(),
                version: "0.1.0".to_string(),
                ..ServiceInfo::default()
            },
        };

        let app = Router::new()
            .route("/actuator/info", get(info_handler))
            .with_state(state);

        let response = app
            .oneshot(Request::get("/actuator/info").body(Body::empty()).unwrap())
            .await
            .unwrap();

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["name"], "minimal");
        // Optional fields should not appear
        assert!(json.get("git_commit").is_none());
        assert!(json.get("build_time").is_none());
    }
}
