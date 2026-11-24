use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use nacos_sdk::api::plugin::{AuthContext, AuthPlugin, LoginIdentityContext, RequestResource};
use serde::Deserialize;

const ACCESS_TOKEN: &str = "accessToken";

/// Nacos username/password authentication compatible with both 3.x and 2.x.
///
/// Nacos 3.x removed the legacy `/v1/auth/login` endpoint used by
/// `nacos-sdk` 0.8's built-in HTTP plugin. Try the current endpoint first and
/// retain the legacy endpoint as a fallback for older 2.x installations.
pub(crate) struct NacosHttpAuthPlugin {
    username: String,
    password: String,
    client: reqwest::Client,
    identity: RwLock<LoginIdentityContext>,
    refresh_after: RwLock<Instant>,
    authenticated: RwLock<bool>,
    last_error: RwLock<Option<String>>,
}

impl NacosHttpAuthPlugin {
    pub(crate) fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
            client: reqwest::Client::new(),
            identity: RwLock::new(LoginIdentityContext::default()),
            refresh_after: RwLock::new(Instant::now()),
            authenticated: RwLock::new(false),
            last_error: RwLock::new(None),
        }
    }

    pub(crate) fn is_authenticated(&self) -> bool {
        self.authenticated.read().is_ok_and(|value| *value)
    }

    pub(crate) fn last_error(&self) -> Option<String> {
        self.last_error.read().ok().and_then(|value| value.clone())
    }

    async fn login_at(&self, server: &str, path: &str) -> Result<LoginResponse, String> {
        let base = if server.starts_with("http://") || server.starts_with("https://") {
            server.trim_end_matches('/').to_owned()
        } else {
            format!("http://{}", server.trim_end_matches('/'))
        };
        let response = self
            .client
            .post(format!("{base}/nacos/{path}"))
            .form(&[
                ("username", self.username.as_str()),
                ("password", self.password.as_str()),
            ])
            .send()
            .await
            .map_err(|error| format!("{path} request failed: {error}"))?;
        if !response.status().is_success() {
            return Err(format!("{path} returned HTTP {}", response.status()));
        }
        response
            .json::<LoginResponse>()
            .await
            .map_err(|error| format!("{path} response could not be decoded: {error}"))
    }
}

#[async_trait::async_trait]
impl AuthPlugin for NacosHttpAuthPlugin {
    async fn login(&self, server_list: Arc<Vec<String>>, _auth_context: Arc<AuthContext>) {
        let now = Instant::now();
        if self
            .refresh_after
            .read()
            .is_ok_and(|refresh_after| now < *refresh_after)
        {
            return;
        }

        let mut failures = Vec::new();
        for server in server_list.iter() {
            let response = match self.login_at(server, "v3/auth/user/login").await {
                Ok(response) => Ok(response),
                Err(v3_error) => {
                    failures.push(format!("{server}: {v3_error}"));
                    self.login_at(server, "v1/auth/login").await
                }
            };
            match response {
                Ok(response) => {
                    if response.access_token.is_empty() {
                        failures.push(format!("{server}: login returned an empty access token"));
                        continue;
                    }
                    if let Ok(mut identity) = self.identity.write() {
                        *identity = LoginIdentityContext::default()
                            .add_context(ACCESS_TOKEN, response.access_token);
                    }
                    if let Ok(mut refresh_after) = self.refresh_after.write() {
                        let refresh_seconds = (response.token_ttl / 10).max(1);
                        *refresh_after = Instant::now() + Duration::from_secs(refresh_seconds);
                    }
                    if let Ok(mut authenticated) = self.authenticated.write() {
                        *authenticated = true;
                    }
                    if let Ok(mut last_error) = self.last_error.write() {
                        *last_error = None;
                    }
                    return;
                }
                Err(error) => failures.push(format!("{server}: {error}")),
            }
        }

        let failure = if failures.is_empty() {
            "Nacos supplied no authentication server".to_string()
        } else {
            failures.join("; ")
        };
        if let Ok(mut authenticated) = self.authenticated.write() {
            *authenticated = false;
        }
        if let Ok(mut last_error) = self.last_error.write() {
            *last_error = Some(failure.clone());
        }
        tracing::error!(
            server_count = server_list.len(),
            reason = %failure,
            "Nacos authentication failed on both v3 and legacy v1 login endpoints"
        );
    }

    fn get_login_identity(&self, _resource: RequestResource) -> LoginIdentityContext {
        self.identity
            .read()
            .map(|identity| identity.clone())
            .unwrap_or_default()
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LoginResponse {
    access_token: String,
    token_ttl: u64,
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    use super::*;

    fn auth_server(
        responses: Vec<(&'static str, &'static str)>,
    ) -> (String, thread::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind auth server");
        let address = listener.local_addr().expect("read auth server address");
        let handle = thread::spawn(move || {
            let mut request_lines = Vec::new();
            for (status, body) in responses {
                let (mut stream, _) = listener.accept().expect("accept auth request");
                let mut buffer = [0_u8; 4096];
                let bytes = stream.read(&mut buffer).expect("read auth request");
                let request = String::from_utf8_lossy(&buffer[..bytes]);
                request_lines.push(request.lines().next().unwrap_or_default().to_string());
                write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .expect("write auth response");
            }
            request_lines
        });
        (address.to_string(), handle)
    }

    async fn login(plugin: &NacosHttpAuthPlugin, address: String) {
        plugin
            .login(Arc::new(vec![address]), Arc::new(AuthContext::default()))
            .await;
    }

    #[tokio::test]
    async fn authenticates_with_nacos_v3_endpoint() {
        let (address, server) = auth_server(vec![(
            "200 OK",
            r#"{"accessToken":"v3-token","tokenTtl":18000}"#,
        )]);
        let plugin = NacosHttpAuthPlugin::new("nacos", "secret");

        login(&plugin, address).await;

        assert!(plugin.is_authenticated());
        assert!(plugin.last_error().is_none());
        let requests = server.join().expect("join auth server");
        assert!(requests[0].contains("/nacos/v3/auth/user/login"));
    }

    #[tokio::test]
    async fn falls_back_to_legacy_nacos_v1_endpoint() {
        let (address, server) = auth_server(vec![
            ("404 Not Found", r#"{"message":"not found"}"#),
            ("200 OK", r#"{"accessToken":"v1-token","tokenTtl":18000}"#),
        ]);
        let plugin = NacosHttpAuthPlugin::new("nacos", "secret");

        login(&plugin, address).await;

        assert!(plugin.is_authenticated());
        let requests = server.join().expect("join auth server");
        assert!(requests[0].contains("/nacos/v3/auth/user/login"));
        assert!(requests[1].contains("/nacos/v1/auth/login"));
    }

    #[tokio::test]
    async fn reports_authentication_failure_without_credentials() {
        let (address, server) = auth_server(vec![
            ("401 Unauthorized", r#"{"message":"unauthorized"}"#),
            ("403 Forbidden", r#"{"message":"forbidden"}"#),
        ]);
        let plugin = NacosHttpAuthPlugin::new("nacos", "super-secret");

        login(&plugin, address).await;

        assert!(!plugin.is_authenticated());
        let error = plugin.last_error().expect("authentication diagnostic");
        assert!(error.contains("401 Unauthorized"));
        assert!(error.contains("403 Forbidden"));
        assert!(!error.contains("super-secret"));
        server.join().expect("join auth server");
    }
}
