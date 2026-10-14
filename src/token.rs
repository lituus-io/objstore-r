// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! OAuth access tokens for Google APIs (feature `gcs`).
//!
//! [`Adc::discover`] follows Application Default Credentials the way `gcloud`
//! does for the cases that need no private-key signing: the metadata server on
//! Cloud Run / GCE / GKE, and user credentials from
//! `gcloud auth application-default login`. Service-account key files are not
//! supported on purpose (use workload identity or impersonation instead of
//! long-lived keys). Applications with their own credential stack implement
//! [`TokenProvider`] directly.
//!
//! Tokens are cached until a minute before they expire and are never printed:
//! [`AccessToken`]'s `Debug` is redacted.

use crate::{Error, Result};
use graph_r::fsio::now_ms;
use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;

const METADATA_TOKEN: &str =
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";
const TOKEN_URI: &str = "https://oauth2.googleapis.com/token";
/// Refresh this long before expiry.
const SKEW_MS: u64 = 60_000;

/// A bearer token.
#[derive(Clone)]
pub struct AccessToken {
    secret: String,
    /// Expiry, Unix milliseconds (`u64::MAX`: never).
    pub expires_at_ms: u64,
}

impl AccessToken {
    /// A token that never expires (tests, emulators, externally managed tokens).
    #[must_use]
    pub fn new(secret: impl Into<String>, expires_at_ms: u64) -> Self {
        Self { secret: secret.into(), expires_at_ms }
    }

    /// The token text, for an `Authorization: Bearer` header.
    #[must_use]
    pub fn secret(&self) -> &str {
        &self.secret
    }

    fn fresh(&self, now: u64) -> bool {
        self.expires_at_ms > now.saturating_add(SKEW_MS)
    }
}

impl std::fmt::Debug for AccessToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccessToken")
            .field("secret", &"<redacted>")
            .field("expires_at_ms", &self.expires_at_ms)
            .finish()
    }
}

/// A source of access tokens.
pub trait TokenProvider {
    /// Future of [`token`](TokenProvider::token).
    type Token<'a>: Future<Output = Result<AccessToken>> + 'a
    where
        Self: 'a;

    /// A token valid for at least a minute (an empty secret sends no header).
    fn token(&self) -> Self::Token<'_>;

    /// The project billed for API quota (user credentials only).
    fn quota_project(&self) -> Option<&str> {
        None
    }
}

/// A fixed token.
#[derive(Debug, Clone)]
pub struct Static(AccessToken);

impl Static {
    /// A fixed bearer token.
    #[must_use]
    pub fn new(secret: impl Into<String>) -> Self {
        Self(AccessToken::new(secret, u64::MAX))
    }

    /// No token at all (emulators such as fake-gcs-server).
    #[must_use]
    pub fn none() -> Self {
        Self::new(String::new())
    }
}

impl TokenProvider for Static {
    type Token<'a> = std::future::Ready<Result<AccessToken>>;
    fn token(&self) -> Self::Token<'_> {
        std::future::ready(Ok(self.0.clone()))
    }
}

#[derive(Debug, Default)]
struct Cache(Mutex<Option<AccessToken>>);

impl Cache {
    fn get(&self, now: u64) -> Option<AccessToken> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .filter(|t| t.fresh(now))
            .cloned()
    }
    fn put(&self, t: &AccessToken) {
        *self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(t.clone());
    }
}

#[derive(serde::Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: Option<u64>,
}

fn http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| Error::unavailable(format!("http client: {e}"), false))
}

async fn parse_token(resp: reqwest::Response) -> Result<AccessToken> {
    let status = resp.status();
    if !status.is_success() {
        return Err(Error::Auth { message: format!("token endpoint answered {status}") });
    }
    let body =
        resp.bytes().await.map_err(|_| Error::unavailable("token response truncated", true))?;
    let t: TokenResponse = serde_json::from_slice(&body)
        .map_err(|_| Error::Auth { message: "malformed token response".into() })?;
    Ok(AccessToken::new(
        t.access_token,
        now_ms().saturating_add(t.expires_in.unwrap_or(300).saturating_mul(1000)),
    ))
}

/// The metadata server (Cloud Run, GCE, GKE workload identity).
#[derive(Debug)]
pub struct Metadata {
    http: reqwest::Client,
    url: String,
    cache: Cache,
}

impl Metadata {
    /// The default service account of the instance.
    pub fn new() -> Result<Self> {
        Self::with_url(METADATA_TOKEN)
    }

    /// Another token URL (tests).
    pub fn with_url(url: &str) -> Result<Self> {
        Ok(Self { http: http_client()?, url: url.to_owned(), cache: Cache::default() })
    }
}

impl TokenProvider for Metadata {
    type Token<'a> = impl Future<Output = Result<AccessToken>> + 'a;
    fn token(&self) -> Self::Token<'_> {
        async move {
            if let Some(t) = self.cache.get(now_ms()) {
                return Ok(t);
            }
            let resp = self
                .http
                .get(&self.url)
                .header("Metadata-Flavor", "Google")
                .send()
                .await
                .map_err(|_| Error::unavailable("metadata server unreachable", true))?;
            let t = parse_token(resp).await?;
            self.cache.put(&t);
            Ok(t)
        }
    }
}

/// User credentials from `gcloud auth application-default login`.
pub struct GcloudUser {
    http: reqwest::Client,
    client_id: String,
    client_secret: String,
    refresh_token: String,
    quota_project: Option<String>,
    token_uri: String,
    cache: Cache,
}

impl std::fmt::Debug for GcloudUser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcloudUser")
            .field("quota_project", &self.quota_project)
            .finish_non_exhaustive()
    }
}

#[derive(serde::Deserialize)]
struct UserFile {
    #[serde(rename = "type")]
    kind: String,
    client_id: Option<String>,
    client_secret: Option<String>,
    refresh_token: Option<String>,
    quota_project_id: Option<String>,
}

impl GcloudUser {
    /// Parse an `authorized_user` credentials file.
    pub fn from_json(json: &[u8]) -> Result<Self> {
        let f: UserFile = serde_json::from_slice(json)
            .map_err(|_| Error::Auth { message: "unreadable credentials file".into() })?;
        if f.kind != "authorized_user" {
            return Err(Error::Auth {
                message: format!("credentials of type {:?} are not supported", f.kind),
            });
        }
        let (Some(client_id), Some(client_secret), Some(refresh_token)) =
            (f.client_id, f.client_secret, f.refresh_token)
        else {
            return Err(Error::Auth { message: "incomplete user credentials".into() });
        };
        Ok(Self {
            http: http_client()?,
            client_id,
            client_secret,
            refresh_token,
            quota_project: f.quota_project_id,
            token_uri: TOKEN_URI.to_owned(),
            cache: Cache::default(),
        })
    }

    /// Another token endpoint (tests).
    #[must_use]
    pub fn with_token_uri(mut self, uri: &str) -> Self {
        uri.clone_into(&mut self.token_uri);
        self
    }
}

impl TokenProvider for GcloudUser {
    type Token<'a> = impl Future<Output = Result<AccessToken>> + 'a;
    fn token(&self) -> Self::Token<'_> {
        async move {
            if let Some(t) = self.cache.get(now_ms()) {
                return Ok(t);
            }
            let form = [
                ("grant_type", "refresh_token"),
                ("client_id", self.client_id.as_str()),
                ("client_secret", self.client_secret.as_str()),
                ("refresh_token", self.refresh_token.as_str()),
            ];
            let body = form
                .iter()
                .map(|(k, v)| format!("{k}={}", form_encode(v)))
                .collect::<Vec<_>>()
                .join("&");
            let resp = self
                .http
                .post(&self.token_uri)
                .header("content-type", "application/x-www-form-urlencoded")
                .body(body)
                .send()
                .await
                .map_err(|_| Error::unavailable("token endpoint unreachable", true))?;
            let t = parse_token(resp).await?;
            self.cache.put(&t);
            Ok(t)
        }
    }

    fn quota_project(&self) -> Option<&str> {
        self.quota_project.as_deref()
    }
}

fn form_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    crate::key::encode_component(s, &mut out);
    out
}

/// Application Default Credentials (no private-key signing).
#[derive(Debug)]
pub enum Adc {
    /// The instance metadata server.
    Metadata(Metadata),
    /// gcloud user credentials.
    User(GcloudUser),
    /// A fixed token.
    Static(Static),
}

impl Adc {
    /// `GOOGLE_APPLICATION_CREDENTIALS` (user credentials), then gcloud's
    /// well-known file, then the metadata server.
    pub fn discover() -> Result<Self> {
        if let Some(path) = std::env::var_os("GOOGLE_APPLICATION_CREDENTIALS") {
            let json = std::fs::read(path)
                .map_err(|_| Error::Auth { message: "unreadable credentials file".into() })?;
            return GcloudUser::from_json(&json).map(Adc::User);
        }
        let home = std::env::var_os("HOME").or_else(|| std::env::var_os("APPDATA"));
        if let Some(home) = home {
            let mut p = std::path::PathBuf::from(home);
            if cfg!(windows) {
                p.push("gcloud");
            } else {
                p.push(".config/gcloud");
            }
            p.push("application_default_credentials.json");
            if let Ok(json) = std::fs::read(&p) {
                return GcloudUser::from_json(&json).map(Adc::User);
            }
        }
        Metadata::new().map(Adc::Metadata)
    }
}

impl TokenProvider for Adc {
    type Token<'a> = impl Future<Output = Result<AccessToken>> + 'a;
    fn token(&self) -> Self::Token<'_> {
        async move {
            match self {
                Adc::Metadata(m) => m.token().await,
                Adc::User(u) => u.token().await,
                Adc::Static(s) => s.token().await,
            }
        }
    }

    fn quota_project(&self) -> Option<&str> {
        match self {
            Adc::User(u) => u.quota_project(),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_are_never_printed() {
        let t = AccessToken::new("ya29.secret", 1);
        assert!(!format!("{t:?}").contains("ya29"));
        let u = GcloudUser::from_json(
            br#"{"type":"authorized_user","client_id":"id","client_secret":"cs","refresh_token":"rt","quota_project_id":"p"}"#,
        )
        .unwrap();
        let dbg = format!("{u:?}");
        assert!(!dbg.contains("cs") && !dbg.contains("rt"), "{dbg}");
        assert_eq!(u.quota_project(), Some("p"));
        assert!(GcloudUser::from_json(br#"{"type":"service_account"}"#).is_err());
    }

    #[test]
    fn freshness_has_skew() {
        let now = 1_000_000;
        assert!(!AccessToken::new("x", now + 30_000).fresh(now));
        assert!(AccessToken::new("x", now + 120_000).fresh(now));
    }
}
