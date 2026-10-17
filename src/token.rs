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
//!
//! A process that must act only as its caller passes the caller's token with
//! [`Adc::from_access_token`], or discovers with [`MetadataPolicy::Forbid`],
//! under which the metadata server is never contacted: no user credential
//! means [`Error::NoCredentials`], not the instance's own identity.

use crate::{Error, Result};
use graph_r::fsio::now_ms;
use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;

const METADATA_HOST: &str = "metadata.google.internal";
const METADATA_TOKEN_PATH: &str = "/computeMetadata/v1/instance/service-accounts/default/token";
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
    /// The default service account of the instance, at
    /// `metadata.google.internal` or the `host[:port]` in `GCE_METADATA_HOST`
    /// (as Google's client libraries honour it).
    pub fn new() -> Result<Self> {
        let host = std::env::var("GCE_METADATA_HOST").ok().filter(|h| !h.is_empty());
        Self::with_url(&format!(
            "http://{}{METADATA_TOKEN_PATH}",
            host.as_deref().unwrap_or(METADATA_HOST)
        ))
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

/// Whether credential discovery may fall through to the metadata server.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum MetadataPolicy {
    /// After user credentials, use the instance's own identity (Cloud Run,
    /// GCE, GKE). The historical behaviour.
    #[default]
    Allow,
    /// Never contact the metadata server: without user credentials,
    /// discovery is [`Error::NoCredentials`].
    Forbid,
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
    /// well-known file, then the metadata server
    /// ([`discover_with`](Self::discover_with) under [`MetadataPolicy::Allow`]).
    pub fn discover() -> Result<Self> {
        Self::discover_with(MetadataPolicy::Allow)
    }

    /// Discovery as [`discover`](Self::discover), with the metadata server
    /// allowed or forbidden.
    pub fn discover_with(metadata: MetadataPolicy) -> Result<Self> {
        Self::discover_from(
            |name: &str| std::env::var_os(name),
            |p: &std::path::Path| std::fs::read(p).ok(),
            cfg!(windows),
            metadata,
        )
    }

    fn discover_from<F, R>(var: F, read: R, windows: bool, metadata: MetadataPolicy) -> Result<Self>
    where
        F: Fn(&str) -> Option<std::ffi::OsString>,
        R: Fn(&std::path::Path) -> Option<Vec<u8>>,
    {
        match user_credentials(var, read, windows)? {
            Some(user) => Ok(Adc::User(user)),
            None => match metadata {
                MetadataPolicy::Allow => Metadata::new().map(Adc::Metadata),
                MetadataPolicy::Forbid => Err(Error::NoCredentials {
                    reason: "no user credentials and the metadata server is forbidden",
                }),
            },
        }
    }

    /// A caller-supplied access token, used as is and never refreshed
    /// ([`Adc::Static`]). An empty or blank token is
    /// [`Error::NoCredentials`]: it would send unauthenticated requests.
    pub fn from_access_token(secret: impl Into<String>) -> Result<Self> {
        let secret = secret.into();
        if secret.trim().is_empty() {
            return Err(Error::NoCredentials { reason: "empty access token" });
        }
        Ok(Adc::Static(Static::new(secret)))
    }
}

/// User credentials: the file named by `GOOGLE_APPLICATION_CREDENTIALS`, else
/// gcloud's well-known file; `None` when neither exists. `var` and `read`
/// are the environment and the file system, passed in for tests.
fn user_credentials<F, R>(var: F, read: R, windows: bool) -> Result<Option<GcloudUser>>
where
    F: Fn(&str) -> Option<std::ffi::OsString>,
    R: Fn(&std::path::Path) -> Option<Vec<u8>>,
{
    if let Some(path) = var("GOOGLE_APPLICATION_CREDENTIALS") {
        let json = read(std::path::Path::new(&path))
            .ok_or_else(|| Error::Auth { message: "unreadable credentials file".into() })?;
        return GcloudUser::from_json(&json).map(Some);
    }
    if let Some(dir) = gcloud_config_dir(&var, windows) {
        if let Some(json) = read(&dir.join("application_default_credentials.json")) {
            return GcloudUser::from_json(&json).map(Some);
        }
    }
    Ok(None)
}

/// gcloud's configuration directory, where `gcloud auth
/// application-default login` writes the well-known credentials file:
/// `CLOUDSDK_CONFIG` when set, else `%APPDATA%\gcloud` on Windows and
/// `$HOME/.config/gcloud` elsewhere.
///
/// Windows never consults `HOME`: OpenSSH, Git Bash and MSYS set it to the
/// profile directory, and `%HOME%\gcloud` is not where gcloud writes, so a
/// HOME-first lookup missed the file and fell through to the metadata server --
/// on a GCE VM that silently authenticated as the VM's own service account.
/// `var` is the environment, passed in so the rule is tested without touching
/// the process's own.
fn gcloud_config_dir<F>(var: F, windows: bool) -> Option<std::path::PathBuf>
where
    F: Fn(&str) -> Option<std::ffi::OsString>,
{
    let set = |name: &str| var(name).filter(|v| !v.is_empty());
    if let Some(dir) = set("CLOUDSDK_CONFIG") {
        return Some(std::path::PathBuf::from(dir));
    }
    if windows {
        return set("APPDATA").map(|a| std::path::PathBuf::from(a).join("gcloud"));
    }
    set("HOME").map(|h| std::path::PathBuf::from(h).join(".config").join("gcloud"))
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

    fn env<'a>(
        pairs: &'a [(&'a str, &'a str)],
    ) -> impl Fn(&str) -> Option<std::ffi::OsString> + 'a {
        move |name| pairs.iter().find(|(k, _)| *k == name).map(|(_, v)| std::ffi::OsString::from(v))
    }

    #[test]
    fn windows_reads_appdata_even_when_home_is_set() {
        let e = [("HOME", r"C:\Users\a"), ("APPDATA", r"C:\Users\a\AppData\Roaming")];
        assert_eq!(
            gcloud_config_dir(env(&e), true),
            Some(std::path::PathBuf::from(r"C:\Users\a\AppData\Roaming").join("gcloud"))
        );
    }

    #[test]
    fn posix_reads_home_dot_config() {
        let e = [("HOME", "/home/a"), ("APPDATA", "/ignored")];
        assert_eq!(
            gcloud_config_dir(env(&e), false),
            Some(std::path::PathBuf::from("/home/a").join(".config").join("gcloud"))
        );
    }

    #[test]
    fn cloudsdk_config_wins_on_every_os() {
        let e = [("CLOUDSDK_CONFIG", "/custom"), ("HOME", "/home/a"), ("APPDATA", "/appdata")];
        for windows in [true, false] {
            assert_eq!(
                gcloud_config_dir(env(&e), windows),
                Some(std::path::PathBuf::from("/custom"))
            );
        }
    }

    #[test]
    fn empty_values_count_as_unset() {
        let e = [("CLOUDSDK_CONFIG", ""), ("APPDATA", "")];
        assert_eq!(gcloud_config_dir(env(&e), true), None);
        assert_eq!(gcloud_config_dir(env(&[("HOME", "")]), false), None);
    }

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
    fn from_access_token_is_static_and_refuses_blank() {
        let a = Adc::from_access_token("ya29.caller").unwrap();
        assert!(matches!(&a, Adc::Static(_)));
        let t = futures::executor::block_on(a.token()).unwrap();
        assert_eq!((t.secret(), t.expires_at_ms), ("ya29.caller", u64::MAX));
        assert_eq!(a.quota_project(), None);
        for blank in ["", "  ", "\n"] {
            assert!(matches!(
                Adc::from_access_token(blank),
                Err(Error::NoCredentials { reason: "empty access token" })
            ));
        }
    }

    #[test]
    fn static_tokens_are_fixed_and_none_sends_nothing() {
        let t = futures::executor::block_on(Static::new("abc").token()).unwrap();
        assert_eq!(t.secret(), "abc");
        assert!(futures::executor::block_on(Static::none().token()).unwrap().secret().is_empty());
    }

    #[test]
    fn user_credentials_come_from_the_env_file_then_the_well_known_file() {
        let user = br#"{"type":"authorized_user","client_id":"i","client_secret":"s","refresh_token":"r","quota_project_id":"q"}"#;
        let files = |want: &'static str| {
            move |p: &std::path::Path| (p == std::path::Path::new(want)).then(|| user.to_vec())
        };
        let e = [("GOOGLE_APPLICATION_CREDENTIALS", "/k.json")];
        let u = user_credentials(env(&e), files("/k.json"), false).unwrap().unwrap();
        assert_eq!(u.quota_project(), Some("q"));
        assert!(user_credentials(env(&e), |_: &std::path::Path| None, false).is_err());
        let wk = "/h/.config/gcloud/application_default_credentials.json";
        assert!(user_credentials(env(&[("HOME", "/h")]), files(wk), false).unwrap().is_some());
        assert!(user_credentials(env(&[("HOME", "/h")]), files("/x"), false).unwrap().is_none());
    }

    #[test]
    fn forbid_never_builds_the_metadata_provider() {
        let none = |_: &std::path::Path| None;
        let e = [("HOME", "/nowhere")];
        assert!(matches!(
            Adc::discover_from(env(&e), none, false, MetadataPolicy::Forbid),
            Err(Error::NoCredentials { .. })
        ));
        // The same environment under Allow falls through, as it always has.
        assert!(matches!(
            Adc::discover_from(env(&e), none, false, MetadataPolicy::Allow),
            Ok(Adc::Metadata(_))
        ));
        assert_eq!(MetadataPolicy::default(), MetadataPolicy::Allow, "compatible default");
        // User credentials win under either policy.
        let user = br#"{"type":"authorized_user","client_id":"i","client_secret":"s","refresh_token":"r"}"#;
        let e = [("GOOGLE_APPLICATION_CREDENTIALS", "/k.json")];
        let read = |_: &std::path::Path| Some(user.to_vec());
        assert!(matches!(
            Adc::discover_from(env(&e), read, false, MetadataPolicy::Forbid),
            Ok(Adc::User(_))
        ));
    }

    #[test]
    fn freshness_has_skew() {
        let now = 1_000_000;
        assert!(!AccessToken::new("x", now + 30_000).fresh(now));
        assert!(AccessToken::new("x", now + 120_000).fresh(now));
    }
}
