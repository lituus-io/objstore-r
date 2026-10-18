// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Request policies for [`crate::http::Client`] (feature `http`).
//!
//! A [`RequestPolicy`] sees every request before any byte is sent:
//!
//! - [`check`](RequestPolicy::check) gets the method and the parsed URL,
//!   exactly as it goes on the wire;
//! - [`check_query`](RequestPolicy::check_query) gets the SQL text of every
//!   `BigQuery` query job (`jobs.query`, `jobs.insert`). The client isolates
//!   that text itself, so no query reaches `BigQuery` unchecked: a body whose
//!   SQL cannot be isolated is refused under every policy
//!   ([`PolicyError::UncheckableQuery`]).
//!
//! [`Permissive`] (the client's default) allows everything. [`AllowList`]
//! allows only listed method + host + path-template triples, and no query
//! unless wrapped by a policy that implements `check_query`.
//!
//! Policies are static types (no trait objects); `&P` is a policy whenever
//! `P` is, so one policy value can serve many clients.

use crate::transport::Method;
use crate::PolicyError;
use std::borrow::Cow;

pub use reqwest::Url;

/// Decides which requests a [`crate::http::Client`] may send.
pub trait RequestPolicy {
    /// Allow or refuse `method url`. Called once per request, before the
    /// token is fetched or a connection opened.
    fn check(&self, method: Method, url: &Url) -> Result<(), PolicyError>;

    /// Allow or refuse the SQL text of a `BigQuery` query job. The default
    /// refuses every query: this crate has no SQL parser, so a policy that
    /// permits queries must say which.
    fn check_query(&self, sql: &str) -> Result<(), PolicyError> {
        let _ = sql;
        Err(PolicyError::Query { reason: Cow::Borrowed("this policy allows no queries") })
    }
}

impl<P: RequestPolicy> RequestPolicy for &P {
    fn check(&self, method: Method, url: &Url) -> Result<(), PolicyError> {
        (**self).check(method, url)
    }
    fn check_query(&self, sql: &str) -> Result<(), PolicyError> {
        (**self).check_query(sql)
    }
}

/// Everything allowed: the client's behaviour before request policies
/// existed (the client's own https-only and redirect rules still apply).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Permissive;

impl RequestPolicy for Permissive {
    fn check(&self, _: Method, _: &Url) -> Result<(), PolicyError> {
        Ok(())
    }
    fn check_query(&self, _: &str) -> Result<(), PolicyError> {
        Ok(())
    }
}

/// One allowed endpoint: a method, an exact host, and a path template.
///
/// The template is an absolute path whose segments are literals, `{name}`
/// placeholders, or a placeholder with a custom-method suffix
/// (`{location}:searchLinks`), e.g.
/// `/bigquery/v2/projects/{project}/datasets/{dataset}`. A literal (and a
/// suffix) matches only its identical raw spelling, so a percent-encoded
/// spelling never matches; a placeholder matches any one non-empty segment
/// that passes the path rules of [`AllowList`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rule<'a> {
    /// The method.
    pub method: Method,
    /// The host, lowercase, without a port.
    pub host: &'a str,
    /// The path template.
    pub path: &'a str,
}

impl<'a> Rule<'a> {
    /// A rule (validated by [`AllowList::new`]).
    #[must_use]
    pub const fn new(method: Method, host: &'a str, path: &'a str) -> Self {
        Self { method, host, path }
    }
}

/// Allows only the listed [`Rule`]s, over `https` on the default port.
///
/// Before matching, every URL must pass these rules (each refusal is a
/// [`PolicyError::Endpoint`]):
///
/// - scheme `https` (plain `http` to a loopback host only when built with
///   `with_loopback_http`, feature `testing`), default port, no
///   user-info, no fragment;
/// - every path segment non-empty (no `//`, no trailing `/`), not `.` or
///   `..` in any spelling, and free of encoded `/`, `\`, NUL or control
///   bytes, and of characters outside RFC 3986 `pchar`.
///
/// The query string is not constrained. Redirects are never followed by the
/// client, so a listed endpoint cannot hand the request on.
#[derive(Debug, Clone, Copy)]
pub struct AllowList<'a> {
    rules: &'a [Rule<'a>],
    loopback_http: bool,
}

impl<'a> AllowList<'a> {
    /// An allow list of `rules`; refuses a malformed host or template.
    pub fn new(rules: &'a [Rule<'a>]) -> crate::Result<Self> {
        for r in rules {
            if !valid_host(r.host) {
                return Err(crate::Error::protocol("allow list: invalid host"));
            }
            if !valid_template(r.path) {
                return Err(crate::Error::protocol("allow list: invalid path template"));
            }
        }
        Ok(Self { rules, loopback_http: false })
    }

    /// Also accept plain `http` to a loopback host (`127.0.0.1`, `[::1]`,
    /// `localhost`, any port), which then stands in for every listed host:
    /// emulators and stub servers in tests.
    #[cfg(feature = "testing")]
    #[must_use]
    pub fn with_loopback_http(mut self) -> Self {
        self.loopback_http = true;
        self
    }

    /// The rules.
    #[must_use]
    pub fn rules(&self) -> &'a [Rule<'a>] {
        self.rules
    }
}

impl RequestPolicy for AllowList<'_> {
    fn check(&self, method: Method, url: &Url) -> Result<(), PolicyError> {
        let refuse = |reason| Err(PolicyError::Endpoint { reason });
        let host = url.host_str().unwrap_or("");
        let loopback = match url.scheme() {
            "https" => {
                if url.port().is_some() {
                    return refuse("non-default port");
                }
                false
            }
            "http" if self.loopback_http && is_loopback_host(host) => true,
            _ => return refuse("scheme must be https"),
        };
        if !url.username().is_empty() || url.password().is_some() {
            return refuse("credentials in the URL");
        }
        if url.fragment().is_some() {
            return refuse("fragment");
        }
        let path = url.path();
        if !valid_path(path) {
            return refuse("path is not normalised");
        }
        let hit = self.rules.iter().any(|r| {
            r.method == method
                && (loopback || r.host.eq_ignore_ascii_case(host))
                && template_matches(r.path, path)
        });
        if hit {
            Ok(())
        } else {
            refuse("not on the allow list")
        }
    }
}

fn is_loopback_host(host: &str) -> bool {
    matches!(host, "127.0.0.1" | "[::1]" | "localhost")
}

fn valid_host(h: &str) -> bool {
    !h.is_empty()
        && h.len() <= 253
        && h.split('.').all(|l| {
            !l.is_empty()
                && !l.starts_with('-')
                && !l.ends_with('-')
                && l.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        })
}

/// For a placeholder segment, `{name}` or `{name}:verb` (a Google custom
/// method), its literal suffix (empty for a bare placeholder); `None` for a
/// literal segment.
fn placeholder_suffix(seg: &str) -> Option<&str> {
    let rest = seg.strip_prefix('{')?;
    let (name, suffix) = rest.split_at(rest.find('}')?);
    let suffix = &suffix[1..];
    let name_ok = !name.is_empty() && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_');
    let suffix_ok = suffix.is_empty()
        || (suffix.len() > 1 && suffix.starts_with(':') && suffix.bytes().all(is_pchar));
    (name_ok && suffix_ok).then_some(suffix)
}

fn valid_template(t: &str) -> bool {
    let Some(rest) = t.strip_prefix('/') else { return false };
    rest.split('/').all(|seg| {
        placeholder_suffix(seg).is_some()
            || (!seg.is_empty()
                && seg != "."
                && seg != ".."
                && seg.bytes().all(|b| is_pchar(b) && b != b'%'))
    })
}

/// RFC 3986 `pchar` minus `%` (checked separately): unreserved, sub-delims,
/// `:` and `@`.
fn is_pchar(b: u8) -> bool {
    PCHAR[usize::from(b)]
}

const PCHAR: [bool; 256] = {
    let mut t = [false; 256];
    let mut i = 0;
    while i < 256 {
        let b = i as u8;
        t[i] = b.is_ascii_alphanumeric();
        i += 1;
    }
    let extra = b"-._~!$&'()*+,;=:@";
    let mut k = 0;
    while k < extra.len() {
        t[extra[k] as usize] = true;
        k += 1;
    }
    t
};

fn hex(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// One path segment, percent-decoded on the fly (no allocation); `None` at
/// a malformed escape.
fn decoded(b: &[u8]) -> impl Iterator<Item = Option<u8>> + '_ {
    let mut i = 0;
    std::iter::from_fn(move || {
        let c = *b.get(i)?;
        if c != b'%' {
            i += 1;
            return Some(Some(c));
        }
        let v = b.get(i + 1).and_then(|&h| hex(h)).zip(b.get(i + 2).and_then(|&l| hex(l)));
        i += 3;
        Some(v.map(|(h, l)| h << 4 | l))
    })
}

fn valid_segment(seg: &[u8]) -> bool {
    let (mut escaped, mut dots) = (false, 0usize);
    for &b in seg {
        escaped |= b == b'%';
        dots += usize::from(b == b'.');
        if b != b'%' && !is_pchar(b) {
            return false;
        }
    }
    if !escaped {
        return !(seg.len() <= 2 && dots == seg.len());
    }
    let (mut len, mut dots) = (0usize, 0usize);
    for c in decoded(seg) {
        let Some(c) = c else { return false };
        if c == b'/' || c == b'\\' || c < 0x20 || c == 0x7f {
            return false;
        }
        len += 1;
        dots += usize::from(c == b'.');
    }
    !(len <= 2 && dots == len)
}

/// Segments of an absolute path, without the leading `/`.
fn segments(path: &[u8]) -> impl Iterator<Item = &[u8]> + '_ {
    path.get(1..).unwrap_or_default().split(|&b| b == b'/')
}

fn valid_path(path: &str) -> bool {
    path.starts_with('/') && segments(path.as_bytes()).all(valid_segment)
}

fn template_matches(template: &str, path: &str) -> bool {
    let mut t = segments(template.as_bytes());
    let mut p = segments(path.as_bytes());
    loop {
        let (ts, ps) = match (t.next(), p.next()) {
            (None, None) => return true,
            (Some(ts), Some(ps)) => (ts, ps),
            _ => return false,
        };
        let hit = if ts.first() == Some(&b'{') {
            // Validated at construction: `{name}` then an optional suffix.
            let close = ts.iter().position(|&b| b == b'}').unwrap_or(ts.len() - 1);
            let sfx = &ts[close + 1..];
            ps.strip_suffix(sfx)
                .is_some_and(|v| !v.is_empty() && (sfx.is_empty() || valid_segment(v)))
        } else {
            ts == ps
        };
        if !hit {
            return false;
        }
    }
}

// ---------------------------------------------------------------------------
// The BigQuery query hook (applied by the client under every policy).

/// The segment, percent-decoded, up to any `;` or `:`, compared to the
/// lowercase `word` ignoring ASCII case.
fn segment_is(seg: &str, word: &str) -> bool {
    let mut w = word.bytes();
    for c in decoded(seg.as_bytes()) {
        match c {
            Some(b';' | b':') => break,
            Some(c) if w.next() == Some(c.to_ascii_lowercase()) => {}
            _ => return false,
        }
    }
    w.next().is_none()
}

fn is_bigquery(url: &Url) -> bool {
    let host = url.host_str().unwrap_or("");
    host.get(..8).is_some_and(|h| h.eq_ignore_ascii_case("bigquery"))
        || url.path().split('/').any(|s| segment_is(s, "bigquery"))
}

#[derive(serde::Deserialize)]
struct QueryRequest<'a> {
    #[serde(borrow)]
    query: Cow<'a, str>,
    #[serde(rename = "useLegacySql", alias = "use_legacy_sql")]
    use_legacy_sql: Option<bool>,
}

#[derive(serde::Deserialize)]
struct Job<'a> {
    #[serde(borrow)]
    configuration: JobConfiguration<'a>,
}

#[derive(serde::Deserialize)]
struct JobConfiguration<'a> {
    #[serde(borrow)]
    query: Option<QueryConfig<'a>>,
    load: Option<serde::de::IgnoredAny>,
    copy: Option<serde::de::IgnoredAny>,
    extract: Option<serde::de::IgnoredAny>,
}

#[derive(serde::Deserialize)]
struct QueryConfig<'a> {
    #[serde(borrow)]
    query: Cow<'a, str>,
    #[serde(rename = "useLegacySql", alias = "use_legacy_sql")]
    use_legacy_sql: Option<bool>,
    #[serde(rename = "tableDefinitions", alias = "table_definitions")]
    table_definitions: Option<serde::de::IgnoredAny>,
    #[serde(rename = "userDefinedFunctionResources", alias = "user_defined_function_resources")]
    udf_resources: Option<serde::de::IgnoredAny>,
}

fn uncheckable(reason: &'static str) -> PolicyError {
    PolicyError::UncheckableQuery { reason }
}

fn standard_sql(sql: Cow<'_, str>, legacy: Option<bool>) -> Result<Cow<'_, str>, PolicyError> {
    if legacy != Some(false) {
        return Err(uncheckable("useLegacySql must be false"));
    }
    let first = sql.trim_start().lines().next().unwrap_or("");
    if first.starts_with('#') && first.to_ascii_lowercase().contains("legacysql") {
        return Err(uncheckable("a #legacySQL prefix"));
    }
    Ok(sql)
}

/// The SQL text a request carries to `BigQuery`, if it is a query job:
/// `Ok(None)` for every other request, `Err` when it targets a query
/// endpoint but its SQL cannot be isolated. Duplicate or aliased keys are
/// refused, never resolved, so the text checked is the text run.
pub(crate) fn query_sql<'b>(
    method: Method,
    url: &Url,
    body: &'b [u8],
) -> Result<Option<Cow<'b, str>>, PolicyError> {
    if (method != Method::Post && body.is_empty()) || !is_bigquery(url) {
        return Ok(None);
    }
    let path = url.path();
    if path.split('/').any(|s| segment_is(s, "batch")) {
        return Err(uncheckable("batch requests"));
    }
    let Some(last) = path.split('/').rev().find(|s| !s.is_empty()) else { return Ok(None) };
    if segment_is(last, "queries") {
        let q: QueryRequest<'_> =
            serde_json::from_slice(body).map_err(|_| uncheckable("not a jobs.query body"))?;
        standard_sql(q.query, q.use_legacy_sql).map(Some)
    } else if segment_is(last, "jobs") {
        let j: Job<'_> =
            serde_json::from_slice(body).map_err(|_| uncheckable("not a jobs.insert body"))?;
        let c = j.configuration;
        let (Some(q), None, None, None) = (c.query, c.load, c.copy, c.extract) else {
            return Err(uncheckable("not exactly one query job"));
        };
        if q.table_definitions.is_some() || q.udf_resources.is_some() {
            return Err(uncheckable("external tables or UDF resources"));
        }
        standard_sql(q.query, q.use_legacy_sql).map(Some)
    } else {
        Ok(None)
    }
}

/// Fuzzing entry point: arbitrary bytes as a URL plus a redirect
/// `Location`. Every URL the allow list accepts, directly or as a redirect
/// target, must independently satisfy its invariants; nothing panics.
#[doc(hidden)]
pub fn fuzz(input: &[u8]) {
    const RULES: &[Rule<'static>] = &[
        Rule::new(Method::Get, "bigquery.googleapis.com", "/bigquery/v2/projects/{p}/datasets/{d}"),
        Rule::new(Method::Post, "bigquery.googleapis.com", "/bigquery/v2/projects/{p}/queries"),
        Rule::new(Method::Get, "storage.googleapis.com", "/storage/v1/b/{b}/o/{o}"),
    ];
    let Ok(s) = std::str::from_utf8(input) else { return };
    let (raw, location) = s.split_once('\n').unwrap_or((s, ""));
    let list = AllowList::new(RULES).expect("valid rules");
    let Ok(url) = Url::parse(raw) else { return };
    let verify = |u: &Url| {
        for m in [Method::Get, Method::Post, Method::Put, Method::Delete, Method::Patch] {
            if list.check(m, u).is_ok() {
                assert_eq!(u.scheme(), "https");
                assert!(u.port().is_none() && u.username().is_empty() && u.fragment().is_none());
                let host = u.host_str().unwrap_or_default();
                let path = u.path();
                assert!(!path.contains("//") && !path.ends_with('/'));
                for seg in path[1..].split('/') {
                    let lower = seg.to_ascii_lowercase();
                    assert!(
                        !lower.contains("%2f") && !lower.contains("%5c") && !lower.contains("%00")
                    );
                    assert!(!matches!(
                        lower.as_str(),
                        "." | ".." | "%2e" | "%2e%2e" | ".%2e" | "%2e."
                    ));
                }
                assert!(RULES.iter().any(|r| r.method == m
                    && r.host == host
                    && r.path.split('/').count() == path.split('/').count()));
            }
        }
        let _ = query_sql(Method::Post, u, location.as_bytes());
    };
    verify(&url);
    // A redirect is never followed; even if a caller did, the target must
    // face the same check. Only an https request can receive one: the client
    // sends nothing else, and resolving against other schemes (`file:` with
    // control bytes) trips a debug assertion inside `url` itself.
    if url.scheme() != "https" {
        return;
    }
    if let Ok(next) = url.join(location) {
        verify(&next);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const H: &str = "bigquery.googleapis.com";
    const RULES: &[Rule<'static>] = &[
        Rule::new(Method::Get, H, "/bigquery/v2/projects/{project}/datasets/{dataset}"),
        Rule::new(Method::Get, H, "/bigquery/v2/projects/{p}/datasets/{d}/tables/{t}"),
        Rule::new(Method::Post, H, "/bigquery/v2/projects/{project}/queries"),
    ];

    fn list() -> AllowList<'static> {
        AllowList::new(RULES).unwrap()
    }

    fn verdict(m: Method, u: &str) -> Result<(), PolicyError> {
        list().check(m, &Url::parse(u).unwrap())
    }

    fn reason(m: Method, u: &str) -> &'static str {
        match verdict(m, u) {
            Err(PolicyError::Endpoint { reason }) => reason,
            other => panic!("{u}: {other:?}"),
        }
    }

    #[test]
    fn listed_requests_pass() {
        let base = "https://bigquery.googleapis.com/bigquery/v2/projects";
        verdict(Method::Get, &format!("{base}/p/datasets/d")).unwrap();
        verdict(Method::Get, &format!("{base}/my-proj/datasets/d_1/tables/t?view=BASIC")).unwrap();
        verdict(Method::Post, &format!("{base}/p/queries")).unwrap();
        verdict(Method::Get, "https://BigQuery.GoogleAPIs.com/bigquery/v2/projects/p/datasets/d")
            .unwrap();
        verdict(Method::Get, &format!("{base}/a%20b/datasets/d")).unwrap();
    }

    #[test]
    fn the_policy_matrix_refuses_everything_else() {
        let base = "https://bigquery.googleapis.com/bigquery/v2/projects";
        let cases: &[(Method, String, &str)] = &[
            (Method::Delete, format!("{base}/p/datasets/d"), "not on the allow list"),
            (Method::Post, format!("{base}/p/datasets/d"), "not on the allow list"),
            (Method::Get, format!("{base}/p/queries"), "not on the allow list"),
            (Method::Get, format!("{base}/p/datasets/d/tables/t/data"), "not on the allow list"),
            (Method::Get, format!("{base}/p/datasets"), "not on the allow list"),
            (
                Method::Get,
                "https://evil.example/bigquery/v2/projects/p/datasets/d".into(),
                "not on the allow list",
            ),
            (
                Method::Get,
                "https://bigquery.googleapis.com.evil.example/bigquery/v2/projects/p/datasets/d"
                    .into(),
                "not on the allow list",
            ),
            (
                Method::Get,
                "http://bigquery.googleapis.com/bigquery/v2/projects/p/datasets/d".into(),
                "scheme must be https",
            ),
            (
                Method::Get,
                "http://127.0.0.1:9/bigquery/v2/projects/p/datasets/d".into(),
                "scheme must be https",
            ),
            (
                Method::Get,
                "https://bigquery.googleapis.com:8443/bigquery/v2/projects/p/datasets/d".into(),
                "non-default port",
            ),
            (
                Method::Get,
                "https://u:pw@bigquery.googleapis.com/bigquery/v2/projects/p/datasets/d".into(),
                "credentials in the URL",
            ),
            (Method::Get, format!("{base}/p/datasets/d#x"), "fragment"),
            (Method::Get, format!("{base}/p%2Fx/datasets/d"), "path is not normalised"),
            (Method::Get, format!("{base}/p%2fx/datasets/d"), "path is not normalised"),
            (Method::Get, format!("{base}/p%5Cx/datasets/d"), "path is not normalised"),
            (Method::Get, format!("{base}/p%00/datasets/d"), "path is not normalised"),
            (Method::Get, format!("{base}/p%0a/datasets/d"), "path is not normalised"),
            (Method::Get, format!("{base}/p%zz/datasets/d"), "path is not normalised"),
            (Method::Get, format!("{base}//datasets/d"), "path is not normalised"),
            (Method::Get, format!("{base}/p/datasets/d/"), "path is not normalised"),
            (Method::Get, format!("{base}/p/%64atasets/d"), "not on the allow list"),
        ];
        for (m, u, want) in cases {
            assert_eq!(reason(*m, u), *want, "{m:?} {u}");
        }
    }

    #[test]
    fn dot_segments_never_escape_a_template() {
        // The URL parser resolves dot segments (in any spelling) before the
        // check, so what is checked is what is sent.
        let u = "https://bigquery.googleapis.com/bigquery/v2/projects/p/datasets/d/tables/%2e%2e/%2E%2E/x";
        assert_eq!(Url::parse(u).unwrap().path(), "/bigquery/v2/projects/p/datasets/x");
        verdict(Method::Get, u).unwrap();
        let out = "https://bigquery.googleapis.com/bigquery/v2/projects/p/datasets/d/../../../../../../storage/v1/b/x";
        assert_eq!(Url::parse(out).unwrap().path(), "/storage/v1/b/x");
        assert_eq!(reason(Method::Get, out), "not on the allow list");
        // Segments that are dots only, in any spelling, are refused outright.
        for s in [".", "..", "%2e", "%2E%2e", ".%2e"] {
            assert!(!valid_segment(s.as_bytes()), "{s}");
        }
        assert!(["...", ".a", "a%2e"].iter().all(|s| valid_segment(s.as_bytes())));
    }

    #[test]
    fn custom_method_suffixes_match_exactly() {
        let rules = [Rule::new(
            Method::Post,
            "datalineage.googleapis.com",
            "/v1/projects/{p}/locations/{l}:searchLinks",
        )];
        let l = AllowList::new(&rules).unwrap();
        let at = |p: &str| {
            l.check(
                Method::Post,
                &Url::parse(&format!("https://datalineage.googleapis.com{p}")).unwrap(),
            )
        };
        at("/v1/projects/p/locations/us:searchLinks").unwrap();
        for bad in [
            "/v1/projects/p/locations/:searchLinks",
            "/v1/projects/p/locations/us:searchLinksX",
            "/v1/projects/p/locations/us:batchSearchLinkProcesses",
            "/v1/projects/p/locations/us",
            "/v1/projects/p/locations/..:searchLinks",
            "/v1/projects/p/locations/us%3AsearchLinks",
        ] {
            assert!(at(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn malformed_rules_are_refused() {
        for (h, p) in [
            ("Bigquery.googleapis.com", "/x"),
            ("bigquery.googleapis.com:443", "/x"),
            ("", "/x"),
            ("h", "x"),
            ("h", "/x//y"),
            ("h", "/x/"),
            ("h", "/x/../y"),
            ("h", "/x/%2e"),
            ("h", "/{}"),
            ("h", "/{a b}"),
            ("h", "/{a}x"),
            ("h", "/{a}:"),
            ("h", "/{a}:{b}"),
            ("h", "/x:{a}"),
        ] {
            assert!(AllowList::new(&[Rule::new(Method::Get, h, p)]).is_err(), "{h} {p}");
        }
    }

    #[test]
    fn allow_lists_allow_no_queries_and_permissive_allows_all() {
        assert!(matches!(list().check_query("SELECT 1"), Err(PolicyError::Query { .. })));
        let u = Url::parse("ftp://anything/at/all").unwrap();
        assert_eq!(Permissive.check(Method::Delete, &u), Ok(()));
        assert_eq!(Permissive.check_query("DROP TABLE x"), Ok(()));
        let l = list();
        let borrowed: &AllowList<'_> = &l;
        let verdict = <&AllowList<'_> as RequestPolicy>::check(&borrowed, Method::Get, &u);
        assert!(verdict.is_err(), "a borrowed policy is the policy");
    }

    fn sql(m: Method, u: &str, body: &str) -> Result<Option<String>, PolicyError> {
        query_sql(m, &Url::parse(u).unwrap(), body.as_bytes()).map(|o| o.map(Cow::into_owned))
    }

    const Q: &str = "https://bigquery.googleapis.com/bigquery/v2/projects/p/queries";
    const J: &str = "https://bigquery.googleapis.com/bigquery/v2/projects/p/jobs";

    #[test]
    fn query_text_is_isolated_from_both_job_endpoints() {
        let body = r#"{"query":"SELECT 1","useLegacySql":false,"location":"US"}"#;
        assert_eq!(sql(Method::Post, Q, body).unwrap().as_deref(), Some("SELECT 1"));
        let esc = r#"{"query":"SELECT \"a\" ","useLegacySql":false}"#;
        assert_eq!(sql(Method::Post, Q, esc).unwrap().as_deref(), Some("SELECT \"a\" "));
        let job = r#"{"configuration":{"query":{"query":"SELECT 2","useLegacySql":false}}}"#;
        assert_eq!(sql(Method::Post, J, job).unwrap().as_deref(), Some("SELECT 2"));
        // Any spelling of the endpoint, any host, a loopback stand-in.
        for u in [
            "https://bigquery.googleapis.com/bigquery/v2/projects/p/queries/",
            "https://x.example/%62igQuery/v2/projects/p/%71ueries",
            "http://127.0.0.1:1/bigquery/v2/projects/p/queries;x=y",
            "https://bigquery.us-east4.rep.googleapis.com/v2/projects/p/queries",
        ] {
            assert_eq!(sql(Method::Post, u, body).unwrap().as_deref(), Some("SELECT 1"), "{u}");
        }
    }

    #[test]
    fn uncheckable_query_bodies_are_refused() {
        let bad = |u: &str, b: &str| {
            assert!(
                matches!(sql(Method::Post, u, b), Err(PolicyError::UncheckableQuery { .. })),
                "{u} {b}"
            );
        };
        bad(Q, "");
        bad(Q, "SELECT 1");
        bad(Q, r#"{"query":"SELECT 1"}"#); // legacy SQL is the API default
        bad(Q, r#"{"query":"SELECT 1","useLegacySql":true}"#);
        bad(Q, r##"{"query":"#legacySQL\nSELECT 1","useLegacySql":false}"##);
        bad(Q, r#"{"query":"SELECT 1","query":"SELECT 2","useLegacySql":false}"#);
        bad(Q, r#"{"query":"SELECT 1","useLegacySql":false,"use_legacy_sql":true}"#);
        bad(Q, r#"{"query":null,"useLegacySql":false}"#);
        bad(Q, "\u{feff}{\"query\":\"SELECT 1\",\"useLegacySql\":false}");
        bad(J, r#"{"configuration":{"extract":{}}}"#);
        bad(J, r#"{"configuration":{"load":{}}}"#);
        bad(
            J,
            r#"{"configuration":{"query":{"query":"SELECT 1","useLegacySql":false},"extract":{}}}"#,
        );
        bad(
            J,
            r#"{"configuration":{"query":{"query":"SELECT 1","useLegacySql":false,"tableDefinitions":{}}}}"#,
        );
        bad(
            J,
            r#"{"configuration":{"query":{"query":"SELECT 1","useLegacySql":false,"user_defined_function_resources":[]}}}"#,
        );
        bad("https://bigquery.googleapis.com/batch/bigquery/v2", "--b\r\nPOST /x");
        bad("https://upload.example/upload/bigquery/v2/projects/p/jobs", "--multipart");
        // Any method that carries a body is inspected.
        assert!(sql(Method::Put, Q, "x").is_err());
    }

    #[test]
    fn other_requests_carry_no_query() {
        assert_eq!(sql(Method::Get, Q, ""), Ok(None), "getQueryResults");
        assert_eq!(sql(Method::Get, J, ""), Ok(None), "jobs.list");
        assert_eq!(sql(Method::Post, &format!("{J}/abc/cancel"), ""), Ok(None));
        let vertex = "https://us-central1-aiplatform.googleapis.com/v1/projects/p/locations/l/jobs";
        assert_eq!(sql(Method::Post, vertex, "{}"), Ok(None));
        assert_eq!(sql(Method::Post, "https://storage.googleapis.com/b/queries", "x"), Ok(None));
    }

    #[test]
    fn the_fuzz_entry_point_accepts_known_inputs() {
        for s in [
            "https://bigquery.googleapis.com/bigquery/v2/projects/p/queries\n/x/../y",
            "https://bigquery.googleapis.com/bigquery/v2/projects/p/datasets/d\nhttps://evil.example/",
            "not a url",
            "",
        ] {
            fuzz(s.as_bytes());
        }
    }
}
