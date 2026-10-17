// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
// Request policies on the authorised transport, end to end against a stub
// server: only allow-listed requests arrive, query jobs reach the server only
// after `check_query` accepted their SQL, redirects are never followed. Plus
// the security property (no method x host x path outside the list passes)
// and the per-request cost of a check.

#![cfg(all(feature = "http", feature = "testing"))]

#[path = "support/stub_http.rs"]
mod stub;

use objstore_r::http::{Client, HttpConfig};
use objstore_r::policy::{AllowList, Permissive, RequestPolicy, Rule, Url};
use objstore_r::token::Static;
use objstore_r::transport::{Method, Request, Transport};
use objstore_r::{Bytes, Error, PolicyError};
use proptest::prelude::*;
use std::borrow::Cow;
use stub::{resp, scripted};

const BQ: &str = "bigquery.googleapis.com";
const RULES: &[Rule<'static>] = &[
    Rule::new(Method::Get, BQ, "/bigquery/v2/projects/{project}/datasets/{dataset}"),
    Rule::new(Method::Get, BQ, "/bigquery/v2/projects/{project}/datasets/{dataset}/tables/{table}"),
    Rule::new(Method::Post, BQ, "/bigquery/v2/projects/{project}/queries"),
    Rule::new(Method::Get, BQ, "/bigquery/v2/projects/{project}/queries/{job}"),
];

fn list() -> AllowList<'static> {
    AllowList::new(RULES).unwrap().with_loopback_http()
}

/// A stand-in for a SQL guard: allow-listed endpoints, and only queries that
/// read `INFORMATION_SCHEMA` (a toy rule; real guards parse the SQL).
struct MetadataOnly<'a>(AllowList<'a>);

impl RequestPolicy for MetadataOnly<'_> {
    fn check(&self, m: Method, u: &Url) -> Result<(), PolicyError> {
        self.0.check(m, u)
    }
    fn check_query(&self, sql: &str) -> Result<(), PolicyError> {
        if sql.starts_with("SELECT") && sql.ends_with("FROM INFORMATION_SCHEMA.TABLES") {
            Ok(())
        } else {
            Err(PolicyError::Query { reason: Cow::Borrowed("not a metadata read") })
        }
    }
}

fn client<P: RequestPolicy>(policy: P) -> Client<Static, P> {
    let cfg = HttpConfig { max_retries: 0, ..HttpConfig::default() };
    Client::with_policy(Static::new("tok"), policy, cfg).unwrap()
}

fn first_line(head: &str) -> &str {
    head.lines().next().unwrap_or("")
}

#[tokio::test(flavor = "current_thread")]
async fn only_allow_listed_requests_arrive() {
    let script = [resp("200 OK", "", "{}"), resp("200 OK", "", "{}")];
    let (seen, refused) = scripted(&script, |ep| async move {
        let c = client(list());
        let base = format!("{ep}/bigquery/v2/projects/p");
        let attempts = [
            (Method::Get, format!("{base}/datasets/d")),
            (Method::Delete, format!("{base}/datasets/d")),
            (Method::Get, format!("{base}/datasets/d/tables/t/data")),
            (Method::Get, format!("{base}/datasets/a%2Fb")),
            (Method::Get, format!("{base}/datasets/d/")),
            (Method::Get, format!("{base}/datasets/d#frag")),
            (Method::Get, format!("{ep}/storage/v1/b/x/o/y?alt=media")),
            (Method::Get, format!("{base}/datasets/d/tables/t?view=BASIC")),
        ];
        let mut refused = Vec::new();
        for (m, url) in &attempts {
            let req = Request { method: *m, ..Request::get(url) };
            match c.send(req).await {
                Ok(r) => assert_eq!(r.status, 200),
                Err(Error::Policy(PolicyError::Endpoint { reason })) => refused.push(reason),
                Err(e) => panic!("{url}: {e}"),
            }
        }
        refused
    })
    .await;
    let lines: Vec<&str> = seen.iter().map(|h| first_line(h)).collect();
    assert_eq!(
        lines,
        [
            "GET /bigquery/v2/projects/p/datasets/d HTTP/1.1",
            "GET /bigquery/v2/projects/p/datasets/d/tables/t?view=BASIC HTTP/1.1",
        ]
    );
    assert_eq!(refused.len(), 6, "{refused:?}");
}

#[tokio::test(flavor = "current_thread")]
async fn a_query_arrives_only_after_its_sql_was_accepted() {
    let script = [resp("200 OK", "", r#"{"jobComplete":true}"#)];
    let (seen, outcomes) = scripted(&script, |ep| async move {
        let c = client(MetadataOnly(list()));
        let url = format!("{ep}/bigquery/v2/projects/p/queries");
        let body = |sql: &str| {
            Bytes::from(format!(r#"{{"query":"{sql}","useLegacySql":false,"location":"US"}}"#))
        };
        let mut out = Vec::new();
        for b in [
            body("SELECT * FROM secret.payroll"),
            body("SELECT table_name FROM INFORMATION_SCHEMA.TABLES"),
            Bytes::from_static(br#"{"query":"SELECT 1 FROM INFORMATION_SCHEMA.TABLES"}"#),
            Bytes::from_static(b"not json"),
        ] {
            out.push(c.send(Request::post_json(&url, b)).await.map(|r| r.status));
        }
        out
    })
    .await;
    assert_eq!(seen.len(), 1, "exactly the accepted query arrived");
    assert!(seen[0].contains("INFORMATION_SCHEMA.TABLES"));
    assert!(matches!(outcomes[0], Err(Error::Policy(PolicyError::Query { .. }))));
    assert!(matches!(outcomes[1], Ok(200)));
    assert!(matches!(outcomes[2], Err(Error::Policy(PolicyError::UncheckableQuery { .. }))));
    assert!(matches!(outcomes[3], Err(Error::Policy(PolicyError::UncheckableQuery { .. }))));
}

#[tokio::test(flavor = "current_thread")]
async fn the_query_hook_holds_under_every_policy_and_both_entry_points() {
    // Permissive allows every query, but an uncheckable body never leaves;
    // `open` goes through the same gate as `send`.
    let c = client(Permissive);
    let url = "http://127.0.0.1:1/bigquery/v2/projects/p/jobs";
    let extract =
        Bytes::from_static(br#"{"configuration":{"extract":{"destinationUris":["gs://x/*"]}}}"#);
    let r = c.send(Request::post_json(url, extract.clone())).await;
    assert!(matches!(r, Err(Error::Policy(PolicyError::UncheckableQuery { .. }))));
    let r = c.open(Request::post_json(url, extract)).await;
    assert!(matches!(r, Err(Error::Policy(PolicyError::UncheckableQuery { .. }))));
    // An allow list with no query rule refuses the endpoint itself.
    let strict = client(AllowList::new(&[]).unwrap().with_loopback_http());
    let ok = Bytes::from_static(br#"{"query":"SELECT 1","useLegacySql":false}"#);
    let r = strict
        .open(Request::post_json("http://127.0.0.1:1/bigquery/v2/projects/p/queries", ok))
        .await;
    assert!(matches!(r, Err(Error::Policy(PolicyError::Endpoint { .. }))));
    // A method override cannot disguise the checked method.
    let hdrs = [("X-HTTP-Method-Override", "DELETE")];
    let r = c.send(Request::get("http://127.0.0.1:1/x").headers(&hdrs)).await;
    assert!(matches!(r, Err(Error::Protocol { .. })));
}

#[tokio::test(flavor = "current_thread")]
async fn redirects_to_listed_endpoints_are_still_not_followed() {
    let script = [resp("302 Found", "location: /bigquery/v2/projects/p/datasets/other\r\n", "")];
    let (seen, r) = scripted(&script, |ep| async move {
        let url = format!("{ep}/bigquery/v2/projects/p/datasets/d");
        client(list()).send(Request::get(&url)).await
    })
    .await;
    assert_eq!(seen.len(), 1, "one request: the redirect was not followed");
    assert_eq!(r.unwrap().status, 302);
}

// ---- security: the allow list admits nothing outside itself ----------------

/// An independent oracle: is `method url` one of RULES, with every
/// placeholder filled by a plain segment?
fn listed(method: Method, url: &Url) -> bool {
    if url.scheme() != "https" || url.port().is_some() || url.host_str() != Some(BQ) {
        return false;
    }
    if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
        return false;
    }
    let segs: Vec<&str> = url.path().split('/').skip(1).collect();
    RULES.iter().any(|r| {
        let t: Vec<&str> = r.path.split('/').skip(1).collect();
        r.method == method
            && t.len() == segs.len()
            && t.iter().zip(&segs).all(|(t, s)| {
                if t.starts_with('{') {
                    let lower = s.to_ascii_lowercase();
                    let dots = lower.replace("%2e", ".");
                    !s.is_empty()
                        && dots != "."
                        && dots != ".."
                        && !["%2f", "%5c", "%00", "%0", "%1", "%7f"]
                            .iter()
                            .any(|b| lower.contains(b))
                        && !s.contains('\\')
                } else {
                    t == s
                }
            })
    })
}

fn segment() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("bigquery".to_owned()),
        Just("v2".to_owned()),
        Just("projects".to_owned()),
        Just("datasets".to_owned()),
        Just("tables".to_owned()),
        Just("queries".to_owned()),
        Just("data".to_owned()),
        Just("jobs".to_owned()),
        Just("p".to_owned()),
        Just("%71ueries".to_owned()),
        Just("Datasets".to_owned()),
        Just("..".to_owned()),
        Just("%2e%2E".to_owned()),
        Just(".".to_owned()),
        Just(String::new()),
        Just("a%2Fb".to_owned()),
        Just("a%5cb".to_owned()),
        Just("a%00".to_owned()),
        Just("x;y=z".to_owned()),
        "[a-zA-Z0-9_.~%-]{1,6}",
    ]
}

fn method() -> impl Strategy<Value = Method> {
    prop_oneof![
        Just(Method::Get),
        Just(Method::Post),
        Just(Method::Put),
        Just(Method::Delete),
        Just(Method::Patch)
    ]
}

fn origin() -> impl Strategy<Value = &'static str> {
    prop_oneof![
        Just("https://bigquery.googleapis.com"),
        Just("https://BIGQUERY.googleapis.com"),
        Just("https://bigquery.googleapis.com:443"),
        Just("https://bigquery.googleapis.com:8443"),
        Just("https://bigquery.googleapis.com."),
        Just("https://bigquery.googleapis.com.evil.example"),
        Just("https://evil.example"),
        Just("https://u@bigquery.googleapis.com"),
        Just("http://bigquery.googleapis.com"),
        Just("https://10.0.0.1"),
        Just("http://127.0.0.1:9"),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 4096, ..ProptestConfig::default() })]

    #[test]
    fn nothing_outside_the_list_passes(
        m in method(),
        o in origin(),
        r in 0..RULES.len(),
        edits in prop::collection::vec((prop::bool::weighted(0.25), segment()), 8),
        extra in prop::collection::vec(segment(), 0..2),
        cut in prop::bool::weighted(0.1),
        tail in prop_oneof![Just(""), Just("?view=BASIC"), Just("#f"), Just("/")],
    ) {
        // Start from a listed template and perturb it: near misses are where
        // a matcher goes wrong.
        let mut segs: Vec<String> = RULES[r].path.split('/').skip(1).zip(&edits).map(|(t, (swap, alt))| {
            match (t.starts_with('{'), swap) {
                (_, true) => alt.clone(),
                (true, false) => "x".to_owned(),
                (false, false) => t.to_owned(),
            }
        }).collect();
        if cut {
            segs.pop();
        }
        segs.extend(extra);
        let raw = format!("{o}/{}{tail}", segs.join("/"));
        let Ok(url) = Url::parse(&raw) else { return Ok(()) };
        // The production list, without the loopback stand-in.
        let policy = AllowList::new(RULES).unwrap();
        if policy.check(m, &url).is_ok() {
            prop_assert!(listed(m, &url), "admitted {m:?} {url}");
        }
    }

    #[test]
    fn every_plainly_listed_request_passes(
        r in 0..RULES.len(),
        fill in prop::collection::vec("[a-z0-9_-]{1,12}", 3),
    ) {
        let rule = RULES[r];
        let mut i = 0;
        let path: Vec<String> = rule.path.split('/').map(|s| {
            if s.starts_with('{') { i += 1; fill[(i - 1) % fill.len()].clone() } else { s.to_owned() }
        }).collect();
        let url = Url::parse(&format!("https://{BQ}{}", path.join("/"))).unwrap();
        prop_assert!(AllowList::new(RULES).unwrap().check(rule.method, &url).is_ok(), "{url}");
    }
}

// ---- performance -------------------------------------------------------------

/// The budget is one microsecond per check; measured in release builds only
/// (the bench `policy_bench` gives the distribution).
#[test]
#[cfg_attr(debug_assertions, ignore = "timed in release builds")]
fn a_policy_check_costs_under_a_microsecond() {
    const N: u32 = 200_000;
    let policy = AllowList::new(RULES).unwrap();
    let hit = Url::parse("https://bigquery.googleapis.com/bigquery/v2/projects/my-project/datasets/analytics/tables/events?view=BASIC").unwrap();
    let miss = Url::parse(
        "https://bigquery.googleapis.com/bigquery/v2/projects/p/datasets/d/tables/t/data",
    )
    .unwrap();
    let mut best = std::time::Duration::MAX;
    for _ in 0..5 {
        let t = std::time::Instant::now();
        for i in 0..N {
            let u = if i % 2 == 0 { &hit } else { &miss };
            let _ = std::hint::black_box(policy.check(Method::Get, std::hint::black_box(u)));
        }
        best = best.min(t.elapsed() / N);
    }
    assert!(best < std::time::Duration::from_micros(1), "{best:?} per check");
}
