// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! JSON API response parsing (a small, fuzzed surface).

use crate::{Error, Meta, Result};

#[derive(serde::Deserialize)]
struct ObjMeta {
    name: String,
    #[serde(default)]
    size: Option<NumStr>,
    generation: NumStr,
}

/// The JSON API sends 64-bit numbers as strings; emulators sometimes as numbers.
#[derive(serde::Deserialize)]
#[serde(untagged)]
enum NumStr {
    S(String),
    N(u64),
}

impl NumStr {
    fn get(&self) -> Option<u64> {
        match self {
            NumStr::S(s) => s.parse().ok(),
            NumStr::N(n) => Some(*n),
        }
    }
}

#[derive(serde::Deserialize)]
struct ListPage {
    #[serde(default)]
    items: Vec<ObjMeta>,
    #[serde(rename = "nextPageToken", default)]
    next_page_token: Option<String>,
}

fn to_meta(m: ObjMeta) -> Result<Meta> {
    let generation =
        m.generation.get().filter(|g| *g != 0).ok_or_else(|| Error::protocol("bad generation"))?;
    let size = match m.size {
        Some(s) => s.get().ok_or_else(|| Error::protocol("bad size"))?,
        None => 0,
    };
    Ok(Meta { key: m.name, size, generation })
}

/// An object resource.
pub(crate) fn parse_meta(body: &[u8]) -> Result<Meta> {
    let m: ObjMeta =
        serde_json::from_slice(body).map_err(|_| Error::protocol("malformed object resource"))?;
    to_meta(m)
}

/// One page of a listing.
pub(crate) fn parse_list(body: &[u8]) -> Result<(Vec<Meta>, Option<String>)> {
    let p: ListPage =
        serde_json::from_slice(body).map_err(|_| Error::protocol("malformed listing"))?;
    let items = p.items.into_iter().map(to_meta).collect::<Result<Vec<_>>>()?;
    Ok((items, p.next_page_token))
}

/// Fuzzing entry point: parse anything, never panic.
#[doc(hidden)]
pub fn fuzz(body: &[u8]) {
    let _ = parse_meta(body);
    let _ = parse_list(body);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_real_shapes() {
        let m =
            parse_meta(br#"{"name":"a/b","size":"12","generation":"1695000000123456"}"#).unwrap();
        assert_eq!((m.key.as_str(), m.size, m.generation), ("a/b", 12, 1_695_000_000_123_456));
        let m = parse_meta(br#"{"name":"x","size":3,"generation":7}"#).unwrap();
        assert_eq!((m.size, m.generation), (3, 7));
        let (items, next) = parse_list(
            br#"{"items":[{"name":"a","size":"1","generation":"2"}],"nextPageToken":"t"}"#,
        )
        .unwrap();
        assert_eq!((items.len(), next.as_deref()), (1, Some("t")));
        let (items, next) = parse_list(b"{}").unwrap();
        assert!(items.is_empty() && next.is_none());
        for bad in [
            &b""[..],
            b"null",
            b"[]",
            br#"{"name":"a","generation":"0"}"#,
            br#"{"name":"a","generation":"x"}"#,
        ] {
            assert!(parse_meta(bad).is_err());
        }
    }
}
