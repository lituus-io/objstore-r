// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! Fenced single-writer publishing, the protocol every snapshot publisher
//! ([`crate::mirror`] for graph-r stores, link-r's `remote` for indexes)
//! shares:
//!
//! 1. upload immutable objects create-only ([`put_immutable`]);
//! 2. swap a small sealed `HEAD` record with compare-and-swap ([`cas`]);
//! 3. readers fetch what `HEAD` names and verify it ([`fetch_verified`]).
//!
//! A lost response (the write landed, the reply did not) is recognised by
//! reading back, so retries are safe; anything else that loses a race is
//! [`Error::Fenced`]. Records and objects are checked with xxh3, the one
//! hash family of the stack.

use crate::{Bytes, Cond, Error, Key, ObjectStore, Result};
use graph_r::hash::xxh3_64;

/// Normalize a publish prefix: empty, or `p/` (a trailing `/` implied),
/// validated so `{prefix}HEAD` is a key.
pub fn prefix(p: &str) -> Result<String> {
    let p = p.trim_end_matches('/');
    if p.is_empty() {
        return Ok(String::new());
    }
    let p = format!("{p}/");
    Key::parse(&format!("{p}HEAD"))?;
    Ok(p)
}

/// Write `body` at `key` if it is still at generation `prev` (`None`: if it
/// does not exist); returns the new generation. Losing the race is
/// [`Error::Fenced`]; on any other error the outcome is unknown and the
/// caller must re-read before trying again.
pub async fn cas<O: ObjectStore>(o: &O, key: &str, prev: Option<u64>, body: Bytes) -> Result<u64> {
    let cond = prev.map_or(Cond::CreateOnly, Cond::IfGeneration);
    match o.put(key, body.clone(), cond).await {
        Ok(g) => Ok(g),
        Err(Error::Conflict | Error::PreconditionFailed) => match o.get(key).await? {
            Some(obj) if obj.bytes == body => Ok(obj.generation),
            _ => Err(Error::Fenced { reason: "another writer published first" }),
        },
        Err(e) => Err(e),
    }
}

/// Upload an immutable object create-only. An existing object with the same
/// bytes (a retried publish) is accepted; different bytes fence.
pub async fn put_immutable<O: ObjectStore>(o: &O, key: &str, body: Bytes) -> Result<()> {
    let sum = xxh3_64(&body);
    match o.put(key, body, Cond::CreateOnly).await {
        Ok(_) => Ok(()),
        Err(Error::Conflict) => match o.get(key).await? {
            Some(obj) if xxh3_64(&obj.bytes) == sum => Ok(()),
            _ => Err(Error::Fenced { reason: "an immutable object already holds other bytes" }),
        },
        Err(e) => Err(e),
    }
}

/// Fetch `key` and check it against the length and xxh3 a record named;
/// missing or different bytes are [`Error::Corrupt`].
pub async fn fetch_verified<O: ObjectStore>(o: &O, key: &str, len: u64, sum: u64) -> Result<Bytes> {
    let obj = o.get(key).await?.ok_or_else(|| Error::corrupt(format!("{key} is missing")))?;
    if obj.bytes.len() as u64 != len || xxh3_64(&obj.bytes) != sum {
        return Err(Error::corrupt(format!("{key} does not match its record")));
    }
    Ok(obj.bytes)
}

/// Seal a record: `magic u32 | version u16 | 0 u16 | body | xxh3 u64`.
#[must_use]
pub fn seal(magic: u32, version: u16, body: &[u8]) -> Vec<u8> {
    let mut o = Vec::with_capacity(16 + body.len());
    o.extend_from_slice(&magic.to_le_bytes());
    o.extend_from_slice(&version.to_le_bytes());
    o.extend_from_slice(&0u16.to_le_bytes());
    o.extend_from_slice(body);
    let sum = xxh3_64(&o);
    o.extend_from_slice(&sum.to_le_bytes());
    o
}

/// Open a [`seal`]ed record of `magic` and `version`: its body, or
/// [`Error::Corrupt`] for any deviation (never a panic).
pub fn unseal(bytes: &[u8], magic: u32, version: u16) -> Result<&[u8]> {
    let Some(body_end) = bytes.len().checked_sub(8).filter(|&e| e >= 8) else {
        return Err(Error::corrupt("record truncated"));
    };
    let (head, tail) = bytes.split_at(body_end);
    let mut sum = [0u8; 8];
    sum.copy_from_slice(tail);
    if xxh3_64(head) != u64::from_le_bytes(sum) {
        return Err(Error::corrupt("record checksum mismatch"));
    }
    if head[..4] != magic.to_le_bytes() || head[4..6] != version.to_le_bytes() {
        return Err(Error::corrupt("record has an unknown magic or version"));
    }
    Ok(&head[8..])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Mem;
    use futures::executor::block_on;

    #[test]
    fn records_round_trip_and_reject_damage() {
        let r = seal(0x4142_4344, 2, b"payload");
        assert_eq!(unseal(&r, 0x4142_4344, 2).unwrap(), b"payload");
        assert!(unseal(&r, 0x4142_4344, 3).is_err());
        assert!(unseal(&r[..r.len() - 1], 0x4142_4344, 2).is_err());
        let mut bad = r.clone();
        bad[9] ^= 1;
        assert!(matches!(unseal(&bad, 0x4142_4344, 2), Err(Error::Corrupt { .. })));
        assert!(unseal(&[], 1, 1).is_err() && unseal(&[0; 15], 1, 1).is_err());
        assert_eq!(prefix("").unwrap(), "");
        assert_eq!(prefix("kb/a/").unwrap(), "kb/a/");
        assert!(prefix("bad key").is_err());
    }

    #[test]
    fn two_writers_one_wins_and_retries_are_safe() {
        block_on(async {
            let m = Mem::default();
            let g = cas(&m, "HEAD", None, "a".into()).await.unwrap();
            // A retry of the same write (lost response) is recognised.
            assert_eq!(cas(&m, "HEAD", None, "a".into()).await.unwrap(), g);
            // A second writer from the same starting point is fenced.
            assert!(matches!(cas(&m, "HEAD", None, "b".into()).await, Err(Error::Fenced { .. })));
            let g2 = cas(&m, "HEAD", Some(g), "c".into()).await.unwrap();
            assert!(matches!(
                cas(&m, "HEAD", Some(g), "d".into()).await,
                Err(Error::Fenced { .. })
            ));
            assert_ne!(g, g2);

            put_immutable(&m, "img/1", "x".into()).await.unwrap();
            put_immutable(&m, "img/1", "x".into()).await.unwrap();
            assert!(matches!(
                put_immutable(&m, "img/1", "y".into()).await,
                Err(Error::Fenced { .. })
            ));
            let sum = xxh3_64(b"x");
            assert_eq!(fetch_verified(&m, "img/1", 1, sum).await.unwrap(), "x");
            assert!(matches!(
                fetch_verified(&m, "img/1", 2, sum).await,
                Err(Error::Corrupt { .. })
            ));
            assert!(matches!(
                fetch_verified(&m, "img/2", 1, sum).await,
                Err(Error::Corrupt { .. })
            ));
        });
    }
}
