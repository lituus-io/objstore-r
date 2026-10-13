// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
// Model-based property test: random operation sequences give the same
// observable results on every backend as on a trivial model. Generations are
// checked relationally (they change on every write; a remembered generation
// matches until the next write), which is all callers may rely on.

use futures::executor::block_on;
use objstore_r::{Bytes, Cond, Error, LocalDir, Mem, ObjectStore};
use proptest::prelude::*;
use std::collections::BTreeMap;

const KEYS: [&str; 4] = ["a", "a/b", "c/d/e", "HEAD"];

#[derive(Debug, Clone)]
enum Op {
    Put { k: usize, v: u8, cond: CondKind },
    Get(usize),
    Range(usize, u8, u8),
    Delete(usize),
    List(bool),
}

#[derive(Debug, Clone, Copy)]
enum CondKind {
    None,
    CreateOnly,
    Current,
    Stale,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        (
            0..KEYS.len(),
            any::<u8>(),
            prop_oneof![
                Just(CondKind::None),
                Just(CondKind::CreateOnly),
                Just(CondKind::Current),
                Just(CondKind::Stale)
            ]
        )
            .prop_map(|(k, v, cond)| Op::Put { k, v, cond }),
        (0..KEYS.len()).prop_map(Op::Get),
        (0..KEYS.len(), any::<u8>(), any::<u8>()).prop_map(|(k, a, b)| Op::Range(
            k,
            a % 12,
            b % 12
        )),
        (0..KEYS.len()).prop_map(Op::Delete),
        any::<bool>().prop_map(Op::List),
    ]
}

/// `nests`: the backend can hold both `k` and `k/...` (not true of LocalDir).
fn run<S: ObjectStore>(s: &S, ops: &[Op], nests: bool) {
    // model: key -> (body, generation as last returned by the store)
    let mut model: BTreeMap<&str, (Vec<u8>, u64)> = BTreeMap::new();
    for op in ops {
        match op {
            Op::Put { k, v, cond } => {
                let key = KEYS[*k];
                let body = vec![*v; usize::from(*v % 9)];
                let current = model.get(key).map(|(_, g)| *g);
                let c = match cond {
                    CondKind::None => Cond::None,
                    CondKind::CreateOnly => Cond::CreateOnly,
                    CondKind::Current => Cond::IfGeneration(current.unwrap_or(1)),
                    CondKind::Stale => Cond::IfGeneration(current.map_or(1, |g| g ^ 0x5555)),
                };
                let expect_ok = match c {
                    Cond::None => true,
                    Cond::CreateOnly => current.is_none(),
                    Cond::IfGeneration(g) => current == Some(g),
                };
                match block_on(s.put(key, Bytes::from(body.clone()), c)) {
                    Ok(g) => {
                        assert!(expect_ok, "{op:?} should have failed");
                        assert!(current != Some(g), "a write must change the generation");
                        model.insert(key, (body, g));
                    }
                    Err(Error::Conflict | Error::PreconditionFailed) => {
                        assert!(!expect_ok, "{op:?} should have succeeded")
                    }
                    Err(Error::InvalidKey { .. }) if !nests => {
                        let collides = model.keys().any(|k| {
                            k.starts_with(&format!("{key}/")) || key.starts_with(&format!("{k}/"))
                        });
                        assert!(collides, "{op:?}: InvalidKey without a real collision");
                    }
                    Err(e) => panic!("{op:?}: {e}"),
                }
            }
            Op::Get(k) => {
                let got = block_on(s.get(KEYS[*k])).unwrap();
                let want = model.get(KEYS[*k]);
                match (got, want) {
                    (None, None) => {}
                    (Some(o), Some((b, g))) => {
                        assert!(o.bytes.as_ref() == b.as_slice() && o.generation == *g, "{op:?}")
                    }
                    (g, w) => panic!("{op:?}: store {g:?} vs model {w:?}"),
                }
            }
            Op::Range(k, a, b) => {
                let (a, b) = (u64::from(*a), u64::from(*b));
                match (block_on(s.get_range(KEYS[*k], a..b)), model.get(KEYS[*k])) {
                    (Err(Error::NotFound), None) => {}
                    (Ok(got), Some((body, _))) => {
                        let len = body.len() as u64;
                        let (st, en) = (a.min(len), b.clamp(a.min(len), len));
                        assert_eq!(got.as_ref(), &body[st as usize..en as usize], "{op:?}");
                    }
                    (r, m) => panic!("{op:?}: {r:?} vs {m:?}"),
                }
            }
            Op::Delete(k) => {
                block_on(s.delete(KEYS[*k])).unwrap();
                model.remove(KEYS[*k]);
            }
            Op::List(all) => {
                let prefix = if *all { "" } else { "c/" };
                let got: Vec<String> =
                    block_on(s.list(prefix)).unwrap().into_iter().map(|m| m.key).collect();
                let want: Vec<String> = model
                    .keys()
                    .filter(|k| k.starts_with(prefix))
                    .map(|k| (*k).to_owned())
                    .collect();
                assert_eq!(got, want, "{op:?}");
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: std::env::var("PROPTEST_CASES").ok().and_then(|v| v.parse().ok()).unwrap_or(128), ..ProptestConfig::default() })]

    #[test]
    fn mem_matches_the_model(ops in prop::collection::vec(op(), 1..60)) {
        run(&Mem::default(), &ops, true);
    }

    #[test]
    fn local_dir_matches_the_model(ops in prop::collection::vec(op(), 1..40)) {
        let dir = tempfile::tempdir().unwrap();
        run(&LocalDir::open(dir.path()).unwrap(), &ops, false);
    }
}
