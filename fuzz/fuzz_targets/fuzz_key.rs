// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! Arbitrary bytes as a key or prefix: classified without panicking, and
//! anything accepted can never name a path outside a store root.

#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    objstore_r::key::fuzz(data);
});
