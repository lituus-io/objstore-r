// Author: support@lituus.io
// Copyright: lituus-io, all rights reserved
//
//! Arbitrary bytes as a stored gzip body: a body within the decode bound or a
//! typed error (corrupt, too large), never a panic or an unbounded inflate.

#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    objstore_r::encoding::fuzz(data);
});
