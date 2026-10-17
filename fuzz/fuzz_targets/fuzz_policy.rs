// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! Arbitrary bytes as a URL plus a redirect `Location` (one line each),
//! through the allow list and the BigQuery query hook: percent-encoding,
//! dot segments and encoded slashes never admit a URL outside the list, and
//! nothing panics.

#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    objstore_r::policy::fuzz(data);
});
