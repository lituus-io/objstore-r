// Copyright: lituus-io, all rights reserved.
// Author: terekete <spicyzhug@gmail.com>
//
//! Arbitrary bytes as a GCS JSON API response (object resource or listing
//! page): typed error or clean parse, never a panic.

#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    objstore_r::gcs::fuzz_wire(data);
});
