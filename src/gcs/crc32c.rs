// Author: spicyzhug@gmail.com
// Copyright: lituus-io, all rights reserved
//
//! CRC-32C (Castagnoli), the checksum Cloud Storage keeps for every object's
//! stored bytes and sends as `x-goog-hash: crc32c=<base64>`. Used to verify an
//! encoded download *before* it is decoded: the checksum covers the
//! compressed bytes, and a decoder must never be the first thing to meet
//! damaged input. Slicing-by-8 over compile-time tables; no dependency.

/// The reflected Castagnoli polynomial.
const POLY: u32 = 0x82F6_3B78;

const TABLES: [[u32; 256]; 8] = tables();

const fn tables() -> [[u32; 256]; 8] {
    let mut t = [[0u32; 256]; 8];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 == 1 { (c >> 1) ^ POLY } else { c >> 1 };
            k += 1;
        }
        t[0][i] = c;
        i += 1;
    }
    let mut i = 0;
    while i < 256 {
        let mut s = 1;
        while s < 8 {
            let prev = t[s - 1][i];
            t[s][i] = (prev >> 8) ^ t[0][(prev & 0xFF) as usize];
            s += 1;
        }
        i += 1;
    }
    t
}

/// CRC-32C of `data`.
#[must_use]
pub(crate) fn crc32c(data: &[u8]) -> u32 {
    let mut c = !0u32;
    let mut chunks = data.chunks_exact(8);
    for b in &mut chunks {
        let lo = c ^ u32::from_le_bytes([b[0], b[1], b[2], b[3]]);
        c = TABLES[7][(lo & 0xFF) as usize]
            ^ TABLES[6][((lo >> 8) & 0xFF) as usize]
            ^ TABLES[5][((lo >> 16) & 0xFF) as usize]
            ^ TABLES[4][(lo >> 24) as usize]
            ^ TABLES[3][b[4] as usize]
            ^ TABLES[2][b[5] as usize]
            ^ TABLES[1][b[6] as usize]
            ^ TABLES[0][b[7] as usize];
    }
    for &b in chunks.remainder() {
        c = (c >> 8) ^ TABLES[0][((c ^ u32::from(b)) & 0xFF) as usize];
    }
    !c
}

/// The CRC-32C in `x-goog-hash` header values (`crc32c=<base64 of 4
/// big-endian bytes>`, comma-separated beside `md5=`, possibly across
/// several headers). `None` when absent or malformed.
pub(crate) fn from_goog_hash<'a>(values: impl IntoIterator<Item = &'a str>) -> Option<u32> {
    values
        .into_iter()
        .flat_map(|v| v.split(','))
        .find_map(|part| part.trim().strip_prefix("crc32c="))
        .and_then(decode_b64_u32)
}

/// Standard base64 of exactly four bytes (`XXXXXX==`), big-endian.
fn decode_b64_u32(s: &str) -> Option<u32> {
    fn val(c: u8) -> Option<u32> {
        Some(u32::from(match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        }))
    }
    let b = s.as_bytes();
    if b.len() != 8 || &b[6..] != b"==" {
        return None;
    }
    let mut acc = 0u64;
    for &c in &b[..6] {
        acc = (acc << 6) | u64::from(val(c)?);
    }
    // 36 bits carry 32 of data; the 4 padding bits must be zero.
    if acc & 0xF != 0 {
        return None;
    }
    u32::try_from(acc >> 4).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_vectors() {
        // RFC 3720 B.4 and the common check value.
        assert_eq!(crc32c(b""), 0);
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(&[0u8; 32]), 0x8A91_36AA);
        assert_eq!(crc32c(&[0xFFu8; 32]), 0x62A8_AB43);
        let inc: Vec<u8> = (0..32).collect();
        assert_eq!(crc32c(&inc), 0x46DD_794E);
        // Every length around the 8-byte stride agrees with the bytewise form.
        let data: Vec<u8> = (0..100u32).map(|i| (i * 37 + 11) as u8).collect();
        for n in 0..data.len() {
            let mut c = !0u32;
            for &b in &data[..n] {
                c = (c >> 8) ^ TABLES[0][((c ^ u32::from(b)) & 0xFF) as usize];
            }
            assert_eq!(crc32c(&data[..n]), !c, "length {n}");
        }
    }

    #[test]
    fn goog_hash_headers() {
        // Base64 of four big-endian bytes, alone or beside an md5.
        assert_eq!(from_goog_hash(["crc32c=AAAAAA=="]), Some(0));
        assert_eq!(from_goog_hash(["crc32c=/////w=="]), Some(u32::MAX));
        assert_eq!(
            from_goog_hash(["md5=dFQlK1iq8G2WnXM1DcaksA==", "crc32c=LUd2Nw=="]),
            Some(0x2D47_7637)
        );
        assert_eq!(
            from_goog_hash(["crc32c=LUd2Nw==,md5=dFQlK1iq8G2WnXM1DcaksA=="]),
            Some(0x2D47_7637)
        );
        for bad in ["", "md5=x", "crc32c=", "crc32c=LUd2Nw", "crc32c=LUd2N!==", "crc32c=LUd2Nx=="] {
            assert_eq!(from_goog_hash([bad]), None, "{bad}");
        }
    }
}
