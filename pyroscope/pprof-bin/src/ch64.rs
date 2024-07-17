pub fn read_uint64_le(bytes: &[u8]) -> u64 {
    let mut res: u64 = 0;
    for i in 0..8 {
        res |= (bytes[i] as u64) << (i * 8);
    }
    res
}

const K_MUL: u64 = 0x9ddfea08eb382d69;

pub fn hash_128_to_64(l: u64, h: u64) -> u64 {
    let mut a = (l ^ h).wrapping_mul(K_MUL);
    a ^= a >> 47;
    let mut b = (h ^ a).wrapping_mul(K_MUL);
    b ^= b >> 47;
    b = b.wrapping_mul(K_MUL);
    b
}

const K0: u64 = 0xc3a5c85c97cb3127;
const K2: u64 = 0x9ae16a3b2f90404f;
const K1: u64 = 0xb492b66fbe98f273;
const K3: u64 = 0xc949d7c7509e6557;
fn ch16(u: u64, v: u64) -> u64 {
    hash_128_to_64(u, v)
}

fn rot64(val: u64, shift: usize) -> u64 {
    if shift == 0 {
        return val;
    }
    return (val >> shift) | val << (64 - shift);
}

fn shift_mix(val: u64) -> u64 {
    return val ^ (val >> 47);
}

fn hash16(u: u64, v: u64) -> u64 {
    hash_128_to_64(u, v)
}

fn fetch32(p: &[u8]) -> u32 {
    let mut res: u32 = 0;
    for i in 0..4 {
        res |= (p[i] as u32) << (i * 8);
    }
    res
}

fn ch33to64(s: &[u8], length: usize) -> u64 {
    let mut z = read_uint64_le(&s[24..]);
    let mut a =
        read_uint64_le(&s) + (length as u64 + read_uint64_le(&s[length - 16..])).wrapping_mul(K0);
    let mut b = rot64(a + z, 52);
    let mut c = rot64(a, 37);
    a += read_uint64_le(&s[8..]);
    c += rot64(a, 7);
    a += read_uint64_le(&s[16..]);
    let vf = a + z;
    let vs = b + rot64(a, 31) + c;

    a = read_uint64_le(&s[16..]) + read_uint64_le(&s[length - 32..]);
    z = read_uint64_le(&s[length - 8..]);
    b = rot64(a + z, 52);
    c = rot64(a, 37);
    a += read_uint64_le(&s[length - 24..]);
    c += rot64(a, 7);
    a += read_uint64_le(&s[length - 16..]);

    let wf = a + z;
    let ws = b + rot64(a, 31) + c;
    let r = shift_mix((vf + ws).wrapping_mul(K2) + (wf + vs).wrapping_mul(K0));
    return shift_mix(r.wrapping_mul(K0) + vs).wrapping_mul(K2);
}

fn ch17to32(s: &[u8], length: usize) -> u64 {
    let a = read_uint64_le(s).wrapping_mul(K1);
    let b = read_uint64_le(&s[8..]);
    let c = read_uint64_le(&s[length - 8..]).wrapping_mul(K2);
    let d = read_uint64_le(&s[length - 16..]).wrapping_mul(K0);
    return hash16(
        rot64(a - b, 43) + rot64(c, 30) + d,
        a + rot64(b ^ K3, 20) - c + (length as u64),
    );
}

fn ch0to16(s: &[u8], length: usize) -> u64 {
    if length > 8 {
        let a = read_uint64_le(s);
        let b = read_uint64_le(&s[length - 8..]);
        return ch16(a, rot64(b + (length as u64), length)) ^ b;
    }
    if length >= 4 {
        let a = fetch32(s) as u64;
        return ch16((length as u64) + (a << 3), fetch32(&s[length - 4..]) as u64);
    }
    if length > 0 {
        let a = s[0];
        let b = s[length >> 1];
        let c = s[length - 1];
        let y = (a as u32) + ((b as u32) << 8);
        let z = (length as u32) + ((c as u32) << 2);
        return shift_mix((y as u64).wrapping_mul(K2) ^ (z as u64).wrapping_mul(K3))
            .wrapping_mul(K2);
    }
    return K2;
}

fn weak_hash32_seeds(w: u64, x: u64, y: u64, z: u64, _a: u64, _b: u64) -> (u64, u64) {
    let mut a = _a + w;
    let mut b = rot64(_b + a + z, 21);
    let c = a;
    a += x;
    a += y;
    b += rot64(a, 44);
    return (a + z, b + c);
}

// Return a 16-byte hash for s[0] ... s[31], a, and b. Quick and dirty.
fn weak_hash32_seeds_byte(s: &[u8], a: u64, b: u64) -> (u64, u64) {
    _ = s[31];
    return weak_hash32_seeds(
        read_uint64_le(&s[0..0 + 8]),
        read_uint64_le(&s[8..8 + 8]),
        read_uint64_le(&s[16..16 + 8]),
        read_uint64_le(&s[24..24 + 8]),
        a,
        b,
    );
}

fn nearest_multiple_64(b: &[u8]) -> usize {
    return ((b.len()) - 1) & !63;
}

// CH64 returns ClickHouse version of Hash64.
pub fn city_hash_64(s: &[u8]) -> u64 {
    let length = s.len();
    if length <= 16 {
        return ch0to16(s, length);
    }
    if length <= 32 {
        return ch17to32(s, length);
    }
    if length <= 64 {
        return ch33to64(s, length);
    }

    let x = read_uint64_le(s);
    let y = read_uint64_le(&s[length - 16..]) ^ K1;
    let mut z = read_uint64_le(&s[length - 56..]) ^ K0;

    let mut v = weak_hash32_seeds_byte(&s[length - 64..], length as u64, y);
    let mut w = weak_hash32_seeds_byte(&s[length - 32..], (length as u64).wrapping_mul(K1), K0);
    z += shift_mix(v.1).wrapping_mul(K1);
    let mut x = rot64(z + x, 39).wrapping_mul(K1);
    let mut y = rot64(y, 33).wrapping_mul(K1);
    // Decrease len to the nearest multiple of 64, and operate on 64-byte chunks.
    let mut _s = &s[..nearest_multiple_64(s)];
    while _s.len() > 0 {
        x = rot64(x + y + v.0 + read_uint64_le(&s[16..]), 37).wrapping_mul(K1);
        y = rot64(y + v.1 + read_uint64_le(&s[48..]), 42).wrapping_mul(K1);
        x ^= w.1;
        y ^= v.0;

        z = rot64(z ^ w.0, 33);
        v = weak_hash32_seeds_byte(s, v.1.wrapping_mul(K1), x + w.0);
        w = weak_hash32_seeds_byte(&s[32..], z + w.1, y);
        (z, x) = (x, z);
        _s = &_s[64..];
    }
    return ch16(
        ch16(v.0, w.0) + shift_mix(y).wrapping_mul(K1) + z,
        ch16(v.1, w.1) + x,
    );
}
