pub type Uint64 = ::std::os::raw::c_ulong;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct Threefish1024Key {
    pub key: [Uint64; 17usize],
}

extern "C" {
    pub fn threefish1024_set_key(key: *mut Threefish1024Key, words: *const u64);
}

extern "C" {
    pub fn threefish1024_clear_key(key: *mut Threefish1024Key);
}

extern "C" {
    pub fn threefish1024_encrypt(
        key: *const Threefish1024Key,
        tweak: *const u64,
        input: *const u64,
        output: *mut u64,
    );
}

extern "C" {
    pub fn threefish1024_decrypt(
        key: *const Threefish1024Key,
        tweak: *const u64,
        input: *const u64,
        output: *mut u64,
    );
}
