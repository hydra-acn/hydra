pub type ThreefishSize = u32;
pub const THREEFISH1024: ThreefishSize = 1024;

pub type Uint64 = ::std::os::raw::c_ulong;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct ThreefishKey {
    pub state_size: Uint64,
    pub key: [Uint64; 17usize],
    pub tweak: [Uint64; 3usize],
}

extern "C" {
    pub fn threefishSetKey(
        keyCtx: *mut ThreefishKey,
        stateSize: ThreefishSize,
        keyData: *mut u64,
        tweak: *mut u64,
    );
}

extern "C" {
    pub fn threefishEncrypt1024(keyCtx: *mut ThreefishKey, input: *mut u64, output: *mut u64);
}
