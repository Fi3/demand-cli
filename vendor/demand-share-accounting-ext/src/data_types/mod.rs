use alloc::vec::Vec;
use core::convert::TryInto;
use roles_logic_sv2::binary_sv2;
mod phash;
mod share;
mod slice;

pub use phash::PHash;
pub use share::Share;
pub use slice::Slice;

#[derive(derive_codec_sv2::Encodable, derive_codec_sv2::Decodable, Clone, Debug, PartialEq, Eq)]
#[already_sized]
#[repr(C)]
pub struct Hash256 {
    first: u64,
    second: u64,
    third: u64,
    forth: u64,
}

impl From<[u8; 32]> for Hash256 {
    fn from(data: [u8; 32]) -> Self {
        Self {
            first: u64::from_le_bytes(data[0..8].try_into().unwrap()),
            second: u64::from_le_bytes(data[8..16].try_into().unwrap()),
            third: u64::from_le_bytes(data[16..24].try_into().unwrap()),
            forth: u64::from_le_bytes(data[24..32].try_into().unwrap()),
        }
    }
}

impl From<Hash256> for [u8; 32] {
    fn from(data: Hash256) -> Self {
        let first = data.first.to_le_bytes();
        let second = data.second.to_le_bytes();
        let third = data.third.to_le_bytes();
        let forth = data.forth.to_le_bytes();
        [
            first[0], first[1], first[2], first[3], first[4], first[5], first[6], first[7],
            second[0], second[1], second[2], second[3], second[4], second[5], second[6], second[7],
            third[0], third[1], third[2], third[3], third[4], third[5], third[6], third[7],
            forth[0], forth[1], forth[2], forth[3], forth[4], forth[5], forth[6], forth[7],
        ]
    }
}

impl binary_sv2::GetSize for Hash256 {
    fn get_size(&self) -> usize {
        32
    }
}

impl binary_sv2::IntoStatic for PHash {
    type Static = Self;
    fn into_static(self) -> Self {
        self
    }
}

impl binary_sv2::IntoStatic for Slice {
    type Static = Self;
    fn into_static(self) -> Self {
        self
    }
}

impl binary_sv2::IntoStatic for Share<'_> {
    type Static = Share<'static>;
    fn into_static(self) -> Self::Static {
        self.into_static()
    }
}
