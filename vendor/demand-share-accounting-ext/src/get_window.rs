use alloc::vec::Vec;
use binary_sv2::{encodable::EncodableField, GetSize, Seq064K, U256};
use core::convert::TryInto;
use roles_logic_sv2::binary_sv2;

use crate::{PHash, Slice};

#[derive(derive_codec_sv2::Encodable, derive_codec_sv2::Decodable, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct GetWindow<'decoder> {
    pub block_hash: U256<'decoder>,
}

#[derive(derive_codec_sv2::Decodable, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct GetWindowSuccess<'decoder> {
    pub slices: Seq064K<'decoder, Slice>,
    pub phashes: Seq064K<'decoder, PHash>,
}
impl<'decoder> From<GetWindowSuccess<'decoder>> for EncodableField<'decoder> {
    fn from(v: GetWindowSuccess<'decoder>) -> Self {
        let fields = vec![
            crate::Slice::seq_into_encodable_field(v.slices),
            PHash::seq_into_encodable_field(v.phashes),
        ];
        Self::Struct(fields)
    }
}
impl GetSize for GetWindowSuccess<'_> {
    fn get_size(&self) -> usize {
        let mut size = 0;
        size += self.slices.get_size();
        size += self.phashes.get_size();
        size
    }
}

#[derive(derive_codec_sv2::Encodable, derive_codec_sv2::Decodable, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct GetWindowBusy {
    pub retry_in_seconds: u64,
}
