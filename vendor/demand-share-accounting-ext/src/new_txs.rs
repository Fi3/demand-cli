use alloc::vec::Vec;
use binary_sv2::{Seq064K, B016M};
use core::convert::TryInto;
use roles_logic_sv2::binary_sv2;

#[derive(derive_codec_sv2::Encodable, derive_codec_sv2::Decodable, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct NewTxs<'decoder> {
    pub transactions: Seq064K<'decoder, B016M<'decoder>>,
}
