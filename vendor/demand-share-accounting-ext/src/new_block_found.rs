use alloc::vec::Vec;
use binary_sv2::U256;
use core::convert::TryInto;
use roles_logic_sv2::binary_sv2;

#[derive(derive_codec_sv2::Encodable, derive_codec_sv2::Decodable, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct NewBlockFound<'decoder> {
    pub block_hash: U256<'decoder>,
}
