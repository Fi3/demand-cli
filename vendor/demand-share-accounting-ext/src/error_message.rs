use alloc::vec::Vec;
use binary_sv2::Str0255;
use core::convert::TryInto;
use roles_logic_sv2::binary_sv2;

#[derive(derive_codec_sv2::Encodable, derive_codec_sv2::Decodable, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct ErrorMessage<'decoder> {
    pub message: Str0255<'decoder>,
}
