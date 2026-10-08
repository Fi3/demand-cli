use alloc::vec::Vec;
use core::convert::TryInto;

#[derive(derive_codec_sv2::Encodable, derive_codec_sv2::Decodable, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct Activate {
    pub request_id: u32,
}

#[derive(derive_codec_sv2::Encodable, derive_codec_sv2::Decodable, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct ActivateSuccess {
    pub request_id: u32,
}
