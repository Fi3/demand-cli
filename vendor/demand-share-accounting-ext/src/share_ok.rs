use alloc::vec::Vec;
use core::convert::TryInto;

#[derive(derive_codec_sv2::Encodable, derive_codec_sv2::Decodable, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct ShareOk {
    pub ref_job_id: u64,
    pub share_index: u32,
}
