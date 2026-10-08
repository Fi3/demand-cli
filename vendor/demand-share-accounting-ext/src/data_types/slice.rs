use binary_sv2::{
    decodable::FieldMarker, encodable::EncodableField, Error, Fixed, GetMarker, GetSize, Seq064K,
    Sv2DataType,
};
use roles_logic_sv2::binary_sv2;

use super::Hash256;
use alloc::vec::Vec;
use binary_sv2::U256;
use core::convert::TryInto;

#[derive(derive_codec_sv2::Encodable, derive_codec_sv2::Decodable, Clone, Debug, PartialEq, Eq)]
#[already_sized]
#[repr(C)]
pub struct Slice {
    // How many shares are in the slice
    pub number_of_shares: u32,
    // Below 2 values ara cached for performance
    // sum of all the difficulties of the shares that compose the slice
    pub difficulty: u64,
    // fees of the ref job for this slice
    pub fees: u64,
    // merkle root of the tree composed by all the shares in the slice
    pub root: Hash256,
    // id of the ref job for this slice
    pub job_id: u64,
}

impl<'a> Slice {
    pub fn seq_into_encodable_field(v: Seq064K<'a, Slice>) -> EncodableField<'a> {
        v.into()
    }
}

impl TryInto<FieldMarker> for Slice {
    type Error = ();
    fn try_into(self) -> Result<FieldMarker, Self::Error> {
        Ok(Slice::get_marker())
    }
}

impl Fixed for Slice {
    const SIZE: usize = 60;
}

impl GetMarker for Slice {
    fn get_marker() -> FieldMarker {
        let markers = vec![
            u32::get_marker(),
            u64::get_marker(),
            u64::get_marker(),
            U256::get_marker(),
            u64::get_marker(),
        ];
        FieldMarker::Struct(markers)
    }
}

impl<'d> Sv2DataType<'d> for Slice {
    fn from_bytes_(data: &'d mut [u8]) -> Result<Self, Error> {
        binary_sv2::from_bytes(data)
    }

    fn to_slice(&'d self, dst: &mut [u8]) -> Result<usize, Error> {
        binary_sv2::to_writer(self.clone(), dst)?;
        Ok(self.get_size())
    }
}
