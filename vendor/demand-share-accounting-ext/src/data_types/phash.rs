use super::Hash256;
use binary_sv2::{
    decodable::FieldMarker, encodable::EncodableField, Error, Fixed, GetMarker, GetSize, Seq064K,
    Sv2DataType, B032,
};
use roles_logic_sv2::binary_sv2;

use alloc::vec::Vec;
use core::convert::TryInto;

#[derive(derive_codec_sv2::Encodable, derive_codec_sv2::Decodable, Clone, Debug, PartialEq, Eq)]
#[already_sized]
#[repr(C)]
pub struct PHash {
    pub phash: Hash256,
    pub index_start: u32,
}

impl Fixed for PHash {
    const SIZE: usize = 32 + 4;
}

impl PHash {
    pub fn seq_into_encodable_field(v: Seq064K<'_, PHash>) -> EncodableField<'_> {
        v.into()
    }
}

impl GetMarker for PHash {
    fn get_marker() -> FieldMarker {
        let markers = vec![B032::get_marker(), u32::get_marker()];
        FieldMarker::Struct(markers)
    }
}

impl TryInto<FieldMarker> for PHash {
    type Error = ();
    fn try_into(self) -> Result<FieldMarker, Self::Error> {
        Ok(PHash::get_marker())
    }
}
impl<'d> Sv2DataType<'d> for PHash {
    fn from_bytes_(data: &'d mut [u8]) -> Result<Self, Error> {
        binary_sv2::from_bytes(data)
    }

    fn to_slice(&'d self, dst: &mut [u8]) -> Result<usize, Error> {
        binary_sv2::to_writer(self.clone(), dst)?;
        Ok(self.get_size())
    }
}
