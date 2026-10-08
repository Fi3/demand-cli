use binary_sv2::{
    decodable::FieldMarker, encodable::EncodableField, Error, GetMarker, GetSize, Seq064K,
    Sv2DataType, B032, B064K,
};
use roles_logic_sv2::binary_sv2;

use alloc::vec::Vec;
use core::convert::TryInto;

#[derive(derive_codec_sv2::Encodable, derive_codec_sv2::Decodable, Clone, Debug, PartialEq, Eq)]
#[already_sized]
#[repr(C)]
pub struct Share<'decoder> {
    pub nonce: u32,
    pub ntime: u32,
    pub version: u32,
    pub extranonce: B032<'decoder>,
    pub job_id: u64,
    pub reference_job_id: u64,
    pub share_index: u32,
    pub merkle_path: B064K<'decoder>,
}

impl<'a> Share<'a> {
    pub fn seq_into_encodable_field(v: Seq064K<'a, Share<'a>>) -> EncodableField<'a> {
        v.into()
    }
}

impl GetMarker for Share<'_> {
    fn get_marker() -> FieldMarker {
        let markers = vec![
            u32::get_marker(),
            u32::get_marker(),
            u32::get_marker(),
            B032::get_marker(),
            u64::get_marker(),
            u64::get_marker(),
            u32::get_marker(),
            B064K::get_marker(),
        ];
        FieldMarker::Struct(markers)
    }
}

impl GetSize for Share<'_> {
    fn get_size(&self) -> usize {
        self.nonce.get_size()
            + self.ntime.get_size()
            + self.version.get_size()
            + self.extranonce.get_size()
            + self.job_id.get_size()
            + self.reference_job_id.get_size()
            + self.share_index.get_size()
            + self.merkle_path.get_size()
    }
}

impl binary_sv2::SizeHint for Share<'_> {
    fn size_hint(data: &[u8], offset: usize) -> Result<usize, binary_sv2::Error> {
        binary_sv2::SizeHint::size_hint_(&Self::get_marker(), data, offset)
    }
    fn size_hint_(&self, _: &[u8], _: usize) -> Result<usize, binary_sv2::Error> {
        Ok(self.get_size())
    }
}

impl TryInto<binary_sv2::decodable::FieldMarker> for Share<'_> {
    type Error = ();
    fn try_into(self) -> Result<binary_sv2::decodable::FieldMarker, Self::Error> {
        Ok(Share::get_marker())
    }
}
impl<'d> Sv2DataType<'d> for Share<'d> {
    fn from_bytes_(data: &'d mut [u8]) -> Result<Self, Error> {
        binary_sv2::from_bytes(data)
    }

    fn to_slice(&'d self, dst: &mut [u8]) -> Result<usize, Error> {
        binary_sv2::to_writer(self.clone(), dst)?;
        Ok(self.get_size())
    }
}
