use alloc::vec::Vec;
use binary_sv2::{Seq064K, B016M, U256};
use core::convert::TryInto;
use roles_logic_sv2::binary_sv2;

#[derive(derive_codec_sv2::Encodable, derive_codec_sv2::Decodable, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct GetTransationsInJob {
    pub request_id: u32,
    pub job_id: u64,
}

#[derive(derive_codec_sv2::Encodable, derive_codec_sv2::Decodable, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct GetTransationsInJobSuccess<'decoder> {
    pub window_req_id: u32,
    pub request_id: u32,
    pub coinbase_id: U256<'decoder>,
    // pub tx_short_hash_nonce: u64,
    tx_list: Seq064K<'decoder, B016M<'decoder>>,
    // pub tx_short_hash_list: Seq064K<'decoder, ShortTxId<'decoder>>,
    // pub tx_hash_list_hash: U256<'decoder>,
}

#[derive(derive_codec_sv2::Encodable, derive_codec_sv2::Decodable, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct IdentifyTransations {
    pub request_id: u32,
}

#[derive(derive_codec_sv2::Encodable, derive_codec_sv2::Decodable, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct IdentifyTransationsSuccess<'decoder> {
    pub request_id: u32,
    pub tx_data_hashes: Seq064K<'decoder, U256<'decoder>>,
}

#[derive(derive_codec_sv2::Encodable, derive_codec_sv2::Decodable, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct ProvideMissinTransactions<'decoder> {
    pub request_id: u32,
    pub unknown_tx_position_list: Seq064K<'decoder, u16>,
}

#[derive(derive_codec_sv2::Encodable, derive_codec_sv2::Decodable, Clone, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct ProvideMissinTransactionsSuccess<'decoder> {
    pub request_id: u32,
    pub transaction_list: Seq064K<'decoder, B016M<'decoder>>,
}
