use bitcoin::secp256k1::{Keypair, Secp256k1, SecretKey};
use demand_share_accounting_ext::{
    parser::{PoolExtMessages, ShareAccountingMessages},
    GetSharesSuccess, GetWindowSuccess, Hash256, PHash, Share, ShareOk, Slice,
};
use demand_sv2_connection::{
    noise_connection_tokio::Connection, HandshakeRole, Initiator, Responder, StandardEitherFrame,
    StandardSv2Frame,
};
use roles_logic_sv2::{
    binary_sv2::{self, GetSize, Seq0255, Seq064K, Sv2Option},
    mining_sv2::{NewExtendedMiningJob, SubmitSharesExtended},
    parsers::{IsSv2Message, PoolMessages, TemplateDistribution},
    template_distribution_sv2::RequestTransactionDataSuccess,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::{Receiver, Sender},
    task::AbortHandle,
};

use std::{
    hint::black_box,
    time::{Duration, Instant},
};

fn mining_job() -> NewExtendedMiningJob<'static> {
    NewExtendedMiningJob {
        channel_id: 1,
        job_id: 1,
        min_ntime: Sv2Option::new(Some(1)),
        version: 0x20000000,
        version_rolling_allowed: true,
        merkle_path: Seq0255::new(vec![[1; 32].into(); 12]).unwrap(),
        coinbase_tx_prefix: vec![1; 100].try_into().unwrap(),
        coinbase_tx_suffix: vec![2; 500].try_into().unwrap(),
    }
}

fn channel_factory() -> roles_logic_sv2::channel_logic::channel_factory::PoolChannelFactory {
    use roles_logic_sv2::{
        channel_logic::channel_factory::{ExtendedChannelKind, PoolChannelFactory},
        job_creator::JobsCreators,
        mining_sv2::ExtendedExtranonce,
        utils::{GroupId, Mutex},
    };
    use std::sync::Arc;

    PoolChannelFactory::new(
        Arc::new(Mutex::new(GroupId::new())),
        ExtendedExtranonce::new(0..0, 0..2, 2..32),
        JobsCreators::new(32),
        60.0,
        ExtendedChannelKind::Pool,
        Vec::new(),
        Vec::new(),
    )
    .unwrap()
}

fn channel_opening_time(count: u32) -> Duration {
    let mut samples = [Duration::ZERO; 3];
    for sample in &mut samples {
        let mut factory = channel_factory();
        let start = Instant::now();
        for request_id in 0..count {
            black_box(
                factory
                    .new_extended_channel(request_id, 100_000.0, 28)
                    .unwrap(),
            );
        }
        *sample = start.elapsed();
        assert_eq!(factory.get_extended_channels_ids().len(), count as usize);
    }
    samples.sort();
    samples[1]
}

#[test]
fn miner_channel_opening_scales_with_connection_count() {
    // Ten times as many connections must remain well below quadratic (100x) work.
    // Three samples and a 30x limit leave room for allocator and scheduler variation.
    let small = channel_opening_time(1_000);
    let large = channel_opening_time(10_000);
    eprintln!("channel opening: 1,000={small:?}, 10,000={large:?}");
    assert!(
        large <= small * 30,
        "channel opening grew faster than the linear budget: 1,000={small:?}, 10,000={large:?}"
    );
}

fn restore_channel(
    factory: &mut roles_logic_sv2::channel_logic::channel_factory::PoolChannelFactory,
    channel_id: u32,
    prefix: &[u8],
) -> Result<Vec<roles_logic_sv2::parsers::Mining<'static>>, roles_logic_sv2::Error> {
    factory.replicate_pool_extended_channel(
        0,
        [0xff; 32].into(),
        prefix.to_vec().try_into().unwrap(),
        channel_id,
        30,
        Vec::new(),
    )
}

#[test]
fn channel_prefix_index_tracks_restored_generated_and_closed_channels() {
    use roles_logic_sv2::{parsers::Mining, Error};

    let mut factory = channel_factory();
    restore_channel(&mut factory, 50_000, &[0, 1]).unwrap();
    let messages = factory.new_extended_channel(0, 100_000.0, 28).unwrap();
    let Mining::OpenExtendedMiningChannelSuccess(opened) = &messages[0] else {
        panic!("expected an open channel");
    };
    assert_eq!(opened.extranonce_prefix.as_ref(), &[0, 2]);
    assert!(matches!(
        restore_channel(&mut factory, 50_001, &[0, 2]),
        Err(Error::ExtranoncePrefixAlreadyInUse)
    ));
    factory.close_channel(opened.channel_id);
    restore_channel(&mut factory, 50_001, &[0, 2]).unwrap();
    factory.close_channel(50_000);
    restore_channel(&mut factory, 50_002, &[0, 1]).unwrap();
}

#[test]
fn channel_prefix_index_retains_duplicate_replicas_and_releases_replacements() {
    use roles_logic_sv2::Error;

    let mut factory = channel_factory();
    for channel_id in [50_000, 50_001] {
        factory
            .replicate_upstream_extended_channel_only_jd(
                [0xff; 32].into(),
                vec![0, 1].try_into().unwrap(),
                channel_id,
                30,
            )
            .unwrap();
    }
    factory.close_channel(50_000);
    assert!(matches!(
        restore_channel(&mut factory, 50_002, &[0, 1]),
        Err(Error::ExtranoncePrefixAlreadyInUse)
    ));
    factory
        .replicate_upstream_extended_channel_only_jd(
            [0xff; 32].into(),
            vec![0, 2].try_into().unwrap(),
            50_001,
            30,
        )
        .unwrap();
    restore_channel(&mut factory, 50_002, &[0, 1]).unwrap();
    factory.close_channel(50_001);
    restore_channel(&mut factory, 50_003, &[0, 2]).unwrap();
    factory.close_channel(50_001);
}

fn extended_share() -> SubmitSharesExtended<'static> {
    SubmitSharesExtended {
        channel_id: 7,
        sequence_number: 8,
        job_id: 9,
        nonce: 10,
        ntime: 11,
        version: 0x20000000,
        extranonce: vec![12; 28].try_into().unwrap(),
    }
}

#[test]
fn mining_message_size_does_not_allocate() {
    let job = mining_job();
    let allocations = crate::test_allocator::count_allocations(|| {
        assert_eq!(black_box(&job).get_size(), 1_007);
    });
    assert_eq!(allocations, 0, "message size must not copy the message");
}

type Message = PoolExtMessages<'static>;
type ConnectionParts = (
    Receiver<StandardEitherFrame<Message>>,
    Sender<StandardEitherFrame<Message>>,
    AbortHandle,
    AbortHandle,
);

fn authority_public_key(seed: u8) -> [u8; 32] {
    let key = SecretKey::from_slice(&[seed; 32]).unwrap();
    let keypair = Keypair::from_secret_key(&Secp256k1::new(), &key);
    keypair.x_only_public_key().0.serialize()
}

async fn socket_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let server = listener.accept().await.unwrap().0;
    (client, server)
}

async fn connection_pair() -> (ConnectionParts, ConnectionParts) {
    let (client, server) = socket_pair().await;
    let public = authority_public_key(7);
    let initiator = Initiator::from_raw_k(public).unwrap();
    let responder =
        Responder::from_authority_kp(&public, &[7; 32], Duration::from_secs(60)).unwrap();
    let (client, server) = tokio::join!(
        Connection::new(client, HandshakeRole::Initiator(initiator)),
        Connection::new(server, HandshakeRole::Responder(responder)),
    );
    (client.unwrap(), server.unwrap())
}

#[tokio::test]
async fn noise_connection_rejects_a_wrong_authority() {
    let (client, server) = socket_pair().await;
    let initiator = Initiator::from_raw_k(authority_public_key(8)).unwrap();
    let responder =
        Responder::from_authority_kp(&authority_public_key(7), &[7; 32], Duration::from_secs(60))
            .unwrap();
    let (client, server) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(
            Connection::new::<Message>(client, HandshakeRole::Initiator(initiator)),
            Connection::new::<Message>(server, HandshakeRole::Responder(responder)),
        )
    })
    .await
    .unwrap();
    stop_connection(server.unwrap());
    assert!(client.is_err(), "the pool authority must be verified");
}

#[tokio::test]
async fn cancelled_noise_handshake_closes_the_socket() {
    use tokio::io::AsyncReadExt;

    let (client, mut server) = socket_pair().await;
    let initiator = Initiator::from_raw_k(authority_public_key(7)).unwrap();
    let result = tokio::time::timeout(
        Duration::from_millis(50),
        Connection::new::<Message>(client, HandshakeRole::Initiator(initiator)),
    )
    .await;
    assert!(result.is_err(), "the silent peer must cause a timeout");
    let mut handshake = Vec::new();
    tokio::time::timeout(Duration::from_secs(1), server.read_to_end(&mut handshake))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(handshake.len(), noise_sv2::ELLSWIFT_ENCODING_SIZE);
}

fn share_ok(index: u32) -> Message {
    PoolExtMessages::ShareAccountingMessages(ShareAccountingMessages::ShareOk(ShareOk {
        ref_job_id: 42,
        share_index: index,
    }))
}

async fn send_message(sender: &Sender<StandardEitherFrame<Message>>, message: Message) {
    let frame: StandardSv2Frame<Message> = message.try_into().unwrap();
    sender.send(frame.into()).await.unwrap();
}

async fn receive_message(receiver: &mut Receiver<StandardEitherFrame<Message>>) -> Message {
    let frame = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    let mut frame: StandardSv2Frame<Message> = frame.try_into().unwrap();
    let header = frame.get_header().unwrap();
    PoolExtMessages::try_from((header.ext_type(), header.msg_type(), frame.payload()))
        .unwrap()
        .into_static()
}

fn stop_connection(parts: ConnectionParts) {
    parts.2.abort();
    parts.3.abort();
}

#[tokio::test]
async fn concurrent_noise_connections_keep_separate_state() {
    let exchanges = (0..8).map(|index| async move {
        let (mut client, mut server) = connection_pair().await;
        for sequence in 0..4 {
            let index = index * 4 + sequence;
            send_message(&client.1, share_ok(index)).await;
            send_message(&server.1, share_ok(index + 100)).await;
            let upstream = receive_message(&mut server.0).await;
            let downstream = receive_message(&mut client.0).await;
            match (upstream, downstream) {
                (
                    PoolExtMessages::ShareAccountingMessages(ShareAccountingMessages::ShareOk(up)),
                    PoolExtMessages::ShareAccountingMessages(ShareAccountingMessages::ShareOk(
                        down,
                    )),
                ) => {
                    assert_eq!(up.share_index, index);
                    assert_eq!(down.share_index, index + 100);
                }
                _ => panic!("unexpected share accounting message"),
            }
        }
        stop_connection(client);
        stop_connection(server);
    });
    tokio::time::timeout(Duration::from_secs(5), futures::future::join_all(exchanges))
        .await
        .unwrap();
}

#[tokio::test]
async fn noise_connection_preserves_multi_chunk_transaction_data() {
    let (client, mut server) = connection_pair().await;
    let transaction = vec![0xab; 150_000];
    let message = RequestTransactionDataSuccess {
        template_id: 73,
        excess_data: vec![1, 2].try_into().unwrap(),
        transaction_list: Seq064K::new(vec![transaction.clone().try_into().unwrap()]).unwrap(),
    };
    send_message(
        &client.1,
        PoolExtMessages::TemplateDistribution(TemplateDistribution::RequestTransactionDataSuccess(
            message,
        )),
    )
    .await;
    match receive_message(&mut server.0).await {
        PoolExtMessages::TemplateDistribution(
            TemplateDistribution::RequestTransactionDataSuccess(message),
        ) => {
            assert_eq!(message.template_id, 73);
            assert_eq!(
                message.transaction_list.into_inner()[0].inner_as_ref(),
                transaction
            );
        }
        _ => panic!("unexpected transaction data message"),
    }
    stop_connection(client);
    stop_connection(server);
}

#[tokio::test]
async fn reader_abort_does_not_block_with_a_held_frame() {
    let (client, mut server) = connection_pair().await;
    send_message(&client.1, share_ok(1)).await;
    let frame = server.0.recv().await.unwrap();
    let release = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(300));
        drop(frame);
    });
    let start = Instant::now();
    server.2.abort();
    tokio::time::sleep(Duration::from_millis(10)).await;
    let elapsed = start.elapsed();
    stop_connection(client);
    stop_connection(server);
    release.join().unwrap();
    assert!(
        elapsed < Duration::from_millis(150),
        "reader cleanup blocked the executor for {elapsed:?}"
    );
}

#[test]
fn share_accounting_window_round_trips_fixed_hashes() {
    let original = GetWindowSuccess {
        slices: Seq064K::new(vec![Slice {
            number_of_shares: 7,
            difficulty: 8,
            fees: 9,
            root: Hash256::from([2; 32]),
            job_id: 10,
        }])
        .unwrap(),
        phashes: Seq064K::new(vec![PHash {
            phash: Hash256::from([3; 32]),
            index_start: 17,
        }])
        .unwrap(),
    };
    let mut bytes = binary_sv2::to_bytes(original.clone()).unwrap();
    let decoded: GetWindowSuccess = binary_sv2::from_bytes(&mut bytes).unwrap();
    assert_eq!(decoded, original);
}

#[test]
fn share_accounting_history_round_trips_variable_fields() {
    let shares = [1, 28]
        .into_iter()
        .map(|length| Share {
            nonce: 1,
            ntime: 2,
            version: 3,
            extranonce: vec![4; length].try_into().unwrap(),
            job_id: 5,
            reference_job_id: 6,
            share_index: 7,
            merkle_path: vec![8; length * 32].try_into().unwrap(),
        })
        .collect();
    let original = GetSharesSuccess {
        shares: Seq064K::new(shares).unwrap(),
    };
    let mut bytes = binary_sv2::to_bytes(original.clone()).unwrap();
    let decoded: GetSharesSuccess = binary_sv2::from_bytes(&mut bytes).unwrap();
    assert_eq!(decoded, original);
}

#[test]
fn mining_frame_matches_the_current_wire_size() {
    let message = extended_share();
    let frame: StandardSv2Frame<PoolMessages> = PoolMessages::Mining(
        roles_logic_sv2::parsers::Mining::SubmitSharesExtended(message.clone()),
    )
    .try_into()
    .unwrap();
    let mut bytes = vec![0; frame.encoded_length()];
    frame.serialize(&mut bytes).unwrap();
    assert_eq!(bytes.len(), 6 + 24 + 1 + 28);
    assert_eq!(&bytes[6..10], &7_u32.to_le_bytes());
    let decoded: SubmitSharesExtended = binary_sv2::from_bytes(&mut bytes[6..]).unwrap();
    assert_eq!(decoded, message);
}

#[test]
fn template_constraints_include_the_sigop_limit() {
    let constraints = roles_logic_sv2::template_distribution_sv2::CoinbaseOutputDataSize {
        coinbase_output_max_additional_size: 143,
    };
    let message = TemplateDistribution::CoinbaseOutputDataSize(constraints);
    let message_type = message.message_type();
    let bytes = binary_sv2::to_bytes(message).unwrap();
    assert_eq!(bytes.len(), 6);
    assert_eq!(&bytes[..4], &143_u32.to_le_bytes());
    assert_eq!(&bytes[4..], &2_860_u16.to_le_bytes());
    assert_eq!(message_type, 0x70);
}

#[test]
fn extended_channel_response_includes_the_group_identifier() {
    let message = roles_logic_sv2::mining_sv2::OpenExtendedMiningChannelSuccess {
        request_id: 7,
        channel_id: 1,
        target: [255; 32].into(),
        extranonce_size: 8,
        extranonce_prefix: vec![1, 2].try_into().unwrap(),
    };
    assert_eq!(message.get_size(), 49);
    let bytes = binary_sv2::to_bytes(message).unwrap();
    assert_eq!(bytes.len(), 49);
    assert_eq!(&bytes[45..], &0_u32.to_le_bytes());
}

#[test]
fn mining_job_encoding_keeps_the_allocation_budget() {
    let job = mining_job();
    let allocations = crate::test_allocator::count_allocations(|| {
        let bytes = binary_sv2::to_bytes(black_box(&job).clone()).unwrap();
        assert_eq!(bytes.len(), 1_007);
        black_box(bytes);
    });
    assert!(
        allocations <= 23,
        "old message encoding used 23 allocations; got {allocations}"
    );
}

#[test]
fn mining_job_encoding_uses_flat_fields() {
    use binary_sv2::encodable::EncodableField;

    let fields: EncodableField = mining_job().into();
    match &fields {
        EncodableField::Struct(fields) => assert!(
            fields
                .iter()
                .all(|field| matches!(field, EncodableField::Primitive(_))),
            "nested fields add allocation and traversal work to each mining job"
        ),
        EncodableField::Primitive(_) => panic!("a mining job has multiple fields"),
    }
}

#[test]
fn share_decoding_does_not_allocate_field_lists() {
    let original = extended_share();
    let mut bytes = binary_sv2::to_bytes(original.clone()).unwrap();
    let allocations = crate::test_allocator::count_allocations(|| {
        let decoded: SubmitSharesExtended = binary_sv2::from_bytes(black_box(&mut bytes)).unwrap();
        assert_eq!(decoded, original);
    });
    assert_eq!(
        allocations, 1,
        "only the owned extranonce needs an allocation"
    );
}

#[test]
fn share_decoding_rejects_incomplete_or_large_extranonce() {
    let mut bytes = binary_sv2::to_bytes(extended_share()).unwrap();
    for length in 0..bytes.len() {
        let mut incomplete = bytes[..length].to_vec();
        assert!(binary_sv2::from_bytes::<SubmitSharesExtended>(&mut incomplete).is_err());
    }
    bytes[24] = 33;
    bytes.extend([0; 5]);
    assert!(binary_sv2::from_bytes::<SubmitSharesExtended>(&mut bytes).is_err());
}
