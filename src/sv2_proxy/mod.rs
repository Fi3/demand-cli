mod error;
mod upstream;

use bitcoin::Address;
use codec_sv2::{HandshakeRole, StandardEitherFrame, StandardSv2Frame};
use demand_sv2_connection::noise_connection_tokio::Connection;
use error::Error;

use key_utils::{Secp256k1PublicKey, Secp256k1SecretKey};
use noise_sv2::Responder;
use roles_logic_sv2::{
    channel_logic::channel_factory::OnNewShare,
    common_messages_sv2::SetupConnectionSuccess,
    mining_sv2::{OpenStandardMiningChannel, SubmitSharesStandard, SubmitSharesSuccess},
    parsers::{CommonMessages, Mining, MiningDeviceMessages, PoolMessages},
    utils::Mutex,
};
use tracing::{error, info, warn};
use upstream::upstream::OpenChannelReq;

use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::mpsc::{self, channel},
    time,
};

use sv1_api::server_to_client;
use tokio::sync::broadcast;

use crate::{
    proxy_state::{ProxyState, TranslatorState},
    shared::utils::AbortOnDrop,
};
use tokio::sync::mpsc::{Receiver as TReceiver, Sender as TSender};

mod task_manager;
use task_manager::TaskManager;
pub type Message = MiningDeviceMessages<'static>;
pub type StdFrame = StandardSv2Frame<Message>;
pub type EitherFrame = StandardEitherFrame<Message>;

pub async fn start(
    pool_connection: TSender<(
        TSender<Mining<'static>>,
        TReceiver<Mining<'static>>,
        Option<Address>,
    )>,
) -> Result<AbortOnDrop, Error<'static>> {
    let (send_to_up, up_recv_from_here) = channel(crate::TRANSLATOR_BUFFER_SIZE);
    let (up_send_to_here, recv_from_up) = channel(crate::TRANSLATOR_BUFFER_SIZE);
    pool_connection
        .send((up_send_to_here, up_recv_from_here, None))
        .await
        .map_err(|_| {
            error!("Internal Error: Failed to send channels to the pool");
            Error::Unrecoverable // Propagate error to that caller. There, we will restart Proxy
        })?;
    let upstream = upstream::Upstream::new(crate::MIN_EXTRANONCE_SIZE - 1, send_to_up).await?;

    let (new_hom_downstream_sender, new_hom_downstream_receiver) = mpsc::channel(100);
    let (submit_sender, submit_receiver) = mpsc::channel(100);
    let abortable =
        upstream::Upstream::start(upstream, recv_from_up, new_hom_downstream_receiver, submit_receiver).await;
    tokio::task::spawn(async move {
        listen_for_downstream(new_hom_downstream_sender,submit_sender).await;
    });
    abortable
}

pub async fn listen_for_downstream(
    add_hom_downstream: mpsc::Sender<(OpenChannelReq, mpsc::Sender<Mining<'static>>)>,
    send_sumbit_up: TSender<(
        SubmitSharesStandard,
        tokio::sync::oneshot::Sender<OnNewShare>,
    )>,
) {
    let auth_pub_k: Secp256k1PublicKey = crate::SELF_AUTH_PUB_KEY
        .parse()
        .expect("Invalid public key");
    let auth_prv_k: Secp256k1SecretKey = crate::SELF_AUTH_PRIV_KEY
        .parse()
        .expect("Invalid private key");
    let auth_pub_k_as_bytes = auth_pub_k.into_bytes();
    let auth_prv_k_as_bytes = auth_prv_k.into_bytes();

    let downstream_addr: SocketAddr = crate::SV2_DOWN_LISTEN_ADDR
        .parse()
        .expect("Invalid listen address");
    let downstream_listener = TcpListener::bind(downstream_addr)
        .await
        .expect("impossible to bind downstream");
    let mut id = 1;
    info!("Listening on {}", downstream_listener.local_addr().unwrap());
    while let Ok((stream, addr)) = downstream_listener.accept().await {
        let is_private = match addr.ip() {
            std::net::IpAddr::V4(a) => a.is_private(),
            std::net::IpAddr::V6(_) => false,
        };
        if is_private {
            continue;
        };
        info!("Try to connect {:#?}", addr);

        let responder = Responder::from_authority_kp(
            &auth_pub_k_as_bytes,
            &auth_prv_k_as_bytes,
            std::time::Duration::from_secs(super::CERT_VALIDITY_SEC),
        )
        .expect("invalid key pair");
        match time::timeout(Duration::from_secs(5), async {
            Connection::new::<'static, MiningDeviceMessages<'static>>(
                stream,
                HandshakeRole::Responder(responder),
            )
            .await
        })
        .await
        {
            // Handshake completed within 5 seconds
            Ok(Ok((mut recv_from_client, send_to_client, _, _))) => {
                let (msg_from_up_tx, msg_from_up_rcv) = channel(100);
                //let (msgs_from_down_rcv, msg_from_down_tx) = channel(100);
                let open_channel =
                    Downstream::start(&mut recv_from_client, &send_to_client, id).await;
                id += 1;
                add_hom_downstream
                    .send((open_channel, msg_from_up_tx))
                    .await
                    .unwrap();

                Downstream::up_to_down(msg_from_up_rcv, send_to_client.clone());
                Downstream::down_to_up(recv_from_client, send_sumbit_up.clone(), send_to_client);
            }
            // Handshake returned an error
            Ok(Err(e)) => {
                warn!("{:#?}, invalid handshake: {:?}", addr, e);
            }
            // Timeout elapsed before handshake completed
            Err(_) => {
                warn!("Handshake timed out for {:#?}", addr);
            }
        }
    }
}

type Frame_ = StandardEitherFrame<MiningDeviceMessages<'static>>;

pub struct Downstream {}

impl Downstream {
    pub async fn start(
        recv_from_client: &mut TReceiver<Frame_>,
        send_to_client: &TSender<Frame_>,
        id: u32,
    ) -> OpenChannelReq {
        match recv_from_client.recv().await {
            Some(codec_sv2::Frame::Sv2(mut msg)) => {
                let m_type = msg.get_header().unwrap().msg_type();
                let payload = msg.payload();
                let msg: CommonMessages = (m_type, payload).try_into().unwrap();
                match msg {
                    CommonMessages::SetupConnection(msg) => {
                        info!("SetupConnection");
                        let protocol: u8 = msg.protocol as u8;
                        let is_hom = msg.requires_standard_job();
                        if protocol == 0_u8 && is_hom {
                            let msg = SetupConnectionSuccess {
                                used_version: 2,
                                flags: 0b0000_0000_0000_0000_0000_0000_0000_0110,
                            };
                            let msg: StdFrame = MiningDeviceMessages::Common(
                                CommonMessages::SetupConnectionSuccess(msg),
                            )
                            .try_into()
                            .unwrap();
                            let msg: Frame_ = EitherFrame::Sv2(msg);
                            send_to_client.send(msg).await.unwrap();
                        } else {
                            panic!()
                        }
                    }
                    _ => panic!(),
                }
            }
            _ => panic!(),
        };
        match recv_from_client.recv().await {
            Some(codec_sv2::Frame::Sv2(mut msg)) => {
                let m_type = msg.get_header().unwrap().msg_type();
                let payload = msg.payload();
                let msg: Mining = (m_type, payload).try_into().unwrap();
                match msg {
                    Mining::OpenStandardMiningChannel(msg) => {
                        info!("OpenStandardMiningChannel");
                        let request_id: u32 = (&msg.request_id).into();
                        let downstream_hash_rate = msg.nominal_hash_rate;
                        let is_header_only = true;
                        OpenChannelReq {
                            request_id,
                            downstream_hash_rate,
                            is_header_only,
                            id,
                        }
                    }
                    _ => panic!(),
                }
            }
            _ => panic!(),
        }
    }
    pub fn up_to_down(
        mut recv_from_up: TReceiver<Mining<'static>>,
        send_to_client: TSender<Frame_>,
    ) {
        tokio::task::spawn(async move {
            while let Some(msg) = recv_from_up.recv().await {
                let msg: StdFrame = MiningDeviceMessages::Mining(msg).try_into().unwrap();
                let msg: Frame_ = EitherFrame::Sv2(msg);
                send_to_client.send(msg).await.unwrap();
            }
        });
    }

    pub fn down_to_up(
        mut recv_from_client: TReceiver<Frame_>,
        send_to_up: TSender<(
            SubmitSharesStandard,
            tokio::sync::oneshot::Sender<OnNewShare>,
        )>,
        send_to_client: TSender<Frame_>,
    ) {
        tokio::task::spawn(async move {
        while let Some(frame) = recv_from_client.recv().await {
            
            match frame {
                EitherFrame::Sv2(mut msg) => {
                    let m_type = msg.get_header().unwrap().msg_type();
                    let payload = msg.payload();
                    let msg: Mining = (m_type, payload).try_into().unwrap();
                    match msg {
                        Mining::SubmitSharesStandard(msg) => {
                            info!("SubmitSharesStandard");
                            let success = SubmitSharesSuccess {
                                channel_id: msg.channel_id,
                                last_sequence_number: msg.sequence_number,
                                new_submits_accepted_count: 1,
                                new_shares_sum: 1,
                            };
                            let (share_response_tx, share_response_rx) =
                                tokio::sync::oneshot::channel();
                            send_to_up.send((msg, share_response_tx)).await.unwrap();
                            match share_response_rx.await {
                                Ok(OnNewShare::SendErrorDownstream(msg)) => {
                                    let msg: StdFrame = MiningDeviceMessages::Mining(
                                        Mining::SubmitSharesError(msg),
                                    )
                                    .try_into()
                                    .unwrap();
                                    let msg: Frame_ = EitherFrame::Sv2(msg);
                                    send_to_client.send(msg).await.unwrap();
                                }
                                Ok(_) => {
                                    let msg: StdFrame = MiningDeviceMessages::Mining(
                                        Mining::SubmitSharesSuccess(success),
                                    )
                                    .try_into()
                                    .unwrap();
                                    let msg: Frame_ = EitherFrame::Sv2(msg);
                                    send_to_client.send(msg).await.unwrap();
                                }
                                Err(_e) => {
                                    panic!()
                                }
                            }
                        }
                        _ => panic!(),
                    }
                }
                _ => panic!(),
            }
            }
        });
    }
}

pub struct Ids {
    last: u64,
}

impl Ids {
    pub fn new() -> Self {
        Self { last: 0 }
    }
    pub fn next(&mut self) -> u64 {
        self.last += 1;
        if self.last == 0 {
            panic!()
        }
        self.last - 1
    }
}
