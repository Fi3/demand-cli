use std::{
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use crate::{config::Configuration, shared::utils::AbortOnDrop};
use codec_sv2::{HandshakeRole, Responder, StandardEitherFrame, StandardSv2Frame};
use demand_share_accounting_ext::parser::PoolExtMessages;
use demand_sv2_connection::noise_connection_tokio::Connection;
use key_utils::{Secp256k1PublicKey, Secp256k1SecretKey};
use roles_logic_sv2::{
    common_messages_sv2::{Protocol, SetupConnectionSuccess},
    parsers::{CommonMessages, Mining},
};
use tokio::{
    net::TcpListener,
    sync::mpsc::{channel, Receiver, Sender},
};
use tracing::{error, info, warn};

type Message = PoolExtMessages<'static>;
type StdFrame = StandardSv2Frame<Message>;
type EitherFrame = StandardEitherFrame<Message>;

const DEFAULT_CERT_VALIDITY: Duration = Duration::from_secs(3600);

pub fn start_listen_for_sv2_downstream(
    downstreams: Sender<(Sender<Mining<'static>>, Receiver<Mining<'static>>, IpAddr)>,
) -> Option<AbortOnDrop> {
    let listen_addr = Configuration::sv2_listening_addr();
    let auth_secret = Configuration::sv2_auth_secret();
    let (listen_addr, auth_secret) = match (listen_addr, auth_secret) {
        (Some(addr), Some(secret)) => (addr, secret),
        _ => {
            info!("SV2 ingress disabled: missing sv2_listening_addr or sv2_auth_secret");
            return None;
        }
    };

    let secret: Secp256k1SecretKey = match auth_secret.parse() {
        Ok(secret) => secret,
        Err(e) => {
            error!("Invalid SV2 auth secret: {}", e);
            return None;
        }
    };
    let public: Secp256k1PublicKey = Secp256k1PublicKey::from(secret);
    let public_key = public.into_bytes();
    let private_key = secret.into_bytes();

    let handle = tokio::spawn(async move {
        let listen_addr: SocketAddr = match listen_addr.parse() {
            Ok(addr) => addr,
            Err(e) => {
                error!("Invalid SV2 listen address '{}': {}", listen_addr, e);
                return;
            }
        };

        let listener = match TcpListener::bind(listen_addr).await {
            Ok(listener) => listener,
            Err(e) => {
                error!("Failed to bind SV2 listener on {}: {}", listen_addr, e);
                return;
            }
        };
        info!("Listening for SV2 downstream connections on {}", listen_addr);

        loop {
            let (stream, peer_addr) = match listener.accept().await {
                Ok((stream, addr)) => (stream, addr),
                Err(e) => {
                    warn!("SV2 listener accept error: {}", e);
                    continue;
                }
            };

            let downstreams = downstreams.clone();
            let public_key = public_key;
            let private_key = private_key;
            tokio::spawn(async move {
                if let Err(e) =
                    handle_sv2_downstream(stream, peer_addr, downstreams, public_key, private_key)
                        .await
                {
                    warn!("SV2 downstream {} disconnected: {:?}", peer_addr, e);
                }
            });
        }
    });

    Some(handle.into())
}

async fn handle_sv2_downstream(
    stream: tokio::net::TcpStream,
    peer_addr: SocketAddr,
    downstreams: Sender<(Sender<Mining<'static>>, Receiver<Mining<'static>>, IpAddr)>,
    public_key: [u8; 32],
    private_key: [u8; 32],
) -> Result<(), ()> {
    info!("SV2 downstream connecting from {}", peer_addr);

    let responder =
        Responder::from_authority_kp(&public_key, &private_key, DEFAULT_CERT_VALIDITY)
            .map_err(|_| ())?;
    let role = HandshakeRole::Responder(responder);
    let (mut receiver, mut sender, _recv_abort, _send_abort) =
        Connection::new(stream, role).await.map_err(|_| ())?;

    handle_setup_connection(&mut receiver, &mut sender, peer_addr).await?;

    let (send_to_translator, recv_from_down) = channel(10);
    let (send_to_down, recv_from_translator) = channel(10);
    downstreams
        .send((send_to_down, recv_from_down, peer_addr.ip()))
        .await
        .map_err(|_| ())?;

    let recv_task = tokio::spawn(relay_from_downstream(
        receiver,
        send_to_translator,
        peer_addr,
    ));
    let send_task = tokio::spawn(relay_to_downstream(
        recv_from_translator,
        sender,
        peer_addr,
    ));

    let _ = tokio::join!(recv_task, send_task);
    Ok(())
}

async fn handle_setup_connection(
    receiver: &mut Receiver<EitherFrame>,
    sender: &mut Sender<EitherFrame>,
    peer_addr: SocketAddr,
) -> Result<(), ()> {
    loop {
        let msg = receiver.recv().await.ok_or(())?;
        let mut msg: StdFrame = match msg.try_into() {
            Ok(msg) => msg,
            Err(_) => {
                warn!(
                    "SV2 downstream {} sent non-SV2 frame during setup",
                    peer_addr
                );
                continue;
            }
        };
        let header = match msg.get_header() {
            Some(header) => header,
            None => {
                warn!("SV2 downstream {} sent frame without header", peer_addr);
                continue;
            }
        };
        let message_type = header.msg_type();
        let extension = header.ext_type();
        let payload = msg.payload();
        let message: Result<PoolExtMessages<'_>, _> = (extension, message_type, payload).try_into();
        match message {
            Ok(PoolExtMessages::Common(CommonMessages::SetupConnection(setup))) => {
                if setup.protocol != Protocol::MiningProtocol {
                    warn!(
                        "SV2 downstream {} requested non-mining protocol",
                        peer_addr
                    );
                }
                let success = SetupConnectionSuccess {
                    flags: setup.flags,
                    used_version: 2,
                };
                let response = PoolExtMessages::Common(CommonMessages::SetupConnectionSuccess(
                    success,
                ));
                let frame: StdFrame = response.try_into().map_err(|_| ())?;
                sender.send(frame.into()).await.map_err(|_| ())?;
                info!("SV2 setup connection completed for {}", peer_addr);
                return Ok(());
            }
            Ok(other) => {
                warn!(
                    "SV2 downstream {} sent unexpected message during setup: {:?}",
                    peer_addr, other
                );
            }
            Err(e) => {
                warn!(
                    "SV2 downstream {} sent invalid setup message: {:?}",
                    peer_addr, e
                );
            }
        }
    }
}

async fn relay_from_downstream(
    mut receiver: Receiver<EitherFrame>,
    send_to_translator: Sender<Mining<'static>>,
    peer_addr: SocketAddr,
) {
    while let Some(msg) = receiver.recv().await {
        let mut msg: StdFrame = match msg.try_into() {
            Ok(msg) => msg,
            Err(_) => {
                warn!(
                    "SV2 downstream {} sent non-SV2 frame after setup",
                    peer_addr
                );
                continue;
            }
        };
        let header = match msg.get_header() {
            Some(header) => header,
            None => {
                warn!("SV2 downstream {} sent frame without header", peer_addr);
                continue;
            }
        };
        let message_type = header.msg_type();
        let extension = header.ext_type();
        let payload = msg.payload();
        let message: Result<PoolExtMessages<'_>, _> = (extension, message_type, payload).try_into();
        match message {
            Ok(PoolExtMessages::Mining(mining)) => {
                if send_to_translator.send(mining.into_static()).await.is_err() {
                    warn!(
                        "SV2 downstream {}: translator channel closed",
                        peer_addr
                    );
                    break;
                }
            }
            Ok(PoolExtMessages::Common(common)) => {
                warn!(
                    "SV2 downstream {} sent unexpected common message: {:?}",
                    peer_addr, common
                );
            }
            Ok(other) => {
                warn!(
                    "SV2 downstream {} sent unsupported message: {:?}",
                    peer_addr, other
                );
            }
            Err(e) => {
                warn!(
                    "SV2 downstream {} sent invalid message: {:?}",
                    peer_addr, e
                );
            }
        }
    }
}

async fn relay_to_downstream(
    mut recv_from_translator: Receiver<Mining<'static>>,
    sender: Sender<EitherFrame>,
    peer_addr: SocketAddr,
) {
    while let Some(message) = recv_from_translator.recv().await {
        let message = PoolExtMessages::Mining(message);
        let frame: StdFrame = match message.try_into() {
            Ok(frame) => frame,
            Err(_) => {
                error!(
                    "SV2 downstream {}: failed to encode mining message",
                    peer_addr
                );
                continue;
            }
        };
        if sender.send(frame.into()).await.is_err() {
            warn!(
                "SV2 downstream {}: connection closed while sending",
                peer_addr
            );
            break;
        }
    }
}
