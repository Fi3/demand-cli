use crate::{Error, Frame, HandshakeRole, StandardEitherFrame};
use binary_sv2::{GetSize, Serialize};
use codec_sv2::{
    Decrypted, Handshake, NoiseEncoder, Transport, TransportDecryptState, TransportEncryptState,
};
use noise_sv2::{
    Initiator, Responder, ELLSWIFT_ENCODING_SIZE, INITIATOR_EXPECTED_HANDSHAKE_MESSAGE_SIZE,
};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpStream,
    },
    sync::mpsc::{channel, Receiver, Sender},
    task::{self, AbortHandle},
};
use tracing::{debug, error};

pub struct Connection;

type ConnectionParts<Message> = (
    Receiver<StandardEitherFrame<Message>>,
    Sender<StandardEitherFrame<Message>>,
    AbortHandle,
    AbortHandle,
);

impl Connection {
    #[allow(clippy::new_ret_no_self)]
    pub async fn new<Message: Serialize + GetSize + Send + 'static>(
        mut stream: TcpStream,
        role: HandshakeRole,
    ) -> Result<ConnectionParts<Message>, Error> {
        let address = stream.peer_addr()?;
        let transport = handshake(&mut stream, role).await?;
        let (encrypt, decrypt) = transport.split();
        let (reader, writer) = stream.into_split();
        let (sender_incoming, receiver_incoming) = channel(10);
        let (sender_outgoing, receiver_outgoing) = channel(10);
        let recv_task = task::spawn(async move {
            if let Err(error) = receive(reader, decrypt, sender_incoming).await {
                error!(%address, ?error, "Noise stream reader stopped");
            }
        });
        let send_task = task::spawn(async move {
            if let Err(error) = send(writer, encrypt, receiver_outgoing).await {
                error!(%address, ?error, "Noise stream writer stopped");
            }
        });
        debug!(%address, "Noise handshake complete");
        Ok((
            receiver_incoming,
            sender_outgoing,
            recv_task.abort_handle(),
            send_task.abort_handle(),
        ))
    }
}

async fn handshake(stream: &mut TcpStream, role: HandshakeRole) -> Result<Transport, Error> {
    match role {
        HandshakeRole::Initiator(initiator) => {
            let (first, state) = Handshake::initiator(initiator).step_0()?;
            stream.write_all(first.payload()).await?;
            let mut response = [0; INITIATOR_EXPECTED_HANDSHAKE_MESSAGE_SIZE];
            stream.read_exact(&mut response).await?;
            Ok(state.step_2(response)?)
        }
        HandshakeRole::Responder(responder) => {
            let mut first = [0; ELLSWIFT_ENCODING_SIZE];
            stream.read_exact(&mut first).await?;
            let (response, transport) = Handshake::responder(responder).step_1(first)?;
            stream.write_all(response.payload()).await?;
            Ok(transport)
        }
    }
}

async fn receive<Message: Serialize + GetSize>(
    mut reader: OwnedReadHalf,
    mut state: TransportDecryptState,
    sender: Sender<StandardEitherFrame<Message>>,
) -> Result<(), Error> {
    let mut decoder = crate::buffer_cleanup::Decoder::new();
    loop {
        match decoder.next_transport_frame(state)? {
            Decrypted::Frame(frame, next) => {
                state = next;
                sender
                    .send(Frame::Sv2(frame.into()))
                    .await
                    .map_err(|_| Error::SendError)?;
            }
            Decrypted::Incomplete(missing, next) => {
                state = next;
                if missing != 0 {
                    tokio::select! {
                        result = reader.read_exact(decoder.writable()) => { result?; }
                        _ = sender.closed() => return Ok(()),
                    }
                }
            }
        }
    }
}

async fn send<Message: Serialize + GetSize>(
    mut writer: OwnedWriteHalf,
    mut state: TransportEncryptState,
    mut receiver: Receiver<StandardEitherFrame<Message>>,
) -> Result<(), Error> {
    let mut encoder = NoiseEncoder::new();
    while let Some(Frame::Sv2(frame)) = receiver.recv().await {
        let encrypted = encoder.encode_transport(frame, &mut state)?;
        writer.write_all(encrypted.as_ref()).await?;
    }
    writer.shutdown().await?;
    Ok(())
}

pub async fn listen(
    address: &str,
    authority_public_key: [u8; 32],
    authority_private_key: [u8; 32],
    cert_validity: Duration,
    sender: Sender<(TcpStream, HandshakeRole)>,
) {
    let listener = TcpListener::bind(address).await.unwrap();
    loop {
        if let Ok((stream, _)) = listener.accept().await {
            let responder = Responder::from_authority_kp(
                &authority_public_key,
                &authority_private_key,
                cert_validity,
            )
            .unwrap();
            if sender
                .send((stream, HandshakeRole::Responder(responder)))
                .await
                .is_err()
            {
                break;
            }
        }
    }
}

pub async fn connect(
    address: &str,
    authority_public_key: [u8; 32],
) -> Result<(TcpStream, HandshakeRole), ()> {
    let stream = TcpStream::connect(address).await.map_err(|_| ())?;
    let initiator = Initiator::from_raw_k(authority_public_key).map_err(|_| ())?;
    Ok((stream, HandshakeRole::Initiator(initiator)))
}
