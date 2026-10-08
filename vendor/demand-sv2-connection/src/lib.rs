mod buffer_cleanup;
pub mod noise_connection_tokio;

pub use buffer_sv2::Slice;
pub use noise_sv2::{Initiator, Responder};
use roles_logic_sv2::framing_sv2::{framing::Sv2Frame, Error as FramingError};

pub type StandardSv2Frame<Message> = Sv2Frame<Message, Slice>;
pub type StandardEitherFrame<Message> = Frame<Message>;

#[derive(Debug)]
pub enum HandshakeRole {
    Initiator(Box<Initiator>),
    Responder(Box<Responder>),
}

#[derive(Debug, Clone)]
pub enum Frame<Message> {
    Sv2(StandardSv2Frame<Message>),
}

impl<Message> From<StandardSv2Frame<Message>> for Frame<Message> {
    fn from(frame: StandardSv2Frame<Message>) -> Self {
        Self::Sv2(frame)
    }
}

impl<Message> TryFrom<Frame<Message>> for StandardSv2Frame<Message> {
    type Error = FramingError;

    fn try_from(frame: Frame<Message>) -> Result<Self, Self::Error> {
        match frame {
            Frame::Sv2(frame) => Ok(frame),
        }
    }
}

#[derive(Debug)]
pub enum Error {
    CodecError(codec_sv2::Error),
    SendError,
    SocketClosed,
    Io(std::io::Error),
}

impl From<codec_sv2::Error> for Error {
    fn from(error: codec_sv2::Error) -> Self {
        Self::CodecError(error)
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}
