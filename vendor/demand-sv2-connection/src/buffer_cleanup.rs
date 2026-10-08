use codec_sv2::NoiseDecoder;
use std::{
    ops::{Deref, DerefMut},
    time::Duration,
};

pub(crate) struct Decoder(Option<NoiseDecoder>);

impl Decoder {
    pub(crate) fn new() -> Self {
        Self(Some(NoiseDecoder::new()))
    }
}

impl Deref for Decoder {
    type Target = NoiseDecoder;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref().unwrap()
    }
}

impl DerefMut for Decoder {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0.as_mut().unwrap()
    }
}

impl Drop for Decoder {
    fn drop(&mut self) {
        let decoder = self.0.take().unwrap();
        if decoder.droppable() {
            return;
        }
        // The pool waits for all received frames before it can release memory.
        // Keep that wait off the executor, including when the reader is aborted.
        tokio::task::spawn_blocking(move || {
            while !decoder.droppable() {
                std::thread::sleep(Duration::from_millis(1));
            }
            drop(decoder);
        });
    }
}
