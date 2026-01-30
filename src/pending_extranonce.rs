use lazy_static::lazy_static;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Mutex,
};

lazy_static! {
    static ref PENDING_EXTRANONCE_UPDATE: Mutex<HashMap<SocketAddr, PendingExtranonce>> =
        Mutex::new(HashMap::new());
}

pub struct PendingExtranonce {
    pub extranonce1: Vec<u8>,
    pub user_agent: String,
}

pub fn mark(addr: SocketAddr, extranonce1: Vec<u8>, user_agent: String) {
    if let Ok(mut pending) = PENDING_EXTRANONCE_UPDATE.lock() {
        pending.insert(
            addr,
            PendingExtranonce {
                extranonce1,
                user_agent,
            },
        );
    }
}

pub fn take(addr: &SocketAddr) -> Option<PendingExtranonce> {
    if let Ok(mut pending) = PENDING_EXTRANONCE_UPDATE.lock() {
        return pending.remove(addr);
    }
    None
}
