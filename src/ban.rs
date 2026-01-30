use lazy_static::lazy_static;
use std::{
    collections::HashMap,
    net::IpAddr,
    sync::Mutex,
    time::{Duration, Instant},
};
use tracing::info;

pub const DEFAULT_BAN_DURATION: Duration = Duration::from_secs(60);

lazy_static! {
    static ref BANNED_IPS: Mutex<HashMap<IpAddr, Instant>> = Mutex::new(HashMap::new());
}

pub fn ban_ip(ip: IpAddr, duration: Duration) {
    let expires_at = Instant::now() + duration;
    if let Ok(mut bans) = BANNED_IPS.lock() {
        bans.insert(ip, expires_at);
    }
    info!("Banned downstream ip {} for {}s", ip, duration.as_secs());
}

pub fn is_banned(ip: &IpAddr) -> bool {
    let now = Instant::now();
    if let Ok(mut bans) = BANNED_IPS.lock() {
        if let Some(expires_at) = bans.get(ip).copied() {
            if now < expires_at {
                return true;
            }
            bans.remove(ip);
        }
    }
    false
}
