use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use key_utils::Secp256k1PublicKey;
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    Mutex, Notify,
};
use tracing::{debug, info, warn};

use crate::{
    api::stats::StatsSender,
    ban::{ban_ip, DEFAULT_BAN_DURATION},
    config::Configuration,
    minin_pool_connection,
    pending_extranonce,
    shared::utils::AbortOnDrop,
    share_accounter, translator,
};

#[derive(Clone)]
pub enum PoolAddressSource {
    Static(Vec<SocketAddr>),
    Dynamic,
}

struct TokenInstance {
    downstream_tx: Sender<(Sender<String>, Receiver<String>, SocketAddr)>,
    abort_handles: Vec<AbortOnDrop>,
}

impl TokenInstance {
    fn is_finished(&self) -> bool {
        self.abort_handles.iter().all(|handle| handle.is_finished())
    }
}

enum InstanceEntry {
    Ready(Arc<TokenInstance>),
    Creating(Arc<Notify>),
}

pub struct TokenRouter {
    instances: Mutex<HashMap<String, InstanceEntry>>,
    pool_addresses: PoolAddressSource,
    pool_cache: Mutex<Option<Vec<SocketAddr>>>,
    auth_pub_k: Secp256k1PublicKey,
    stats_sender: StatsSender,
    signature: String,
}

impl TokenRouter {
    pub fn new(
        pool_addresses: PoolAddressSource,
        auth_pub_k: Secp256k1PublicKey,
        stats_sender: StatsSender,
        signature: String,
    ) -> Arc<Self> {
        Arc::new(Self {
            instances: Mutex::new(HashMap::new()),
            pool_addresses,
            pool_cache: Mutex::new(None),
            auth_pub_k,
            stats_sender,
            signature,
        })
    }

    pub fn start(
        self: Arc<Self>,
        mut downstreams: Receiver<(Sender<String>, Receiver<String>, SocketAddr)>,
    ) -> AbortOnDrop {
        let handle = tokio::spawn(async move {
            while let Some((send, recv, ip)) = downstreams.recv().await {
                let router = self.clone();
                tokio::spawn(async move {
                    router.handle_connection(send, recv, ip).await;
                });
            }
        });
        handle.into()
    }

    async fn handle_connection(
        &self,
        send_to_down: Sender<String>,
        mut recv_from_down: Receiver<String>,
        addr: SocketAddr,
    ) {
        let mut buffered = Vec::new();
        let token = match self
            .wait_for_token(&mut recv_from_down, &mut buffered, send_to_down.clone(), addr)
            .await
        {
            Ok(token) => token,
            Err(TokenError::Missing) => {
                warn!("Downstream {} missing token, closing", addr.ip());
                ban_ip(addr.ip(), DEFAULT_BAN_DURATION);
                return;
            }
            Err(TokenError::Timeout) => {
                warn!(
                    "Downstream {} did not authorize in time, closing",
                    addr.ip()
                );
                return;
            }
            Err(TokenError::Disconnected) => return,
        };

        let instance = match self.get_or_create_instance(&token).await {
            Ok(instance) => instance,
            Err(e) => {
                warn!(
                    "Failed to create upstream for downstream {}: {}. Banning for 60s",
                    addr.ip(),
                    e
                );
                ban_ip(addr.ip(), DEFAULT_BAN_DURATION);
                return;
            }
        };

        let (tx_forward, rx_forward) = channel(10);
        if instance
            .downstream_tx
            .send((send_to_down, rx_forward, addr))
            .await
            .is_err()
        {
            warn!(
                "Failed to attach downstream {} to token upstream",
                addr.ip()
            );
            return;
        }

        for msg in buffered {
            if tx_forward.send(msg).await.is_err() {
                return;
            }
        }

        while let Some(msg) = recv_from_down.recv().await {
            if tx_forward.send(msg).await.is_err() {
                return;
            }
        }
    }

    async fn wait_for_token(
        &self,
        recv: &mut Receiver<String>,
        buffered: &mut Vec<String>,
        send_to_down: Sender<String>,
        addr: SocketAddr,
    ) -> Result<String, TokenError> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
            if timeout.is_zero() {
                return Err(TokenError::Timeout);
            }
            let msg = match tokio::time::timeout(timeout, recv.recv()).await {
                Ok(Some(msg)) => msg,
                Ok(None) => return Err(TokenError::Disconnected),
                Err(_) => return Err(TokenError::Timeout),
            };

            if let Some((response, extranonce1, user_agent)) = try_dummy_subscribe_response(&msg) {
                pending_extranonce::mark(addr, extranonce1, user_agent);
                let _ = send_to_down.send(response).await;
                continue;
            }

            if let Some(token) = parse_authorize_token(&msg) {
                if token.trim().is_empty() {
                    return Err(TokenError::Missing);
                }
                buffered.push(msg);
                return Ok(token);
            }
            buffered.push(msg);
        }
    }

    async fn get_or_create_instance(
        &self,
        token: &str,
    ) -> Result<Arc<TokenInstance>, String> {
        loop {
            let notify = {
                let mut instances = self.instances.lock().await;
                if let Some(InstanceEntry::Ready(instance)) = instances.get(token) {
                    if instance.is_finished() {
                        instances.remove(token);
                    } else {
                        return Ok(instance.clone());
                    }
                }

                if let Some(InstanceEntry::Creating(notify)) = instances.get(token) {
                    Some(notify.clone())
                } else {
                    let notify = Arc::new(Notify::new());
                    instances
                        .insert(token.to_string(), InstanceEntry::Creating(notify.clone()));
                    drop(instances);
                    let result = self.create_instance(token).await;
                    let mut instances = self.instances.lock().await;
                    match result {
                        Ok(instance) => {
                            instances
                                .insert(token.to_string(), InstanceEntry::Ready(instance.clone()));
                            notify.notify_waiters();
                            return Ok(instance);
                        }
                        Err(e) => {
                            instances.remove(token);
                            notify.notify_waiters();
                            return Err(e);
                        }
                    }
                }
            };

            if let Some(notify) = notify {
                notify.notified().await;
            }
        }
    }

    async fn create_instance(&self, token: &str) -> Result<Arc<TokenInstance>, String> {
        let pool_addresses = self.resolve_pool_addresses(token).await.ok_or_else(|| {
            "No pool addresses available for this token".to_string()
        })?;
        let pool_addr = pool_addresses[0];

        info!("Connecting upstream for token {}", token);
        let setup_msg = minin_pool_connection::get_mining_setup_connection_msg(token, true);
        let (send_to_pool, recv_from_pool, pool_abortable) =
            minin_pool_connection::connect_pool(
                pool_addr,
                self.auth_pub_k,
                Some(setup_msg),
                None,
            )
            .await
            .map_err(|e| format!("SV2 pool connection failed: {e}"))?;

        let (downs_tx, downs_rx) = channel(10);
        let (translator_up_tx, mut translator_up_rx) = channel(10);
        let translator_abortable = translator::start(
            downs_rx,
            translator_up_tx,
            self.stats_sender.clone(),
            self.signature.clone(),
        )
        .await
        .map_err(|e| format!("Failed to start translator: {e}"))?;

        let (to_translator, from_translator, _) = translator_up_rx
            .recv()
            .await
            .ok_or_else(|| "Translator failed before initialization".to_string())?;

        let share_accounter_abortable = share_accounter::start(
            from_translator,
            to_translator,
            recv_from_pool,
            send_to_pool,
        )
        .await
        .map_err(|e| format!("Failed to start share accounter: {e}"))?;

        Ok(Arc::new(TokenInstance {
            downstream_tx: downs_tx,
            abort_handles: vec![pool_abortable, translator_abortable, share_accounter_abortable],
        }))
    }

    async fn resolve_pool_addresses(&self, token: &str) -> Option<Vec<SocketAddr>> {
        match &self.pool_addresses {
            PoolAddressSource::Static(addrs) => {
                if addrs.is_empty() {
                    None
                } else {
                    Some(addrs.clone())
                }
            }
            PoolAddressSource::Dynamic => {
                if let Some(cached) = self.pool_cache.lock().await.clone() {
                    return Some(cached);
                }
                let addresses = Configuration::pool_address_for_token(token)
                    .await
                    .filter(|p| !p.is_empty());
                if let Some(ref addrs) = addresses {
                    *self.pool_cache.lock().await = Some(addrs.clone());
                }
                addresses
            }
        }
    }
}

#[derive(Debug)]
enum TokenError {
    Missing,
    Timeout,
    Disconnected,
}

fn parse_authorize_token(msg: &str) -> Option<String> {
    let parsed: Result<sv1_api::json_rpc::Message, _> = serde_json::from_str(msg);
    let parsed = parsed.ok()?;
    if let sv1_api::Message::StandardRequest(request) = parsed {
        if request.method != "mining.authorize" {
            return None;
        }
        let auth: sv1_api::client_to_server::Authorize = request.try_into().ok()?;
        debug!("Received authorize for {}", auth.name);
        if !auth.password.trim().is_empty() {
            Some(auth.password)
        } else {
            Some(auth.name)
        }
    } else {
        None
    }
}

fn try_dummy_subscribe_response(msg: &str) -> Option<(String, Vec<u8>, String)> {
    let parsed: Result<sv1_api::json_rpc::Message, _> = serde_json::from_str(msg);
    let parsed = parsed.ok()?;
    let sv1_api::Message::StandardRequest(request) = parsed else {
        return None;
    };
    if request.method != "mining.subscribe" {
        return None;
    }
    let subscribe: sv1_api::client_to_server::Subscribe<'static> = request.try_into().ok()?;
    let subscriptions = vec![
        (
            "mining.set_difficulty".to_string(),
            "ae6812eb4cd7735a302a8a9dd95cf71f".to_string(),
        ),
        (
            "mining.notify".to_string(),
            "ae6812eb4cd7735a302a8a9dd95cf71f".to_string(),
        ),
    ];
    let extranonce1 = Vec::new();
    let extranonce1_msg = sv1_api::utils::Extranonce::try_from(extranonce1.clone()).ok()?;
    let user_agent = subscribe.agent_signature.clone();
    let response = subscribe.respond(
        subscriptions,
        extranonce1_msg,
        crate::MIN_EXTRANONCE2_SIZE as usize,
    );
    let msg = sv1_api::json_rpc::Message::from(response);
    let json = serde_json::to_string(&msg).ok().map(|json| format!("{}\n", json))?;
    Some((json, extranonce1, user_agent))
}
