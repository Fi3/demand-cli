use std::{collections::HashSet, net::IpAddr, sync::Arc};

use binary_sv2::Str0255;
use roles_logic_sv2::{
    mining_sv2::{OpenMiningChannelError, SetCustomMiningJobError},
    parsers::Mining,
    utils::Mutex,
};
use tokio::sync::mpsc::{Receiver as TReceiver, Sender as TSender};
use tracing::{error, info, warn};

use crate::{config::Configuration, shared::utils::AbortOnDrop, translator::error::Error};

use super::proxy::Bridge;

pub async fn accept_connections(
    bridge: Arc<Mutex<Bridge>>,
    mut downstreams: TReceiver<(TSender<Mining<'static>>, TReceiver<Mining<'static>>, IpAddr)>,
) -> Result<AbortOnDrop, Error<'static>> {
    let handle = tokio::spawn(async move {
        while let Some((sender, receiver, addr)) = downstreams.recv().await {
            info!("SV2 downstream connected: {}", addr);
            let bridge = bridge.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_downstream(bridge, sender, receiver, addr).await {
                    error!("SV2 downstream {} error: {}", addr, e);
                }
            });
        }
    });
    Ok(handle.into())
}

async fn handle_downstream(
    bridge: Arc<Mutex<Bridge>>,
    sender: TSender<Mining<'static>>,
    mut receiver: TReceiver<Mining<'static>>,
    addr: IpAddr,
) -> Result<(), Error<'static>> {
    let mut channel_ids: HashSet<u32> = HashSet::new();

    while let Some(message) = receiver.recv().await {
        match message {
            Mining::OpenExtendedMiningChannel(m) => {
                let requested_min = m.min_extranonce_size;
                let effective_min = if requested_min == 0 {
                    let fallback = Configuration::min_extranonce2_size();
                    warn!(
                        "SV2 downstream {} requested min_extranonce_size=0, using fallback {}",
                        addr, fallback
                    );
                    fallback
                } else {
                    requested_min
                };
                info!(
                    "SV2 downstream {} OpenExtendedMiningChannel request_id={} nominal_hash_rate={} min_extranonce_size={}",
                    addr, m.request_id, m.nominal_hash_rate, effective_min
                );
                let request_id = m.request_id;
                let open_result = bridge
                    .safe_lock(|b| {
                        b.open_sv2_channel(request_id, m.nominal_hash_rate, effective_min)
                    })
                    .map_err(|_| Error::BridgeMutexPoisoned)?;

                match open_result {
                    Ok((responses, channel_id)) => {
                        if let Some(channel_id) = channel_id {
                            bridge
                                .safe_lock(|b| {
                                    b.register_sv2_downstream(channel_id, sender.clone());
                                })
                                .map_err(|_| Error::BridgeMutexPoisoned)?;
                            channel_ids.insert(channel_id);
                        }
                        for response in responses {
                            if let Mining::OpenExtendedMiningChannelSuccess(success) = &response {
                                info!(
                                    "SV2 downstream {} OpenExtendedMiningChannelSuccess request_id={} channel_id={} extranonce_size={} extranonce_prefix_len={}",
                                    addr,
                                    success.request_id,
                                    success.channel_id,
                                    success.extranonce_size,
                                    success.extranonce_prefix.len()
                                );
                            }
                            if let Mining::OpenMiningChannelError(err) = &response {
                                warn!(
                                    "SV2 downstream {} OpenMiningChannelError request_id={} error_code={}",
                                    addr,
                                    err.request_id,
                                    std::str::from_utf8(err.error_code.as_ref())
                                        .unwrap_or("unknown")
                                );
                            }
                            if sender.send(response).await.is_err() {
                                return Err(Error::AsyncChannelError);
                            }
                        }
                    }
                    Err(e) => {
                        error!("SV2 downstream {} channel open error: {}", addr, e);
                        let err = OpenMiningChannelError::new_unknown_user(request_id);
                        let _ = sender.send(Mining::OpenMiningChannelError(err)).await;
                    }
                }
            }
            Mining::SubmitSharesExtended(share) => {
                match Bridge::handle_sv2_submit_shares(bridge.clone(), share).await {
                    Ok(Some(response)) => {
                        if sender.send(response).await.is_err() {
                            return Err(Error::AsyncChannelError);
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        error!("SV2 downstream {} submit error: {}", addr, e);
                    }
                }
            }
            Mining::SetCustomMiningJob(m) => {
                let error_code: Str0255<'static> = "invalid-mining-job-token"
                    .to_string()
                    .try_into()
                    .expect("invalid error code");
                let err = SetCustomMiningJobError {
                    channel_id: m.channel_id,
                    request_id: m.request_id,
                    error_code,
                };
                let _ = sender.send(Mining::SetCustomMiningJobError(err)).await;
            }
            Mining::CloseChannel(m) => {
                if channel_ids.remove(&m.channel_id) {
                    let _ = bridge.safe_lock(|b| b.unregister_sv2_downstream(m.channel_id));
                }
            }
            other => {
                warn!("SV2 downstream {} sent unsupported message: {:?}", addr, other);
            }
        }
    }

    for channel_id in channel_ids {
        let _ = bridge.safe_lock(|b| b.unregister_sv2_downstream(channel_id));
    }
    Ok(())
}
