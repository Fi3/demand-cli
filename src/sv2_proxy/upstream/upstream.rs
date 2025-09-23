use super::super::error::{Error, ProxyResult};
use binary_sv2::U256;
use roles_logic_sv2::{
    channel_logic::channel_factory::{
        ExtendedChannelKind, OnNewShare, ProxyExtendedChannelFactory, Share,
    },
    common_messages_sv2::Protocol,
    common_properties::{
        CommonDownstreamData, IsDownstream, IsMiningDownstream, IsMiningUpstream, IsUpstream,
    },
    handlers::{
        common::{ParseUpstreamCommonMessages, SendTo as SendToCommon},
        mining::{ParseUpstreamMiningMessages, SendTo},
    },
    mining_sv2::{
        ExtendedExtranonce, Extranonce, OpenExtendedMiningChannel, SubmitSharesStandard,
        UpdateChannel,
    },
    parsers::Mining,
    routing_logic::{MiningRoutingLogic, NoRouting},
    selectors::NullDownstreamMiningSelector,
    utils::{GroupId, Mutex},
    Error as RolesLogicError,
};
use std::{
    collections::{HashMap, VecDeque},
    sync::{atomic::AtomicBool, Arc},
};
use tokio::{
    sync::mpsc::{Receiver as TReceiver, Sender as TSender},
    task,
};
use tracing::{error, info, warn};

use super::task_manager::TaskManager;
use crate::shared::utils::AbortOnDrop;
use bitcoin::BlockHash;

pub struct OpenChannelReq {
    pub request_id: u32,
    pub downstream_hash_rate: f32,
    pub is_header_only: bool,
    pub id: u32,
}

pub static IS_NEW_JOB_HANDLED: AtomicBool = AtomicBool::new(true);
/// Represents the currently active `prevhash` of the mining job being worked on OR being submitted
/// from the Downstream role.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct PrevHash {
    /// `prevhash` of mining job.
    prev_hash: BlockHash,
    /// `nBits` encoded difficulty target.
    nbits: u32,
}

#[derive(Debug)]
pub struct Upstream {
    /// Newly assigned identifier of the channel, stable for the whole lifetime of the connection,
    /// e.g. it is used for broadcasting new jobs by the `NewExtendedMiningJob` message.
    pub(super) channel_id: Option<u32>,
    /// Identifier of the job as provided by the `NewExtendedMiningJob` message.
    job_id: Option<u32>,
    /// Identifier of the job as provided by the ` SetCustomMiningJobSucces` message
    last_job_id: Option<u32>,
    /// Bytes used as implicit first part of `extranonce`.
    extranonce_prefix: Option<Vec<u8>>,
    pub min_extranonce_size: u16,
    pub upstream_extranonce1_size: usize,
    pub sender: TSender<Mining<'static>>,
    channel_factory: Option<ProxyExtendedChannelFactory>,
    hom_downstreams: HashMap<u32, tokio::sync::mpsc::Sender<Mining<'static>>>,
    total_hash_rate: f32,
}

impl PartialEq for Upstream {
    fn eq(&self, other: &Self) -> bool {
        self.channel_id == other.channel_id
    }
}

impl Upstream {
    fn keep_alive(self_: Arc<Mutex<Self>>) {
        tokio::task::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                let nominal_hash_rate = self_.safe_lock(|u| u.total_hash_rate).unwrap();
                let sender = self_.safe_lock(|u| u.sender.clone()).unwrap();
                let update_channel = UpdateChannel {
                    channel_id: self_.safe_lock(|u| u.channel_id.unwrap()).unwrap(),
                    nominal_hash_rate,
                    maximum_target: vec![255_u8; 32].try_into().unwrap(),
                };
                sender
                    .send(Mining::UpdateChannel(update_channel))
                    .await
                    .unwrap();
            }
        });
    }
    fn on_submit(&mut self, submit: SubmitSharesStandard) -> OnNewShare {
        if let Some(channel_factory) = self.channel_factory.as_mut() {
            match channel_factory.on_submit_shares_standard(submit.as_static().clone()) {
                Ok(msg) => msg,
                Err(e) => panic!("{}", e),
            }
        } else {
            panic!()
        }
    }

    fn handle_submission(
        self_: Arc<Mutex<Self>>,
        mut submit_receiver: TReceiver<(
            SubmitSharesStandard,
            tokio::sync::oneshot::Sender<OnNewShare>,
        )>,
    ) {
        tokio::task::spawn(async move {
            while let Some((submit, response)) = submit_receiver.recv().await {
                if let Some(msg) = match self_.safe_lock(|u| u.on_submit(submit)).unwrap() {
                    OnNewShare::SendErrorDownstream(e) => {
                        warn!("share error");
                        response
                            .send(OnNewShare::SendErrorDownstream(e))
                            .unwrap();
                        None
                    }
                    OnNewShare::SendSubmitShareUpstream((share,_)) => {
                        match share {
                            Share::Extended(share) => {
                                info!("share meet upstream target");
                                response.send(OnNewShare::ShareMeetDownstreamTarget).unwrap();
                                Some(Mining::SubmitSharesExtended(share))
                            }
                            Share::Standard((_, _)) => {
                                unreachable!()
                            }
                        }
                    }
                    OnNewShare::RelaySubmitShareUpstream => {
                        unreachable!()
                    }
                    OnNewShare::ShareMeetBitcoinTarget(_) => {
                        unreachable!()
                    }
                    OnNewShare::ShareMeetDownstreamTarget => {
                        info!("share meet downstream target");
                        response
                            .send(OnNewShare::ShareMeetDownstreamTarget)
                            .unwrap();
                        None
                    }
                } {
                    let tx_frame = self_
                        .safe_lock(|s| s.sender.clone())
                        .map_err(|_| Error::TranslatorUpstreamMutexPoisoned)
                        .unwrap();
                    tx_frame.send(msg).await.unwrap();
                };
            }
        });
    }
    /// Instantiate a new `Upstream`.
    /// Connect to the SV2 Upstream role (most typically a SV2 Pool). Initializes the
    /// `UpstreamConnection` with a channel to send and receive messages from the SV2 Upstream
    /// role and uses channels provided in the function arguments to send and receive messages
    /// from the `Downstream`.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        min_extranonce_size: u16,
        sender: TSender<Mining<'static>>,
    ) -> ProxyResult<'static, Arc<Mutex<Self>>> {
        Ok(Arc::new(Mutex::new(Self {
            extranonce_prefix: None,
            channel_id: None,
            job_id: None,
            last_job_id: None,
            min_extranonce_size,
            upstream_extranonce1_size: crate::UPSTREAM_EXTRANONCE1_SIZE,
            sender,
            channel_factory: None,
            hom_downstreams: HashMap::new(),
            total_hash_rate: 0.0,
        })))
    }

    pub async fn start(
        self_: Arc<Mutex<Self>>,
        incoming_receiver: TReceiver<Mining<'static>>,
        mut add_hom_downstream: tokio::sync::mpsc::Receiver<(
            OpenChannelReq,
            tokio::sync::mpsc::Sender<Mining<'static>>,
        )>,
        submit_receiver: TReceiver<(
            SubmitSharesStandard,
            tokio::sync::oneshot::Sender<OnNewShare>,
        )>,
    ) -> Result<AbortOnDrop, Error<'static>> {
        let task_manager = TaskManager::initialize();
        let abortable = task_manager
            .safe_lock(|t| t.get_aborter())
            .map_err(|_| Error::TranslatorTaskManagerMutexPoisoned)?
            .ok_or(Error::TranslatorTaskManagerFailed)?;

        Self::connect(self_.clone()).await?;
        Self::handle_submission(self_.clone(), submit_receiver);
        Self::keep_alive(self_.clone());

        let main_loop_abortable = Self::parse_incoming(self_.clone(), incoming_receiver)?;

        tokio::task::spawn(async move {
            let mut downstream_to_add: VecDeque<(
                OpenChannelReq,
                tokio::sync::mpsc::Sender<Mining<'static>>,
            )> = VecDeque::with_capacity(10_000);
            while let Some((mut open_channel_req, message_sender)) = add_hom_downstream.recv().await {
                open_channel_req.downstream_hash_rate = open_channel_req.downstream_hash_rate;
                if self_.safe_lock(|u| u.channel_factory.is_some()).unwrap() {
                    let mut new_hash_rate = self_.safe_lock(|u| u.total_hash_rate).unwrap();

                    for (open_channel_req, message_sender) in &downstream_to_add {
                        let request_id = open_channel_req.request_id;
                        let downstream_hash_rate = open_channel_req.downstream_hash_rate;
                        let is_header_only = open_channel_req.is_header_only;
                        let id = open_channel_req.id;
                        new_hash_rate += downstream_hash_rate;
                        let to_send: Vec<Mining<'static>> = self_
                            .safe_lock(|u| {
                                u.channel_factory
                                    .as_mut()
                                    .unwrap()
                                    .add_standard_channel(
                                        request_id,
                                        downstream_hash_rate,
                                        is_header_only,
                                        id,
                                    )
                                    .unwrap()
                                    .into_iter()
                                    .map(|m| m.into_static())
                                    .collect()
                            })
                            .unwrap();
                        for m in to_send {
                            message_sender.send(m).await.unwrap();
                        }
                        self_
                            .safe_lock(|u| u.hom_downstreams.insert(id, message_sender.clone()))
                            .unwrap();
                    }

                    let request_id = open_channel_req.request_id;
                    let downstream_hash_rate = open_channel_req.downstream_hash_rate;
                    let is_header_only = open_channel_req.is_header_only;
                    let id = open_channel_req.id;
                    new_hash_rate += downstream_hash_rate;
                    let to_send: Vec<Mining<'static>> = self_
                        .safe_lock(|u| {
                            u.channel_factory
                                .as_mut()
                                .unwrap()
                                .add_standard_channel(
                                    request_id,
                                    downstream_hash_rate,
                                    is_header_only,
                                    id,
                                )
                                .unwrap()
                                .into_iter()
                                .map(|m| m.into_static())
                                .collect()
                        })
                        .unwrap();
                    for m in to_send {
                        message_sender.send(m).await.unwrap();
                    }
                    self_
                        .safe_lock(|u| u.hom_downstreams.insert(id, message_sender))
                        .unwrap();

                    let sender = self_.safe_lock(|u| u.sender.clone()).unwrap();
                    let update_channel = UpdateChannel {
                        channel_id: self_.safe_lock(|u| u.channel_id.unwrap()).unwrap(),
                        nominal_hash_rate: new_hash_rate,
                        maximum_target: vec![255_u8; 32].try_into().unwrap(),
                    };
                    sender
                        .send(Mining::UpdateChannel(update_channel))
                        .await
                        .unwrap();

                    if downstream_to_add.len() > 0 {
                        downstream_to_add = VecDeque::with_capacity(0);
                    }
                } else {
                    if downstream_to_add.len() < 10_000 {
                        downstream_to_add.push_back((open_channel_req, message_sender));
                    } else {
                        downstream_to_add.pop_front();
                        downstream_to_add.push_back((open_channel_req, message_sender));
                    }
                }
            }
        });

        TaskManager::add_main_loop(task_manager.clone(), main_loop_abortable)
            .await
            .map_err(|_| Error::TranslatorTaskManagerFailed)?;

        Ok(abortable)
    }

    /// Setups the connection with the SV2 Upstream role (most typically a SV2 Pool).
    async fn connect(self_: Arc<Mutex<Self>>) -> ProxyResult<'static, ()> {
        let sender = self_
            .safe_lock(|s| s.sender.clone())
            .map_err(|_e| Error::TranslatorUpstreamMutexPoisoned)?;

        let user_identity = "ABC".to_string().try_into().expect("Internal error: this operation can not fail because the string ABC can always be converted into Inner");
        let open_channel = Mining::OpenExtendedMiningChannel(OpenExtendedMiningChannel {
            request_id: 0, // TODO
            user_identity, // TODO
            nominal_hash_rate: 0.0,
            max_target: vec![255_u8; 32].try_into().unwrap(),
            min_extranonce_size: crate::MIN_EXTRANONCE2_SIZE,
        });

        if sender.send(open_channel).await.is_err() {
            error!("Failed to send message");
            return Err(Error::AsyncChannelError);
        };
        Ok(())
    }

    /// Parses the incoming SV2 message from the Upstream role and routes the message to the
    /// appropriate handler.
    #[allow(clippy::result_large_err)]
    pub fn parse_incoming(
        self_: Arc<Mutex<Self>>,
        mut receiver: TReceiver<Mining<'static>>,
    ) -> ProxyResult<'static, AbortOnDrop> {
        let clone = self_.clone();
        let tx_frame = clone
            .safe_lock(|s| s.sender.clone())
            .map_err(|_| Error::TranslatorUpstreamMutexPoisoned)?;
        let main_loop_handle = {
            let self_ = self_.clone();
            task::spawn(async move {
                while let Some(m) = receiver.recv().await {
                    let routing_logic = MiningRoutingLogic::None;

                    // Gets the response message for the received SV2 Upstream role message
                    // `handle_message_mining` takes care of the SetupConnection +
                    // SetupConnection.Success
                    let next_message_to_send = Upstream::handle_message_mining_deserialized(
                        self_.clone(),
                        Ok(m),
                        routing_logic,
                    );

                    // Routes the incoming messages accordingly
                    match next_message_to_send {
                        Ok(SendTo::Respond(message_for_upstream)) => {
                            if tx_frame.send(message_for_upstream).await.is_err() {
                                error!("Failed to send message to upstream role");
                                return;
                            };
                        }
                        Ok(SendTo::RelayNewMessage(Mining::SetNewPrevHash(m))) => {
                            let homs = self_.safe_lock(|s| s.hom_downstreams.clone()).unwrap();
                            for (_, channel) in homs {
                                if channel
                                    .send(Mining::SetNewPrevHash(m.clone()))
                                    .await
                                    .is_err()
                                {
                                    error!("Failed to send message to downstream role");
                                    return;
                                };
                            }
                        }
                        Ok(SendTo::Multiple(ms)) => {
                            for m in ms {
                                match m {
                                    SendTo::RelayNewMessageToRemote(downstream, m) => {
                                        let downstream = downstream.safe_lock(|d| d.0).unwrap();
                                        let channel = self_
                                            .safe_lock(|s| {
                                                s.hom_downstreams
                                                    .get(&downstream)
                                                    .expect("Invalid state")
                                                    .clone()
                                            })
                                            .unwrap();
                                        if channel.send(m).await.is_err() {
                                            error!("Failed to send message to downstream role");
                                            return;
                                        };
                                    }
                                    _ => unreachable!(),
                                }
                            }
                        }
                        Ok(SendTo::None(None)) => (),
                        Ok(_) => panic!(),
                        Err(e) => {
                            error!("{}", Error::RolesSv2Logic(e));
                            return;
                        }
                    }
                }
                error!("Failed to receive message");
            })
        };
        Ok(main_loop_handle.into())
    }
}

#[derive(Debug)]
pub struct Downstream(pub u32);

impl IsDownstream for Downstream {
    fn get_downstream_mining_data(&self) -> CommonDownstreamData {
        CommonDownstreamData {
            header_only: true,
            work_selection: false,
            version_rolling: true,
        }
    }
}

impl IsMiningDownstream for Downstream {}

impl IsUpstream<Downstream, NullDownstreamMiningSelector> for Upstream {
    fn get_version(&self) -> u16 {
        todo!()
    }

    fn get_flags(&self) -> u32 {
        todo!()
    }

    fn get_supported_protocols(&self) -> Vec<Protocol> {
        todo!()
    }

    fn get_id(&self) -> u32 {
        todo!()
    }

    fn get_mapper(&mut self) -> Option<&mut roles_logic_sv2::common_properties::RequestIdMapper> {
        todo!()
    }

    fn get_remote_selector(&mut self) -> &mut NullDownstreamMiningSelector {
        todo!()
    }
}

impl IsMiningUpstream<Downstream, NullDownstreamMiningSelector> for Upstream {
    fn total_hash_rate(&self) -> u64 {
        todo!()
    }

    fn add_hash_rate(&mut self, _to_add: u64) {
        todo!()
    }

    fn get_opened_channels(
        &mut self,
    ) -> &mut Vec<roles_logic_sv2::common_properties::UpstreamChannel> {
        todo!()
    }

    fn update_channels(&mut self, _c: roles_logic_sv2::common_properties::UpstreamChannel) {
        todo!()
    }
}

impl ParseUpstreamCommonMessages<NoRouting> for Upstream {
    fn handle_setup_connection_success(
        &mut self,
        _: roles_logic_sv2::common_messages_sv2::SetupConnectionSuccess,
    ) -> Result<SendToCommon, RolesLogicError> {
        Ok(SendToCommon::None(None))
    }

    fn handle_setup_connection_error(
        &mut self,
        _: roles_logic_sv2::common_messages_sv2::SetupConnectionError,
    ) -> Result<SendToCommon, RolesLogicError> {
        todo!()
    }

    fn handle_channel_endpoint_changed(
        &mut self,
        _: roles_logic_sv2::common_messages_sv2::ChannelEndpointChanged,
    ) -> Result<SendToCommon, RolesLogicError> {
        todo!()
    }
}

/// Connection-wide SV2 Upstream role messages parser implemented by a downstream ("downstream"
/// here is relative to the SV2 Upstream role and is represented by this `Upstream` struct).
impl ParseUpstreamMiningMessages<Downstream, NullDownstreamMiningSelector, NoRouting> for Upstream {
    /// Returns the channel type between the SV2 Upstream role and the `Upstream`, which will
    /// always be `Extended` for a SV1/SV2 Translator Proxy.
    fn get_channel_type(&self) -> roles_logic_sv2::handlers::mining::SupportedChannelTypes {
        roles_logic_sv2::handlers::mining::SupportedChannelTypes::Extended
    }

    /// Work selection is disabled for SV1/SV2 Translator Proxy and all work selection is performed
    /// by the SV2 Upstream role.
    fn is_work_selection_enabled(&self) -> bool {
        false
    }

    /// The SV2 `OpenStandardMiningChannelSuccess` message is NOT handled because it is NOT used
    /// for the Translator Proxy as only `Extended` channels are used between the SV1/SV2 Translator
    /// Proxy and the SV2 Upstream role.
    fn handle_open_standard_mining_channel_success(
        &mut self,
        _m: roles_logic_sv2::mining_sv2::OpenStandardMiningChannelSuccess,
        _remote: Option<Arc<Mutex<Downstream>>>,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        panic!("Standard Mining Channels are not used in Translator Proxy")
    }

    /// Handles the SV2 `OpenExtendedMiningChannelSuccess` message which provides important
    /// parameters including the `target` which is sent to the Downstream role in a SV1
    /// `mining.set_difficulty` message, and the extranonce values which is sent to the Downstream
    /// role in a SV1 `mining.subscribe` message response.
    fn handle_open_extended_mining_channel_success(
        &mut self,
        m: roles_logic_sv2::mining_sv2::OpenExtendedMiningChannelSuccess,
    ) -> Result<SendTo<Downstream>, RolesLogicError> {
        info!(
            "Handling OpenExtendedMiningChannelSuccess message from Pool for Channel Id: {}",
            m.channel_id
        );
        let tproxy_e1_len =
            proxy_extranonce1_len(m.extranonce_size as usize, self.min_extranonce_size.into())
                as u16;
        if self.min_extranonce_size + tproxy_e1_len < m.extranonce_size {
            error!(
                "Invalid extranonce size for Channel Id {}: expected at least {} but got {}",
                m.channel_id,
                self.min_extranonce_size + tproxy_e1_len,
                m.extranonce_size
            );
            return Err(RolesLogicError::InvalidExtranonceSize(
                self.min_extranonce_size,
                m.extranonce_size,
            ));
        }
        info!(
            "Extended Channel with Channel Id {} has Target: {:?}",
            m.channel_id, m.target
        );
        self.channel_id = Some(m.channel_id);
        self.extranonce_prefix = Some(m.extranonce_prefix.to_vec());
        let prefix_len = m.extranonce_prefix.len();
        self.upstream_extranonce1_size = prefix_len;
        let miner_extranonce2_size = self.min_extranonce_size as usize;

        let extranonce_prefix: Extranonce = m.extranonce_prefix.into();
        // Create the extended extranonce that will be saved in bridge and
        // it will be used to open downstream (sv1) channels
        // range 0 is the extranonce1 from upstream
        // range 1 is the extranonce1 added by the tproxy
        // range 2 is the extranonce2 used by the miner for rolling
        // range 0 + range 1 is the extranonce1 sent to the miner
        let tproxy_e1_len =
            proxy_extranonce1_len(m.extranonce_size as usize, miner_extranonce2_size);
        let range_0 = 0..prefix_len; // upstream extranonce1
        let range_1 = prefix_len..prefix_len + tproxy_e1_len; // downstream extranonce1
        let range_2 = prefix_len + tproxy_e1_len..prefix_len + m.extranonce_size as usize; // extranonce2
        let extended = match ExtendedExtranonce::from_upstream_extranonce(
            extranonce_prefix.clone(),
            range_0.clone(),
            range_1.clone(),
            range_2.clone(),
        )
        .ok_or(Error::InvalidExtranonce(format!(
            "Impossible to create a valid extended extranonce from {:?} {:?} {:?} {:?}",
            extranonce_prefix, range_0, range_1, range_2
        ))) {
            Ok(extended_extranounce) => extended_extranounce,
            Err(_) => {
                todo!();
            }
        };
        let ids = Arc::new(Mutex::new(GroupId::new()));
        let channel_factory = ProxyExtendedChannelFactory::new(
            ids,
            extended,
            None,
            crate::SHARE_PER_MIN,
            ExtendedChannelKind::Proxy {
                upstream_target: m.target.into(),
            },
            None,
            m.channel_id,
        );
        self.channel_factory = Some(channel_factory);
        Ok(SendTo::None(None))
    }

    /// Handles the SV2 `OpenExtendedMiningChannelError` message (TODO).
    fn handle_open_mining_channel_error(
        &mut self,
        m: roles_logic_sv2::mining_sv2::OpenMiningChannelError,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        Ok(SendTo::None(Some(Mining::OpenMiningChannelError(
            m.as_static(),
        ))))
    }

    /// Handles the SV2 `UpdateChannelError` message (TODO).
    fn handle_update_channel_error(
        &mut self,
        m: roles_logic_sv2::mining_sv2::UpdateChannelError,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        Ok(SendTo::None(Some(Mining::UpdateChannelError(
            m.as_static(),
        ))))
    }

    /// Handles the SV2 `CloseChannel` message (TODO).
    fn handle_close_channel(
        &mut self,
        m: roles_logic_sv2::mining_sv2::CloseChannel,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        Ok(SendTo::None(Some(Mining::CloseChannel(m.as_static()))))
    }

    /// Handles the SV2 `SetExtranoncePrefix` message (TODO).
    fn handle_set_extranonce_prefix(
        &mut self,
        _: roles_logic_sv2::mining_sv2::SetExtranoncePrefix,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        todo!()
    }

    /// Handles the SV2 `SubmitSharesSuccess` message.
    fn handle_submit_shares_success(
        &mut self,
        _m: roles_logic_sv2::mining_sv2::SubmitSharesSuccess,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        info!("SubmitSharesSuccess");
        Ok(SendTo::None(None))
    }

    /// Handles the SV2 `SubmitSharesError` message.
    fn handle_submit_shares_error(
        &mut self,
        _m: roles_logic_sv2::mining_sv2::SubmitSharesError,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        error!("SubmitSharesError");
        Ok(SendTo::None(None))
    }

    /// The SV2 `NewMiningJob` message is NOT handled because it is NOT used for the Translator
    /// Proxy as only `Extended` channels are used between the SV1/SV2 Translator Proxy and the SV2
    /// Upstream role.
    fn handle_new_mining_job(
        &mut self,
        _m: roles_logic_sv2::mining_sv2::NewMiningJob,
    ) -> Result<SendTo<Downstream>, RolesLogicError> {
        panic!("Standard Mining Channels are not used in Translator Proxy")
    }

    /// Handles the SV2 `NewExtendedMiningJob` message which is used (along with the SV2
    /// `SetNewPrevHash` message) to later create a SV1 `mining.notify` for the Downstream
    /// role.
    fn handle_new_extended_mining_job(
        &mut self,
        m: roles_logic_sv2::mining_sv2::NewExtendedMiningJob,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        // TODO TODO TODO if we want to enable both sv2 and sv1 downstreams together in the
        // same proxy this global should have another name!!!!!!!
        IS_NEW_JOB_HANDLED.store(false, std::sync::atomic::Ordering::SeqCst);
        if !m.version_rolling_allowed {
            warn!("VERSION ROLLING NOT ALLOWED IS A TODO");
            // todo!()
        }

        if let Some(channel_factory) = self.channel_factory.as_mut() {
            let messages = channel_factory
                .on_new_extended_mining_job(m.as_static().clone())
                .unwrap()
                .into_iter()
                .map(|(channel, message)| {
                    if let Mining::NewMiningJob(mut message) = message {
                        message.job_id = m.job_id;
                        let message = Mining::NewMiningJob(message);
                        SendTo::RelayNewMessageToRemote(
                            Arc::new(Mutex::new(Downstream(channel))),
                            message.into_static(),
                        )
                        
                    } else {
                        SendTo::RelayNewMessageToRemote(
                            Arc::new(Mutex::new(Downstream(channel))),
                            message.into_static(),
                        )
                    }
                })
                .collect();
            Ok(SendTo::Multiple(messages))
        } else {
            Ok(SendTo::None(None))
        }
    }

    /// Handles the SV2 `SetNewPrevHash` message which is used (along with the SV2
    /// `NewExtendedMiningJob` message) to later create a SV1 `mining.notify` for the Downstream
    /// role.
    fn handle_set_new_prev_hash(
        &mut self,
        m: roles_logic_sv2::mining_sv2::SetNewPrevHash,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        if let Some(channel_factory) = self.channel_factory.as_mut() {
            channel_factory
                .on_new_prev_hash(m.as_static().clone())
                .unwrap();
            let message = Mining::SetNewPrevHash(m.into_static());
            Ok(SendTo::RelayNewMessage(message))
        } else {
            Ok(SendTo::None(None))
        }
    }

    /// Handles the SV2 `SetCustomMiningJobSuccess` message (TODO).
    fn handle_set_custom_mining_job_success(
        &mut self,
        m: roles_logic_sv2::mining_sv2::SetCustomMiningJobSuccess,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        self.last_job_id = Some(m.job_id);
        Ok(SendTo::None(None))
    }

    /// Handles the SV2 `SetCustomMiningJobError` message (TODO).
    fn handle_set_custom_mining_job_error(
        &mut self,
        _m: roles_logic_sv2::mining_sv2::SetCustomMiningJobError,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        unimplemented!()
    }

    /// Handles the SV2 `SetTarget` message which updates the Downstream role(s) target
    /// difficulty via the SV1 `mining.set_difficulty` message.
    fn handle_set_target(
        &mut self,
        m: roles_logic_sv2::mining_sv2::SetTarget,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        let m = m.into_static();
        Ok(SendTo::None(None))
    }

    /// Handles the SV2 `Reconnect` message (TODO).
    fn handle_reconnect(
        &mut self,
        _m: roles_logic_sv2::mining_sv2::Reconnect,
    ) -> Result<roles_logic_sv2::handlers::mining::SendTo<Downstream>, RolesLogicError> {
        unimplemented!()
    }
}

/// currently the pool only supports 16 bytes exactly for its channels
/// to use but that may change
pub fn proxy_extranonce1_len(
    channel_extranonce2_size: usize,
    downstream_extranonce2_len: usize,
) -> usize {
    // full_extranonce_len - pool_extranonce1_len - miner_extranonce2 = tproxy_extranonce1_len
    channel_extranonce2_size - downstream_extranonce2_len
}

pub fn hash_rate_to_target(hashrate_hs: f64, shares_per_min: f64) -> Result<U256<'static>, ()> {
    if shares_per_min <= 0.0 { return Err(()); }
    if hashrate_hs < 0.0 { return Err(()); }

    let s = 60.0 / shares_per_min;
    let hs_f = (hashrate_hs * s).max(1.0).round();
    let hs = hs_f as u128;

    let denom = hs.saturating_add(1);

    let two256_minus1 = primitive_types::U256::MAX;
    let mut hs_be = [0u8; 32];
    hs_be[16..].copy_from_slice(&hs.to_be_bytes());
    let hs_u256 = primitive_types::U256::from_big_endian(&hs_be);

    let numerator = two256_minus1.saturating_sub(hs_u256);

    let target = numerator / primitive_types::U256::from(denom);
    let target = target.to_little_endian();
    Ok(target.into())
}
