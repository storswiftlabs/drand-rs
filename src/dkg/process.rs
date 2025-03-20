use super::actions::ActionsActive;
use super::actions::ActionsError;
use super::actions::ActionsPassive;
use super::actions::ActionsSigning;
use super::state::State;
use super::status::Status;
use super::store::DkgStore;
use super::store::DkgStoreError;
use super::utils::GossipAuth;

use crate::core::beacon::BeaconID;
use crate::core::beacon::BeaconProcess;
use crate::key::group::Group;
use crate::key::Scheme;

use crate::protobuf::dkg::packet::Bundle;
use crate::protobuf::dkg::DkgStatusResponse;
use crate::transport::dkg::Command;
use crate::transport::dkg::GossipPacket;
use crate::transport::dkg::Participant;

use energon::traits::Affine;
use std::collections::HashSet;
use std::fmt::Display;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::debug;
use tracing::error;
use tracing::warn;

/// Callback for DKG actions
pub struct Callback<T> {
    inner: oneshot::Sender<Result<T, ActionsError>>,
}

/// For diagnostic purposes, this should never happen
pub const ERR_SEND: &str = "dkg callback receiver is dropped";

impl<T> Callback<T> {
    /// Sends a response back through the callback channel.
    fn reply(self, result: Result<T, ActionsError>) {
        // Track all outcoming errors
        if let Err(err) = &result {
            error!("failed to proceed dkg request: {err}")
        }
        if self.inner.send(result).is_err() {
            error!("{ERR_SEND}");
        };
    }
}

impl<T> From<oneshot::Sender<Result<T, ActionsError>>> for Callback<T> {
    fn from(tx: oneshot::Sender<Result<T, ActionsError>>) -> Self {
        Self { inner: tx }
    }
}

pub enum Actions {
    Gossip(GossipPacket, Callback<()>),
    Shutdown(Callback<()>),
    Command(Command, Callback<()>),
    Broadcast(Bundle, Callback<()>),
    Status(Callback<DkgStatusResponse>),
}

/// Contains signatures of seen packets.
pub(super) struct SeenPackets {
    seen: HashSet<String>,
}
/// Short signature of [`GossipMetadata`]
pub const SHORT_SIG_LEN: usize = 8;

impl SeenPackets {
    fn new() -> Self {
        Self {
            seen: HashSet::new(),
        }
    }

    pub(super) fn is_new_packet(&mut self, p: &GossipPacket) -> Result<bool, ActionsError> {
        let sig_hex = hex::encode(&p.metadata.signature);

        match sig_hex.get(..SHORT_SIG_LEN) {
            Some(short_sig) => {
                if self.seen.contains(&sig_hex) {
                    debug!(
                        "ignoring duplicate gossip packet, type: {} sig: {short_sig}",
                        p.data
                    );

                    Ok(false)
                } else {
                    tracing::debug!("processing DKG gossip packet, type: {}, sig: {short_sig}, id: {}, allegedly from: {}",
                        p.data, p.metadata.beacon_id, p.metadata.address);

                    // always true
                    Ok(self.seen.insert(sig_hex))
                }
            }
            None => Err(ActionsError::GossipSignatureLen),
        }
    }
}

pub struct DkgProcess {
    id: BeaconID,
    store: DkgStore,
    me: Participant,
}

impl DkgProcess {
    pub fn init<S: Scheme>(
        bp: &BeaconProcess<S>,
        fresh: bool,
    ) -> Result<mpsc::Sender<Actions>, DkgStoreError> {
        let store = DkgStore::init(bp.path(), fresh)?;

        match store.get_current::<S>() {
            Ok(mut state) => {
                // Check timeout in case node is within dkg
                if matches!(
                    *state.status(),
                    Status::Proposed
                        | Status::Proposing
                        | Status::Accepted
                        | Status::Executing
                        | Status::Joined
                ) && state.time_expired()
                {
                    state.status = Status::TimedOut;
                    store.save_current(&state)?
                }
            }
            Err(err) => {
                if err == DkgStoreError::NotFound {
                    store.save_current(&State::<S>::fresh(bp.id().as_str()))?;
                } else {
                    return Err(err);
                }
            }
        }

        let (tx, mut rx) = mpsc::channel::<Actions>(1);
        let process = Self {
            id: bp.id().clone(),
            store,
            me: bp.identity().try_into().unwrap(),
        };

        
        bp.tracker().spawn(async move {
            let mut seen = SeenPackets::new();
            while let Some(actions) = rx.recv().await {
                process.handle_actions::<S>(actions, &mut seen).await;
            }

            debug!("dkg handler is shutting down")
        });

        Ok(tx)
    }

    async fn handle_actions<S: Scheme>(&self, request: Actions, seen: &mut SeenPackets) {
        match request {
            Actions::Shutdown(callback) => callback.reply(todo_request("shutdown")),
            Actions::Status(callback) => callback.reply(self.dkg_status::<S>()),
            Actions::Command(cmd, callback) => callback.reply(self.command::<S>(cmd).await),
            Actions::Gossip(packet, callback) => {
                callback.reply(self.packet::<S>(packet, seen).await)
            }
            Actions::Broadcast(bundle, callback) => {
                callback.reply(todo_request(&bundle.to_string()))
            }
        }
    }
}

impl ActionsActive for DkgProcess {
    fn dkg_status<S: Scheme>(&self) -> Result<DkgStatusResponse, ActionsError> {
        let complete = match self.store.get_finished::<S>() {
            Ok(state) => Some(state.into()),
            Err(err) => {
                if err == DkgStoreError::NotFound {
                    None
                } else {
                    return Err(ActionsError::DKGStore(err));
                }
            }
        };

        let responce = DkgStatusResponse {
            current: Some(self.store.get_current::<S>()?.into()),
            complete,
        };

        Ok(responce)
    }

    async fn command<S: Scheme>(&self, cmd: Command) -> Result<(), ActionsError> {
        // Apply the proposal to the last succesful state
        let state = self.store.get_last_succesful::<S>(self.id.as_str())?;

        debug!("running DKG command: {cmd}, beaconID: {}", self.id);
        match cmd {
            Command::Join(join_options) => self.start_join(state, join_options).await?,
            _ => super::process::todo_request(&cmd)?,
        }

        Ok(())
    }

    async fn start_join<S: Scheme>(
        &self,
        mut state: State<S>,
        options: crate::protobuf::dkg::JoinOptions,
    ) -> Result<(), ActionsError> {
        // TODO: reshape
        let prev_group: Option<Group<S>> = if state.epoch() > 1 {
            drop(options);
            panic!("start_join: epoch can not be bigger than 1, reshape is not implemented yet")
        } else {
            None
        };

        state
            .joined(&self.me, prev_group)
            .map_err(ActionsError::DBState)?;
        // joiners don't need to gossip anything

        self.store.save_current(&state)?;

        Ok(())
    }
}

impl ActionsPassive for DkgProcess {
    async fn packet<S: Scheme>(
        &self,
        packet: GossipPacket,
        seen: &mut SeenPackets,
    ) -> Result<(), ActionsError> {
        // Ignore duplicate/incorrect packets
        if !seen.is_new_packet(&packet)? {
            return Ok(());
        }

        // TODO: if we're in the DKG protocol phase, we automatically broadcast it as it shouldn't update state
        self.apply_packet_to_state::<S>(packet).await?;
        // TODO: if packet.GetExecute(){..}

        Ok(())
    }

    async fn apply_packet_to_state<S: Scheme>(
        &self,
        packet: GossipPacket,
    ) -> Result<(), ActionsError> {
        let mut state = self.store.get_last_succesful::<S>(self.id.as_str())?;

        state.apply(&self.me, packet.clone()).await?;

        if let Err(err) = self.verify_msg(&packet, &state).await {
            error!(
                "failed to verify message, BeaconID: {}, error: {err}",
                self.id
            );
            return Err(err);
        }

        self.store.save_current(&state)?;

        Ok(())
    }
}

impl ActionsSigning for DkgProcess {
    async fn verify_msg<S: Scheme>(
        &self,
        gp: &GossipPacket,
        state: &State<S>,
    ) -> Result<(), ActionsError> {
        debug!(
            "Verifying DKG packet, beaconID: {}, from: {}",
            self.id, gp.metadata.address
        );

        // Find the participant the signature is allegedly from.
        // Return error if participant is not found in `remaining` or `joining`.
        if let Some(participant) = state
            .joining
            .iter()
            .find(|p| p.address == gp.metadata.address)
            .or_else(|| {
                state
                    .remaining
                    .iter()
                    .find(|p| p.address == gp.metadata.address)
            })
        {
            // Verify signature
            {
                let msg = self.msg_for_signing(gp, &state.encode());
                if !is_valid_signature::<S>(&participant.key, &gp.metadata.signature, &msg) {
                    Err(ActionsError::InvalidSignature)
                } else {
                    Ok(())
                }
            }
        } else {
            Err(ActionsError::MissingParticipant)
        }
    }
}

fn is_valid_signature<S: Scheme>(key: &[u8], sig: &[u8], msg: &[u8]) -> bool {
    if let Ok(key) = Affine::deserialize(key) {
        if let Ok(sig) = Affine::deserialize(sig) {
            return S::bls_verify(&key, &sig, msg).is_ok();
        };
    }
    false
}

/// Temporary tracker for unfinished logic
pub fn todo_request<D: Display + ?Sized>(kind: &D) -> Result<(), ActionsError> {
    warn!("received TODO request: {kind}");

    Err(ActionsError::TODO)
}
