use std::fmt::Display;

use super::actions_active as active;
use super::state::DBStateError;
use super::state::State;

use crate::core::beacon::BeaconProcess;
use crate::key::keys::Identity;
use crate::key::Scheme;

use crate::protobuf::dkg::packet::Bundle;
use crate::protobuf::dkg::DkgStatusResponse;
use crate::transport::dkg::Command;
use crate::transport::dkg::GossipData;
use crate::transport::dkg::GossipPacket;

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
const ERR_SEND: &str = "dkg callback receiver is dropped";

impl<T> Callback<T> {
    /// Sends a response back through the callback channel.
    fn reply(self, result: Result<T, ActionsError>) {
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

pub struct DkgHandler {
    sender: mpsc::Sender<Actions>,
}

pub enum Actions {
    Gossip(GossipPacket, Callback<()>),
    Shutdown(Callback<()>),
    Command(Command, Callback<()>),
    Broadcast(Bundle, Callback<()>),
    Status(Callback<DkgStatusResponse>),
}

#[derive(thiserror::Error, Debug)]
pub enum ActionsError {
    #[error("db state error: {0}")]
    DBState(#[from] DBStateError),
    #[error("TODO: this dkg action is not implemented yet")]
    TODO,
}

impl DkgHandler {
    pub async fn new_action(&self, action: Actions) {
        if self.sender.send(action).await.is_err() {
            error!("{ERR_SEND}")
        }
    }

    pub fn fresh_install<S: Scheme>(bp: BeaconProcess<S>) -> Self {
        let (sender, mut receiver) = mpsc::channel::<Actions>(1);
        let tracker = bp.tracker().clone();

        tracker.spawn(async move {
            let me = bp.keypair.public_identity();
            let mut db_state = State::<S>::new_fresh(bp.id());

            while let Some(actions) = receiver.recv().await {
                handle_actions(&mut db_state, me, actions).await;
            }

            debug!("dkg handler is shutting down")
        });

        Self { sender }
    }
}

async fn handle_actions<S: Scheme>(db: &mut State<S>, me: &Identity<S>, request: Actions) {
    match request {
        Actions::Shutdown(callback) => callback.reply(todo_request("shutdown")),
        Actions::Status(callback) => callback.reply(active::status(db)),
        Actions::Command(cmd, callback) => callback.reply(active::command(cmd, db, me).await),
        Actions::Gossip(packet, callback) => callback.reply(gossip(db, me, packet).await),
        Actions::Broadcast(bundle, callback) => callback.reply(todo_request(&bundle.to_string())),
    }
}

async fn gossip<S: Scheme>(
    db: &mut State<S>,
    me: &Identity<S>,
    packet: GossipPacket,
) -> Result<(), ActionsError> {
    let meta = &packet.metadata;
    match packet.data {
        GossipData::Proposal(proposal) => db.proposed(me, proposal, meta)?,
        _ => todo_request(&packet.data)?,
    }

    Ok(())
}

/// Temporary tracker for unfinished logic
pub fn todo_request<D: Display + ?Sized>(kind: &D) -> Result<(), ActionsError> {
    warn!("received TODO request: {kind}");

    Err(ActionsError::TODO)
}
