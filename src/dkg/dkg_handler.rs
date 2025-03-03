use super::state_machine::db_state::DBState;
use super::state_machine::db_state::DBStateError;

use crate::core::beacon::BeaconProcess;
use crate::key::keys::Identity;
use crate::key::Scheme;

use crate::protobuf::dkg::packet::Bundle;
use crate::protobuf::dkg::DkgStatusResponse;
use crate::transport::dkg::DkgCommand;
use crate::transport::dkg::GossipData;
use crate::transport::dkg::GossipPacket;

use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::debug;
use tracing::error;

/// Callback for all dkg actions
pub type DkgCallback<T> = tokio::sync::oneshot::Sender<Result<T, DkgActionsError>>;

/// For diagnostic purposes, should be never possible
const ERR_SEND: &str = "dkg receiver is dropped";

pub struct DkgHandler {
    sender: mpsc::Sender<DkgActions>,
}

pub enum DkgActions {
    Gossip(GossipPacket, DkgCallback<()>),
    Shutdown(DkgCallback<()>),
    Command(DkgCommand, DkgCallback<()>),
    Broadcast(Bundle, DkgCallback<()>),
    Status(DkgCallback<DkgStatusResponse>),
}

#[derive(thiserror::Error, Debug)]
pub enum DkgActionsError {
    #[error("db state error: {0}")]
    DBState(#[from] DBStateError),
    #[error("TODO: this dkg action is not implemented yet")]
    TODO,
}

impl DkgHandler {
    pub async fn new_action(&self, action: DkgActions) {
        if self.sender.send(action).await.is_err() {
            error!("{ERR_SEND}")
        }
    }

    pub fn fresh_install<S: Scheme>(bp: BeaconProcess<S>) -> Self {
        let (sender, mut receiver) = mpsc::channel::<DkgActions>(1);
        let tracker = bp.tracker().clone();

        tracker.spawn(async move {
            let me = bp.keypair.public_identity();
            let mut db_state = DBState::<S>::new_fresh(bp.id());

            while let Some(actions) = receiver.recv().await {
                handle_actions(&mut db_state, me, actions).await;
            }

            debug!("dkg handler is shutting down")
        });

        Self { sender }
    }
}

async fn handle_actions<S: Scheme>(db: &mut DBState<S>, me: &Identity<S>, actions: DkgActions) {
    match actions {
        DkgActions::Shutdown(callback) => todo_request(callback, "shutdown"),
        DkgActions::Status(callback) => status(db, callback),
        DkgActions::Command(command, callback) => handle_command(command, callback).await,
        DkgActions::Gossip(gossip, callback) => handle_gossip(db, me, gossip, callback).await,
        DkgActions::Broadcast(_bundle, callback) => todo_request(callback, "broadcast"),
    }
}

/// Dkg status request
fn status<S: Scheme>(db: &mut DBState<S>, callback: DkgCallback<DkgStatusResponse>) {
    if callback
        .send(db.status().map_err(|err| {
            error!("dkg status request, id: {}, error: {err}", db.id());
            err.into()
        }))
        .is_err()
    {
        error!("dkg status request, id: {}, error: {ERR_SEND}", db.id());
    };
}

async fn handle_command(dkg_command: DkgCommand, callback: DkgCallback<()>) {
    let _metadata = &dkg_command.metadata;
    todo_request(callback, dkg_command.command.to_string().as_str())
}

async fn handle_gossip<S: Scheme>(
    db: &mut DBState<S>,
    me: &Identity<S>,
    gossip: crate::transport::dkg::GossipPacket,
    callback: oneshot::Sender<Result<(), DkgActionsError>>,
) {
    let metadata = &gossip.metadata;
    match gossip.data {
        GossipData::Proposal(proposal) => {
            if callback
                .send(db.proposed(me, proposal, metadata).map_err(|err| {
                    error!("received dkg proposal, id: {}, error: {err}", db.id());
                    err.into()
                }))
                .is_err()
            {
                error!("received dkg proposal, id: {}, error: {ERR_SEND}", db.id());
            };
        }
        _ => todo_request(callback, gossip.data.to_string().as_str()),
    }
}

fn todo_request<T>(callback: DkgCallback<T>, kind: &str) {
    if callback.send(Err(DkgActionsError::TODO)).is_err() {
        error!("failed to proceed {kind}, error: {ERR_SEND}",);
    };
}
