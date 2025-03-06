//! This module contains all the DKG actions that require user interaction:
//! creating a network, accepting or rejecting a DKG, getting the status, etc.
//! Both leader and follower interactions are contained herein

use super::dkg_handler::ActionsError;
use super::state::State;

use crate::key::group::Group;
use crate::key::keys::Identity;
use crate::key::Scheme;

use crate::protobuf::dkg::DkgStatusResponse;
use crate::protobuf::dkg::JoinOptions;
use crate::transport::dkg::Command;

use tracing::info;

pub(super) async fn command<S: Scheme>(
    cmd: Command,
    state: &mut State<S>,
    me: &Identity<S>,
) -> Result<(), ActionsError> {
    info!("running DKG command: {cmd}, beaconID: {}", state.id());

    match cmd {
        Command::Join(join_options) => start_join(me, state, join_options).await,
        _ => super::dkg_handler::todo_request(&cmd),
    }
}

pub(super) async fn start_join<S: Scheme>(
    me: &Identity<S>,
    state: &mut State<S>,
    options: JoinOptions,
) -> Result<(), ActionsError> {
    let prev_group: Option<Group<S>> = if state.epoch() > 1 {
        // TODO: reshape
        drop(options);
        panic!("start_join: epoch can not be bigger than 1, reshape is not implemented yet")
    } else {
        None
    };

    state
        .joined(me, prev_group)
        .map_err(ActionsError::DBState)?;
    // joiners don't need to gossip anything

    Ok(())
}

/// TODO: this method should make request to dkg.db
pub(super) fn status<S: Scheme>(state: &State<S>) -> Result<DkgStatusResponse, ActionsError> {
    state.status().map_err(ActionsError::DBState)
}
