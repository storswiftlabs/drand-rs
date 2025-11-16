use super::{state::State, store::DkgStoreError, ActionsError};
use crate::{
    core::beacon::BeaconProcess,
    info,
    key::{group::Group, toml::Toml, Scheme},
    protobuf::dkg::{DkgStatusResponse, JoinOptions},
    transport::dkg::Command,
};
use std::future::Future;

/// Contains all the DKG actions that require user interaction: creating a network,
/// accepting or rejecting a DKG, getting the status, etc. Both leader and follower interactions are contained herein.
pub trait ActionsActive {
    type Scheme: Scheme;

    fn command(&self, cmd: Command) -> impl Future<Output = Result<(), ActionsError>>;
    fn dkg_status(&self) -> Result<DkgStatusResponse, ActionsError>;
    fn start_join(
        &self,
        state: &mut State<Self::Scheme>,
        options: JoinOptions,
    ) -> impl Future<Output = Result<(), ActionsError>>;
    fn start_accept(
        &self,
        state: State<Self::Scheme>,
    ) -> impl Future<Output = Result<(), ActionsError>>;
    fn start_reject(
        &self,
        state: State<Self::Scheme>,
    ) -> impl Future<Output = Result<(), ActionsError>>;
}

impl<S: Scheme> ActionsActive for BeaconProcess<S> {
    type Scheme = S;
    fn dkg_status(&self) -> Result<DkgStatusResponse, ActionsError> {
        let complete = match self.dkg_store().get_finished::<S>() {
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
            current: Some(self.dkg_store().get_current::<S>()?.into()),
            complete,
        };

        Ok(responce)
    }

    async fn command(&self, cmd: Command) -> Result<(), ActionsError> {
        // Apply the proposal to the last succesful state
        let mut state = self.dkg_store().get_last_succesful::<S>(self.id())?;

        info!(self.log(), "running DKG command: {cmd}");
        match cmd {
            Command::Join(join_options) => self.start_join(&mut state, join_options).await?,
            Command::Accept(_) => self.start_accept(state).await?,
            Command::Reject(_) => self.start_reject(state).await?,
            // Other commands are reserved for leader role.
            _ => return Err(ActionsError::NotSupported),
        }

        Ok(())
    }

    async fn start_join(
        &self,
        state: &mut State<S>,
        options: crate::protobuf::dkg::JoinOptions,
    ) -> Result<(), ActionsError> {
        let prev_group: Option<Group<S>> = if state.epoch() > 1 {
            if options.group_file.is_empty() {
                return Err(ActionsError::GroupfileIsMissing);
            }
            let group_str =
                String::from_utf8(options.group_file).map_err(|_| ActionsError::GroupFileParse)?;

            let group: Group<S> = Toml::toml_decode(
                &group_str
                    .parse()
                    .map_err(|_| ActionsError::GroupFileParse)?,
            )
            .ok_or(ActionsError::GroupFileParse)?;

            Some(group)
        } else {
            None
        };

        let me = self.as_participant()?;
        state
            .joined(&me, prev_group)
            .map_err(ActionsError::DBState)?;
        // joiners don't need to gossip anything

        self.dkg_store().save_current(state)?;

        Ok(())
    }

    async fn start_accept(&self, mut state: State<S>) -> Result<(), ActionsError> {
        let me = self.as_participant()?;
        state.accepted(me)?;
        self.dkg_store().save_current(&state)?;

        // TODO: return packet to gossip
        Ok(())
    }

    async fn start_reject(&self, mut state: State<S>) -> Result<(), ActionsError> {
        let me = self.as_participant()?;
        state.rejected(me)?;
        self.dkg_store().save_current(&state)?;

        // TODO: return packet to gossip
        Ok(())
    }
}
