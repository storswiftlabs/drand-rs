// Copyright 2023-2025 StorSwift Inc.
// SPDX-License-Identifier: Apache-2.0

use super::{actions_signing::ActionsSigning, ActionsError};
use crate::{core::beacon::BeaconProcess, key::Scheme, transport::dkg::GossipPacket};
use prost_types::Timestamp;
use std::future::Future;

/// Contains all internal messaging between nodes triggered by the
/// protocol - things it does automatically upon receiving messages
/// from other nodes: storing proposals, aborting when the leader aborts, etc.
pub trait ActionsPassive {
    fn packet(
        &self,
        packet: GossipPacket,
    ) -> impl Future<Output = Result<Option<Timestamp>, ActionsError>>;

    fn apply_packet_to_state(
        &self,
        packet: GossipPacket,
    ) -> impl Future<Output = Result<Option<Timestamp>, ActionsError>>;
}

impl<S: Scheme> ActionsPassive for BeaconProcess<S> {
    async fn packet(&self, packet: GossipPacket) -> Result<Option<Timestamp>, ActionsError> {
        // TODO(not confirmed): if we're in the DKG protocol phase,
        // we automatically broadcast it as it shouldn't update state.
        self.apply_packet_to_state(packet).await
    }

    async fn apply_packet_to_state(
        &self,
        packet: GossipPacket,
    ) -> Result<Option<Timestamp>, ActionsError> {
        let mut state = self.dkg_store().get_last_succesful(self.id())?;
        let me = self.as_participant()?;

        // We must verify the message against the next state,
        // as the current state upon first proposal will be empty.
        // Packet data is moved into state, for this reason packet is cloned.
        state
            .apply(&me, packet.clone())
            .map_err(ActionsError::DBState)?;

        self.verify_msg(&packet, &state).await?;
        self.dkg_store().save_current(&state)?;

        Ok(packet.data.get_execute())
    }
}
