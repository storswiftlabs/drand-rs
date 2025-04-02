use super::actions_signing::ActionsSigning;
use super::process::SeenPackets;
use super::ActionsError;

use crate::core::beacon::BeaconProcess;
use crate::key::Scheme;
use crate::transport::dkg::GossipPacket;

use std::future::Future;
use tracing::error;

/// Contains all internal messaging between nodes triggered by the protocol - things it does automatically
/// upon receiving messages from other nodes: storing proposals, aborting when the leader aborts, etc
pub trait ActionsPassive {
    fn packet(
        &self,
        packet: GossipPacket,
        seen: &mut SeenPackets,
    ) -> impl Future<Output = Result<(), ActionsError>>;

    fn apply_packet_to_state(
        &self,
        packet: GossipPacket,
    ) -> impl Future<Output = Result<(), ActionsError>>;
}

impl<S: Scheme> ActionsPassive for BeaconProcess<S> {
    async fn packet(
        &self,
        packet: GossipPacket,
        seen: &mut SeenPackets,
    ) -> Result<(), ActionsError> {
        // Ignore duplicate/incorrect packets
        if !seen.is_new_packet(&packet)? {
            return Ok(());
        }

        // TODO: if we're in the DKG protocol phase, we automatically broadcast it as it shouldn't update state
        self.apply_packet_to_state(packet).await?;
        // TODO: if packet.GetExecute(){..}

        Ok(())
    }

    async fn apply_packet_to_state(&self, packet: GossipPacket) -> Result<(), ActionsError> {
        let mut state = self.dkg_store().get_last_succesful(self.id())?;
        let me = self.as_participant()?;

        state.apply(&me, packet.clone()).await?;

        if let Err(err) = self.verify_msg(&packet, &state).await {
            error!(
                "failed to verify message, BeaconID: {}, error: {err}",
                self.id()
            );
            return Err(err);
        }

        self.dkg_store().save_current(&state)?;

        Ok(())
    }
}
