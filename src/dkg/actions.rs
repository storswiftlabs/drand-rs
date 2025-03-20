use super::process::SeenPackets;
use super::state::State;
use super::utils::GossipAuth;
use crate::key::Scheme;

use crate::protobuf::dkg::DkgStatusResponse;
use crate::protobuf::dkg::JoinOptions;
use crate::transport::dkg::Command;
use crate::transport::dkg::GossipPacket;

use std::future::Future;

/// Contains all the DKG actions that require user interaction: creating a network,
/// accepting or rejecting a DKG, getting the status, etc. Both leader and follower interactions are contained herein.
pub(super) trait ActionsActive {
    fn command<S: Scheme>(&self, cmd: Command) -> impl Future<Output = Result<(), ActionsError>>;
    fn dkg_status<S: Scheme>(&self) -> Result<DkgStatusResponse, ActionsError>;
    fn start_join<S: Scheme>(
        &self,
        state: State<S>,
        options: JoinOptions,
    ) -> impl Future<Output = Result<(), ActionsError>>;
}

/// Contains all internal messaging between nodes triggered by the protocol - things it does automatically
/// upon receiving messages from other nodes: storing proposals, aborting when the leader aborts, etc
pub(super) trait ActionsPassive {
    fn packet<S: Scheme>(
        &self,
        packet: GossipPacket,
        seen: &mut SeenPackets,
    ) -> impl Future<Output = Result<(), ActionsError>>;

    fn apply_packet_to_state<S: Scheme>(
        &self,
        packet: GossipPacket,
    ) -> impl Future<Output = Result<(), ActionsError>>;
}

/// Contains logic for signing and validation packets
pub(super) trait ActionsSigning {
    fn verify_msg<S: Scheme>(
        &self,
        packet: &GossipPacket,
        state: &State<S>,
    ) -> impl Future<Output = Result<(), ActionsError>>;

    fn msg_for_signing(&self, packet: &GossipPacket, enc_state: &[u8]) -> Vec<u8> {
        let mut msg = packet.encode();
        msg.extend_from_slice(enc_state);

        msg
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ActionsError {
    #[error("db state: {0}")]
    DBState(#[from] crate::dkg::state::DBStateError),
    #[error("dkg store: {0}")]
    DKGStore(#[from] crate::dkg::store::DkgStoreError),
    #[error("participant is not found")]
    MissingParticipant,
    #[error("invalid packet signature")]
    InvalidSignature,
    #[error("gossip packet signature is too short")]
    GossipSignatureLen,
    #[error("TODO: this dkg action is not implemented yet")]
    TODO,
}
