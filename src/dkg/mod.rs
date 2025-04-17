pub mod actions_active;
pub mod actions_passive;
pub mod actions_signing;
#[allow(dead_code)]
pub mod broadcast;
pub mod process;
pub mod state;
pub mod status;
pub(super) mod store;
pub(super) mod utils;

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
    #[error("failed to initialize participant")]
    IntoParticipant,
    #[error("TODO: this dkg action is not implemented yet")]
    TODO,
}
