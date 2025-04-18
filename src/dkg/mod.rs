pub mod actions_active;
pub mod actions_passive;
pub mod actions_signing;
pub mod broadcast;
pub(super) mod execution;
pub mod state;
pub mod status;
pub(super) mod store;
pub(super) mod utils;

pub use energon::kyber::dkg::Node as DkgNode;

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
    #[error("dkg config: failed to create new dkg nodes from participants")]
    ParticipantsToNewNodes,
    #[error("dkg protocol error: {0}")]
    DkgError(energon::kyber::dkg::dkg::DkgError),
    #[error("dkg protocol is not running")]
    ProtocolIsNotRunning,
    #[error("dkg protocol already running")]
    ProtocolAlreadyRunning,
    #[error("failed to deserialize bundle from proto")]
    InvalidProtoBundle,
    #[error("unknown start execution time - input is not canonical")]
    StartExecutionTimeNotCanonical,
    #[error("received start execution time must be in future")]
    StartExecutionTimeIsPassed,
    #[error("TODO: this dkg action is not implemented yet")]
    TODO,
}
