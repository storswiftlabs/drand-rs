// Copyright 2023-2025 StorSwift Inc.
// SPDX-License-Identifier: Apache-2.0

pub mod actions_active;
pub mod actions_passive;
pub mod actions_signing;
pub mod broadcast;
pub mod execution;
pub mod state;
pub mod status;
pub mod store;
pub mod utils;

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
    #[error("failed to initialize participant")]
    IntoParticipant,
    #[error("dkg config: failed to create new dkg nodes from participants")]
    ParticipantsToNewNodes,
    #[error("dkg protocol error: {0}")]
    DkgError(energon::kyber::dkg::DkgError),
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
    #[error("group file required to join after the first epoch")]
    GroupfileIsMissing,
    #[error("failed to parse group file to join after the first epoch")]
    GroupFileParse,
    #[error("reshare: previous group can not be empty")]
    ResharePrevGroupRequired,
    #[error("reshare: previous share can not be empty")]
    ResharePrevShareRequired,
    #[error("leader action is not supported")]
    NotSupported,
}
