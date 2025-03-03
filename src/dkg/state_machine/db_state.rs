use super::status::StateError;
use super::status::Status;

use crate::core::beacon::BeaconID;
use crate::key::group::minimum_t;
use crate::key::group::Group;
use crate::key::keys::Identity;
use crate::key::keys::Share;
use crate::key::ConversionError;
use crate::key::Scheme;

use crate::protobuf::dkg::DkgEntry;
use crate::protobuf::dkg::DkgStatusResponse;
use crate::protobuf::dkg::Participant;
use crate::transport::dkg::GossipMetadata;
use crate::transport::dkg::ProposalTerms;
use crate::transport::dkg::Timestamp;

use crate::net::utils::Seconds;
use std::time::SystemTime;
use tracing::debug;

#[derive(thiserror::Error, Debug)]
pub enum DBStateError {
    #[error("proposal terms cannot be empty")]
    MissingTerms,
    #[error("timeout has been reached")]
    TimeoutReached,
    #[error("BeaconID was invalid")]
    InvalidBeaconID,
    #[error("the scheme proposed does not exist")]
    InvalidScheme,
    #[error("genesis time cannot be changed after the initial DKG")]
    GenesisTimeNotEqual,
    #[error("the genesis seed is created during the first epoch, so you can't provide it in the proposal")]
    NoGenesisSeedForFirstEpoch,
    #[error("the genesis time in the group file provided did not match the one from the proposal")]
    GenesisTimeNotConsistentWithProposal,
    #[error("genesis seed cannot change after the first epoch")]
    GenesisSeedCannotChange,
    #[error("you must include yourself in a proposal")]
    SelfMissingFromProposal,
    #[error("you cannot join a proposal in which you are not a joiner")]
    CannotJoinIfNotInJoining,
    #[error("joining after the first epoch requires a previous group file")]
    JoiningAfterFirstEpochNeedsGroupFile,
    #[error("the epoch provided was invalid")]
    InvalidEpoch,
    #[error("you cannot lead a DKG and join at the same time (unless it is epoch 1)")]
    LeaderCantJoinAfterFirstEpoch,
    #[error("you cannot lead a DKG and leave at the same time")]
    LeaderNotRemaining,
    #[error("the leader must join in the first epoch")]
    LeaderNotJoining,
    #[error("participants can only be joiners for the first epoch")]
    OnlyJoinersAllowedForFirstEpoch,
    #[error("cannot propose a network without nodes remaining")]
    NoNodesRemaining,
    #[error("some node(s) in the current epoch are missing from the proposal - they should be remaining or leaving")]
    MissingNodesInProposal,
    #[error("cannot make a proposal where you are not the leader")]
    CannotProposeAsNonLeader,
    #[error("the threshold cannot be higher than the count of remaining + joining nodes")]
    ThresholdHigherThanNodeCount,
    #[error("the new node count cannot be lower than the prior threshold")]
    NodeCountTooLow,
    #[error("the threshold is below the minimum required to allow effective secret recovery given the node count")]
    ThresholdTooLow,
    #[error("remaining and leaving nodes contained a node that does not exist in the current epoch - they must be added as joiners")]
    RemainingAndLeavingNodesMustExistInCurrentEpoch,
    #[error("you cannot accept a proposal where your node is leaving")]
    CannotAcceptProposalWhereLeaving,
    #[error(
        "you cannot accept a proposal where your node is joining - run the join command instead"
    )]
    CannotAcceptProposalWhereJoining,
    #[error("you cannot reject a proposal where your node is leaving")]
    CannotRejectProposalWhereLeaving,
    #[error("you cannot reject a proposal where your node is joining (just turn your node off)")]
    CannotRejectProposalWhereJoining,
    #[error("you cannot execute leave if you were not included as a leaver in the proposal")]
    CannotLeaveIfNotALeaver,
    #[error("only the leader can trigger the execution")]
    OnlyLeaderCanTriggerExecute,
    #[error("only the leader can remotely abort the DKG")]
    OnlyLeaderCanRemoteAbort,
    #[error("you cannot start execution if you are not a remainer or joiner to the DKG")]
    CannotExecuteIfNotJoinerOrRemainer,
    #[error("somebody unknown tried to accept the proposal")]
    UnknownAcceptor,
    #[error("this participant already accepted the proposal")]
    DuplicateAcceptance,
    #[error("the node that signed this message is not the one claiming be accepting")]
    InvalidAcceptor,
    #[error("the node that signed this message is not the one claiming be rejecting")]
    InvalidRejector,
    #[error("somebody unknown tried to reject the proposal")]
    UnknownRejector,
    #[error("this participant already rejected the proposal")]
    DuplicateRejection,
    #[error("you cannot complete a DKG with a nil final group")]
    FinalGroupCannotBeEmpty,
    #[error("you cannot complete a DKG with a nil key share")]
    KeyShareCannotBeEmpty,
    #[error("received acceptance but not during proposal phase")]
    ReceivedAcceptance,
    #[error("received rejection but not during proposal phase")]
    ReceivedRejection,
    #[error("dkg state error: {0}")]
    InvalidStateChange(#[from] StateError),
    #[error("conversion: {0}")]
    ConversionError(#[from] ConversionError),
    #[error("the key's scheme may not match the beacon's scheme")]
    InvalidKeyScheme,
}

#[derive(PartialEq)]
pub struct DBState<S: Scheme> {
    // Parameters
    beacon_id: BeaconID,
    epoch: u32,
    state: Status,
    threshold: u32,
    timeout: Timestamp,
    genesis_time: Timestamp,
    genesis_seed: Vec<u8>,
    catchup_period: Seconds,
    beacon_period: Seconds,
    leader: Option<Identity<S>>,
    // Participants
    remaining: Vec<Identity<S>>,
    joining: Vec<Identity<S>>,
    leaving: Vec<Identity<S>>,
    acceptors: Vec<Identity<S>>,
    rejectors: Vec<Identity<S>>,
    // Result
    final_group: Option<Group<S>>,
    key_share: Option<Share<S>>,
}

impl<S: Scheme> DBState<S> {
    pub fn id(&self) -> &BeaconID {
        &self.beacon_id
    }

    pub fn show_status(&self) -> &super::status::Status {
        &self.state
    }

    pub fn validate_joiner_signatures(&self) -> Result<(), DBStateError> {
        for joiner in &self.joining {
            if !joiner.is_valid_signature() {
                return Err(DBStateError::InvalidKeyScheme);
            }
        }

        Ok(())
    }

    /// Returns default DBState representation which is used only at fresh state.
    pub fn new_fresh(beacon_id: &BeaconID) -> Self {
        Self {
            beacon_id: beacon_id.to_owned(),
            genesis_time: Timestamp {
                seconds: -62135596800,
                nanos: 0,
            },
            epoch: 0,
            state: Status::Fresh,
            threshold: 0,
            timeout: Timestamp::default(),
            genesis_seed: vec![],
            catchup_period: Seconds::default(),
            beacon_period: Seconds::default(),
            leader: None,
            remaining: vec![],
            joining: vec![],
            leaving: vec![],
            acceptors: vec![],
            rejectors: vec![],
            final_group: None,
            key_share: None,
        }
    }

    /// TODO: this method should make request to dkg.db
    pub fn status(&self) -> Result<DkgStatusResponse, DBStateError> {
        let DBState::<S> {
            beacon_id,
            epoch,
            state,
            threshold,
            timeout,
            genesis_time,
            genesis_seed,
            catchup_period: _,
            beacon_period: _,
            leader,
            remaining,
            joining,
            leaving,
            acceptors,
            rejectors,
            final_group,
            key_share: _,
        } = self;

        let leader = if state == &Status::Fresh {
            None
        } else {
            Some(
                leader
                    .as_ref()
                    .ok_or(DBStateError::LeaderNotJoining)?
                    .try_into()?,
            )
        };

        let final_group = match final_group {
            Some(group) => group
                .nodes()
                .iter()
                .map(|node| node.public().address().to_string())
                .collect(),
            None => vec![],
        };

        let entry = DkgStatusResponse {
            // TODO: Complete entry should be updated if dkg is finished.
            complete: None,
            current: Some(DkgEntry {
                beacon_id: beacon_id.to_string(),
                state: *state as u32,
                epoch: *epoch,
                threshold: *threshold,
                timeout: Some(*timeout),
                genesis_time: Some(*genesis_time),
                genesis_seed: genesis_seed.to_owned(),
                leader,
                remaining: into_participants(remaining)?,
                joining: into_participants(joining)?,
                leaving: into_participants(leaving)?,
                acceptors: into_participants(acceptors)?,
                rejectors: into_participants(rejectors)?,
                final_group,
            }),
        };

        Ok(entry)
    }

    /// Proposed is used by non-leader nodes to set their own state when they receive a proposal
    pub fn proposed(
        &mut self,
        me: &Identity<S>,
        terms: ProposalTerms,
        metadata: &GossipMetadata,
    ) -> Result<(), DBStateError> {
        self.state.is_valid_state_change(Status::Proposed)?;

        // it's important to verify that the sender (and by extension the signature of the sender)
        // is the same as the proposed leader, to avoid nodes trying to propose DKGs on behalf of somebody else
        let sender = metadata.address();
        if sender != &terms.leader.address {
            return Err(DBStateError::CannotProposeAsNonLeader);
        }

        let time_now = Timestamp::from(SystemTime::now());
        if time_now.seconds >= terms.timeout.seconds && time_now.nanos >= terms.timeout.nanos {
            return Err(DBStateError::TimeoutReached);
        }

        let node_count = terms.joining.len() + terms.remaining.len();
        let threshold = terms.threshold as usize;

        if threshold > node_count {
            return Err(DBStateError::ThresholdHigherThanNodeCount);
        }

        if threshold < minimum_t(node_count) {
            return Err(DBStateError::ThresholdTooLow);
        }

        // epochs should be monotonically increasing
        if terms.epoch < self.epoch {
            return Err(DBStateError::InvalidEpoch);
        }

        // aborted or timed out DKGs can be reattempted at the same epoch
        if terms.epoch == self.epoch
            && self.state != Status::Aborted
            && self.state != Status::TimedOut
            && self.state != Status::Failed
        {
            return Err(DBStateError::InvalidEpoch);
        }

        // if we have some leftover state after having left the network, we can accept higher epochs
        if terms.epoch > self.epoch + 1
            && (self.state != Status::Left && self.state != Status::Fresh)
        {
            return Err(DBStateError::InvalidEpoch);
        }

        // some terms (such as genesis seed) get set during the first epoch
        // additionally, we can't have remainers, `GenesisTime` == `TransitionTime`, amongst other things
        if terms.epoch == 1 {
            validate_first_epoch(&terms)?;
        };

        let proposed = Self::try_from(terms)?;

        // Local identity should be present in received proposal
        if !proposed.joining.contains(me)
            && !proposed.remaining.contains(me)
            && !proposed.leaving.contains(me)
        {
            return Err(DBStateError::SelfMissingFromProposal);
        }

        debug!("received proposal is valid");
        *self = proposed;

        Ok(())
    }
}

fn validate_first_epoch(terms: &ProposalTerms) -> Result<(), DBStateError> {
    if !terms.genesis_seed.is_empty() {
        return Err(DBStateError::NoGenesisSeedForFirstEpoch);
    }

    if !terms.remaining.is_empty() || !terms.leaving.is_empty() {
        return Err(DBStateError::OnlyJoinersAllowedForFirstEpoch);
    }

    if !terms
        .joining
        .iter()
        .any(|identity| identity == &terms.leader)
    {
        return Err(DBStateError::LeaderNotJoining);
    }

    if terms.joining.len() < terms.threshold as usize {
        return Err(DBStateError::ThresholdHigherThanNodeCount);
    }

    Ok(())
}

impl<S: Scheme> TryFrom<ProposalTerms> for DBState<S> {
    type Error = ConversionError;

    fn try_from(value: ProposalTerms) -> Result<Self, Self::Error> {
        let ProposalTerms {
            beacon_id,
            epoch,
            leader,
            threshold,
            timeout,
            catchup_period_seconds,
            beacon_period_seconds,
            scheme_id: _,
            genesis_time,
            genesis_seed,
            joining,
            remaining,
            leaving,
        } = value;

        let state = DBState::<S> {
            beacon_id: BeaconID::new(beacon_id),
            epoch,
            state: Status::Proposed,
            threshold,
            timeout,
            genesis_time,
            genesis_seed,
            catchup_period: catchup_period_seconds,
            beacon_period: beacon_period_seconds,
            leader: Some(leader.try_into()?),
            remaining: from_vec(remaining)?,
            joining: from_vec(joining)?,
            leaving: from_vec(leaving)?,
            acceptors: vec![],
            rejectors: vec![],
            final_group: None,
            key_share: None,
        };

        Ok(state)
    }
}

/// Abstract mapping raw data into a corresponding generic type
#[inline(always)]
fn from_vec<S: Scheme>(
    v: Vec<crate::transport::drand::Identity>,
) -> Result<Vec<Identity<S>>, ConversionError> {
    v.into_iter().map(TryInto::try_into).collect()
}

// &[Identity<S>] -> Vec<proto::Participant>
#[inline(always)]
fn into_participants<S: Scheme>(
    participants: &[Identity<S>],
) -> Result<Vec<Participant>, ConversionError> {
    participants.iter().map(TryInto::try_into).collect()
}
