use super::{
    actions_signing::{enc_participant, enc_timestamp, GossipAuth},
    status::{StateError, Status},
};
use crate::{
    key::{
        group::{minimum_t, Group},
        toml::Toml,
        PointSerDeError, Scheme,
    },
    net::utils::{Address, Seconds},
    transport::dkg::{
        GossipData, GossipMetadata, GossipPacket, Participant, ProposalTerms, Timestamp,
    },
};
use energon::kyber::dkg::DistKeyShare;
use std::{str::FromStr, time::SystemTime};
use toml_edit::{ArrayOfTables, DocumentMut, Item, Table};

#[allow(dead_code, reason = "subset of errors is reserved for Leader logic")]
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
    #[error("the epoch provided was invalid for leftover state")]
    InvalidEpochLeftover,
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
    ConversionError(#[from] PointSerDeError),
    #[error("invalid signature of participant")]
    ParticipantSignature,
    #[error("final group for remainers can not be empty")]
    MissingFinalGroupForRemainers,
}

#[derive(PartialEq)]
pub struct State<S: Scheme> {
    // Parameters
    pub beacon_id: String,
    pub epoch: u32,
    pub status: Status,
    pub threshold: u32,
    pub timeout: Timestamp,
    pub genesis_time: Timestamp,
    pub genesis_seed: Vec<u8>,
    pub catchup_period: Seconds,
    pub beacon_period: Seconds,
    pub leader: Participant,
    // Participants
    pub remaining: Vec<Participant>,
    pub joining: Vec<Participant>,
    leaving: Vec<Participant>,
    acceptors: Vec<Participant>,
    rejectors: Vec<Participant>,

    pub final_group: Option<Group<S>>,
    pub key_share: Option<DistKeyShare<S>>,
}

impl Toml for Participant {
    type Inner = Table;

    fn toml_encode(&self) -> Option<Self::Inner> {
        let mut table = Self::Inner::new();
        let _ = table.insert("Address", self.address.as_str().into());
        let _ = table.insert("Key", hex::encode(&self.key).into());
        let _ = table.insert("Signature", hex::encode(&self.signature).into());

        Some(table)
    }

    fn toml_decode(table: &Self::Inner) -> Option<Self> {
        let address = Address::precheck(table.get("Address")?.as_str()?).ok()?;
        let key = hex::decode(table.get("Key")?.as_str()?).ok()?;
        let signature = hex::decode(table.get("Signature")?.as_str()?).ok()?;

        Some(Self {
            address,
            key,
            signature,
        })
    }
}

impl<S: Scheme> Toml for State<S> {
    type Inner = DocumentMut;

    fn toml_encode(&self) -> Option<Self::Inner> {
        fn to_array(items: &[Participant]) -> Option<ArrayOfTables> {
            let mut array = ArrayOfTables::new();
            for i in items {
                array.push(i.toml_encode()?);
            }
            Some(array)
        }

        let mut doc = Self::Inner::new();
        doc.insert("BeaconID", self.beacon_id.as_str().into());
        doc.insert("State", self.status.to_string().into());

        // Do not store default values at Fresh state. Decoding is simplified accordingly.
        if self.status == Status::Fresh {
            return Some(doc);
        }
        doc.insert("Epoch", i64::from(self.epoch).into());
        doc.insert("Threshold", i64::from(self.threshold).into());
        doc.insert("Timeout", (self.timeout.to_string()).into());
        doc.insert("GenesisTime", (self.genesis_time.to_string()).into());
        doc.insert("GenesisSeed", hex::encode(&self.genesis_seed).into());
        doc.insert("CatchupPeriod", self.catchup_period.to_string().into());
        doc.insert("BeaconPeriod", self.beacon_period.to_string().into());
        doc.insert("Leader", Item::Table(self.leader.toml_encode()?));
        doc.insert("Remaining", Item::ArrayOfTables(to_array(&self.remaining)?));
        doc.insert("Joining", Item::ArrayOfTables(to_array(&self.joining)?));
        doc.insert("Leaving", Item::ArrayOfTables(to_array(&self.leaving)?));
        doc.insert("Acceptors", Item::ArrayOfTables(to_array(&self.acceptors)?));
        doc.insert("Rejectors", Item::ArrayOfTables(to_array(&self.rejectors)?));
        doc.insert(
            "FinalGroup",
            match &self.final_group {
                Some(group) => group.toml_encode()?.as_item().into(),
                None => Item::None,
            },
        );
        doc.insert(
            "KeyShare",
            match &self.key_share {
                Some(share) => share.toml_encode()?.as_item().into(),
                None => Item::None,
            },
        );

        Some(doc)
    }

    fn toml_decode(table: &Self::Inner) -> Option<Self> {
        fn from_array(role: &str, table: &Table) -> Option<Vec<Participant>> {
            match table.get(role) {
                Some(item) => item
                    .as_array_of_tables()?
                    .iter()
                    .map(Participant::toml_decode)
                    .collect::<Option<Vec<_>>>(),
                None => Some(vec![]),
            }
        }

        let beacon_id = table.get("BeaconID")?.as_str()?;
        let state = Status::from_str(table.get("State")?.as_str()?).ok()?;
        if state == Status::Fresh {
            // Other values are default.
            return Some(Self::fresh(beacon_id));
        }

        let catchup_period = table
            .get("CatchupPeriod")?
            .as_str()
            .map(Seconds::from_str)?
            .ok()?;

        let beacon_period = table
            .get("BeaconPeriod")?
            .as_str()
            .map(Seconds::from_str)?
            .ok()?;

        // Missing `Group` and `Share` is not an error at this layer.
        let final_group = match table.get("FinalGroup") {
            Some(item) => Group::toml_decode(&item.as_table()?.to_owned().into()),
            None => None,
        };

        let key_share = match table.get("KeyShare") {
            Some(item) => DistKeyShare::toml_decode(&item.as_table()?.to_owned().into()),
            None => None,
        };

        Some(Self {
            beacon_id: beacon_id.into(),
            epoch: u32::try_from(table.get("Epoch")?.as_integer()?).ok()?,
            status: state,
            catchup_period,
            beacon_period,
            threshold: u32::try_from(table.get("Threshold")?.as_integer()?).ok()?,
            timeout: Timestamp::from_str(table.get("Timeout")?.as_str()?).ok()?,
            genesis_time: Timestamp::from_str(table.get("GenesisTime")?.as_str()?).ok()?,
            genesis_seed: table.get("GenesisSeed")?.as_str().map(hex::decode)?.ok()?,
            leader: Participant::toml_decode(table.get("Leader")?.as_table()?)?,
            remaining: from_array("Remaining", table)?,
            joining: from_array("Joining", table)?,
            leaving: from_array("Leaving", table)?,
            acceptors: from_array("Acceptors", table)?,
            rejectors: from_array("Rejectors", table)?,
            final_group,
            key_share,
        })
    }
}

impl<S: Scheme> GossipAuth for State<S> {
    fn encode(&self) -> Vec<u8> {
        let mut ret = [
            "Proposal:\n".as_bytes(),
            self.beacon_id.as_bytes(),
            "\n".as_bytes(),
            &self.epoch.to_le_bytes(),
            "\nLeader:".as_bytes(),
            self.leader.address.as_str().as_bytes(),
            "\n".as_bytes(),
            &self.leader.signature,
            &self.threshold.to_le_bytes(),
            &enc_timestamp(self.timeout),
            &self.catchup_period.get_value().to_le_bytes(),
            &self.beacon_period.get_value().to_le_bytes(),
            "\nScheme: ".as_bytes(),
            S::ID.as_bytes(),
            "\n".as_bytes(),
            &enc_timestamp(self.genesis_time),
        ]
        .concat();

        for j in &self.joining {
            ret.extend_from_slice(&enc_participant("\nJoiner:", j));
        }
        for r in &self.remaining {
            ret.extend_from_slice(&enc_participant("\nRemainer:", r));
        }
        for l in &self.leaving {
            ret.extend_from_slice(&enc_participant("\nLeaver:", l));
        }

        ret
    }
}

impl<S: Scheme> State<S> {
    pub fn epoch(&self) -> u32 {
        self.epoch
    }

    pub fn status(&self) -> &super::status::Status {
        &self.status
    }

    /// Fresh is default state representation.
    pub fn fresh(beacon_id: &str) -> Self {
        Self {
            beacon_id: beacon_id.to_string(),
            genesis_time: Timestamp {
                seconds: -62135596800,
                nanos: 0,
            },
            epoch: 0,
            status: Status::Fresh,
            threshold: 0,
            timeout: Timestamp::default(),
            genesis_seed: vec![],
            catchup_period: Seconds::default(),
            beacon_period: Seconds::default(),
            leader: Participant::default(),
            remaining: vec![],
            joining: vec![],
            leaving: vec![],
            acceptors: vec![],
            rejectors: vec![],
            final_group: None,
            key_share: None,
        }
    }

    /// Proposed is used by non-leader nodes to set their own state when they receive a proposal
    pub fn proposed(
        &mut self,
        me: &Participant,
        terms: ProposalTerms,
        metadata: &GossipMetadata,
    ) -> Result<(), DBStateError> {
        self.status.is_valid_state_change(Status::Proposed)?;

        // it's important to verify that the sender (and by extension the signature of the sender)
        // is the same as the proposed leader, to avoid nodes trying to propose DKGs on behalf of somebody else
        let sender = metadata.address();
        if sender != &terms.leader.address {
            return Err(DBStateError::CannotProposeAsNonLeader);
        }

        validate_proposal(self, &terms)?;

        // if I've received a proposal, I must surely be in it!
        if !terms.joining.contains(me)
            && !terms.remaining.contains(me)
            && !terms.leaving.contains(me)
        {
            return Err(DBStateError::SelfMissingFromProposal);
        }

        let proposed = Self::try_from(terms)?;
        *self = proposed;

        Ok(())
    }

    pub fn joined(
        &mut self,
        me: &Participant,
        prev_group: Option<Group<S>>,
    ) -> Result<(), DBStateError> {
        self.status.is_valid_state_change(Status::Joined)?;

        if Timestamp::from(SystemTime::now()).seconds >= self.timeout.seconds {
            return Err(DBStateError::TimeoutReached);
        }

        if !self.joining.contains(me) {
            return Err(DBStateError::CannotJoinIfNotInJoining);
        }

        validate_previous_group_for_joiners(self, prev_group.as_ref())?;

        self.final_group = prev_group;
        self.status = Status::Joined;

        Ok(())
    }

    pub fn apply(&mut self, me: &Participant, packet: GossipPacket) -> Result<(), DBStateError> {
        let metadata = &packet.metadata;

        match packet.data {
            GossipData::Proposal(terms) => self.proposed(me, terms, metadata),
            GossipData::Execute(_execute) => self.executing(me, metadata),
            GossipData::Accept(accept) => self.received_acceptance(accept.acceptor, metadata),
            GossipData::Abort(_abort_dkg) => self.aborted(metadata),
            // TODO: add before joining mainnet
            GossipData::Reject(_reject_proposal) => Ok(()),
            // DKG packets are not gossiped in Drand-go
            GossipData::Dkg(_dkg_packet) => Ok(()),
        }
    }

    pub fn executing(
        &mut self,
        me: &Participant,
        metadata: &GossipMetadata,
    ) -> Result<(), DBStateError> {
        if self.time_expired() {
            return Err(DBStateError::TimeoutReached);
        }

        if self.leaving.contains(me) {
            return self.left(me);
        }
        self.status.is_valid_state_change(Status::Executing)?;

        if &self.leader.address != metadata.address() {
            return Err(DBStateError::OnlyLeaderCanTriggerExecute);
        }

        self.status = Status::Executing;

        Ok(())
    }

    pub fn left(&mut self, me: &Participant) -> Result<(), DBStateError> {
        self.status.is_valid_state_change(Status::Left)?;

        if self.time_expired() {
            return Err(DBStateError::TimeoutReached);
        }

        if !self.leaving.contains(me) && !self.joining.contains(me) {
            return Err(DBStateError::CannotLeaveIfNotALeaver);
        }

        self.status = Status::Left;

        Ok(())
    }

    /// Timeout check operates at resolution of seconds.
    pub(super) fn time_expired(&self) -> bool {
        Timestamp::from(SystemTime::now()).seconds >= self.timeout.seconds
    }

    pub(super) fn complete(
        &mut self,
        final_group: Group<S>,
        share: DistKeyShare<S>,
    ) -> Result<(), DBStateError> {
        self.status.is_valid_state_change(Status::Complete)?;

        if self.time_expired() {
            return Err(DBStateError::TimeoutReached);
        }

        self.status = Status::Complete;
        if self.genesis_seed.is_empty() {
            self.genesis_seed
                .extend_from_slice(&final_group.genesis_seed);
        }
        self.final_group = Some(final_group);
        self.key_share = Some(share);

        Ok(())
    }

    /// `ReceivedAcceptance` is used by nodes when they receive a gossiped acceptance packet
    /// they needn't necessarily collect _all_ acceptances for executing, but it gives them some insight into
    /// the state of the DKG when they run the status command.
    fn received_acceptance(
        &mut self,
        them: Participant,
        metadata: &GossipMetadata,
    ) -> Result<(), DBStateError> {
        if !is_proposal_phase(self) {
            return Err(DBStateError::ReceivedAcceptance);
        }

        if !self.remaining.iter().any(|r| *r == them) {
            return Err(DBStateError::UnknownAcceptor);
        }

        if self.acceptors.iter().any(|a| *a == them) {
            return Err(DBStateError::DuplicateAcceptance);
        }

        if metadata.address != them.address {
            return Err(DBStateError::InvalidAcceptor);
        }

        self.rejectors.retain(|r| r != &them);
        self.acceptors.push(them);

        Ok(())
    }

    pub(super) fn accepted(&mut self, me: Participant) -> Result<(), DBStateError> {
        self.status.is_valid_state_change(Status::Accepted)?;

        if self.time_expired() {
            return Err(DBStateError::TimeoutReached);
        }
        // Leavers get no say if the rest of the network wants them out
        if self.leaving.contains(&me) {
            return Err(DBStateError::CannotAcceptProposalWhereLeaving);
        }
        // Joiners should run the `Join` command instead
        if self.joining.contains(&me) {
            return Err(DBStateError::CannotAcceptProposalWhereJoining);
        }

        // Move our node from rejectors to acceptors
        self.rejectors.retain(|i| i != &me);
        self.acceptors.push(me);
        self.status = Status::Accepted;

        Ok(())
    }

    pub(super) fn aborted(&mut self, metadata: &GossipMetadata) -> Result<(), DBStateError> {
        self.status.is_valid_state_change(Status::Aborted)?;

        if self.leader.address != metadata.address {
            return Err(DBStateError::OnlyLeaderCanRemoteAbort);
        }

        self.status = Status::Aborted;

        Ok(())
    }

    pub(super) fn rejected(&mut self, me: Participant) -> Result<(), DBStateError> {
        self.status.is_valid_state_change(Status::Rejected)?;

        if self.time_expired() {
            return Err(DBStateError::TimeoutReached);
        }

        // Joiners should just not run the `Join` command if they don't want to join
        if self.joining.contains(&me) {
            return Err(DBStateError::CannotRejectProposalWhereJoining);
        }

        // Leavers get no say if the rest of the network wants them out
        if self.leaving.contains(&me) {
            return Err(DBStateError::CannotRejectProposalWhereLeaving);
        }

        self.acceptors.retain(|i| i != &me);
        self.rejectors.push(me);
        self.status = Status::Rejected;

        Ok(())
    }
}

fn validate_proposal<S: Scheme>(
    current: &State<S>,
    terms: &ProposalTerms,
) -> Result<(), DBStateError> {
    validate_for_all_dkgs(current, terms)?;

    // Some terms (such as genesis seed) get set during the first epoch
    // additionally, we can't have remainers, `GenesisTime` == `TransitionTime`, amongst other things
    if terms.epoch == 1 {
        return validate_first_epoch(terms);
    };

    validate_reshare_terms(current, terms)?;

    if current.status != Status::Fresh {
        return validate_reshare_for_remainers(current, terms);
    }

    Ok(())
}

fn validate_reshare_for_remainers<S: Scheme>(
    current: &State<S>,
    terms: &ProposalTerms,
) -> Result<(), DBStateError> {
    if terms.genesis_time != current.genesis_time {
        return Err(DBStateError::GenesisTimeNotEqual);
    }

    if terms.genesis_seed != current.genesis_seed {
        return Err(DBStateError::GenesisSeedCannotChange);
    }

    let final_group = current
        .final_group
        .as_ref()
        .ok_or(DBStateError::MissingFinalGroupForRemainers)?;

    let last_epoch_addresses: Vec<&str> = final_group
        .nodes
        .iter()
        .map(|p| p.public().address())
        .collect();

    let terms_remaining_addresses: Vec<&str> =
        terms.remaining.iter().map(|p| p.address.as_str()).collect();
    let terms_leaving_addresses: Vec<&str> =
        terms.leaving.iter().map(|p| p.address.as_str()).collect();

    if !last_epoch_addresses.iter().all(|addr| {
        terms_remaining_addresses.contains(addr) || terms_leaving_addresses.contains(addr)
    }) {
        return Err(DBStateError::RemainingAndLeavingNodesMustExistInCurrentEpoch);
    }

    if !terms_remaining_addresses
        .iter()
        .all(|addr| last_epoch_addresses.contains(addr))
        && terms_leaving_addresses
            .iter()
            .all(|addr| last_epoch_addresses.contains(addr))
    {
        return Err(DBStateError::MissingNodesInProposal);
    }

    if terms.remaining.len() < current.threshold as usize {
        return Err(DBStateError::NodeCountTooLow);
    }

    Ok(())
}

fn validate_reshare_terms<S: Scheme>(
    current: &State<S>,
    terms: &ProposalTerms,
) -> Result<(), DBStateError> {
    if terms.remaining.is_empty() {
        return Err(DBStateError::NoNodesRemaining);
    }

    if terms.joining.iter().any(|p| *p == terms.leader) {
        return Err(DBStateError::LeaderCantJoinAfterFirstEpoch);
    }

    // There's no theoretical reason the leader can't be leaving, but from a practical perspective
    // it makes sense in case e.g. the DKG fails or aborts
    if terms.leaving.iter().any(|p| *p == terms.leader)
        || !terms.remaining.iter().any(|p| *p == terms.leader)
    {
        return Err(DBStateError::LeaderNotRemaining);
    }

    if terms.remaining.len() < current.threshold as usize {
        return Err(DBStateError::NodeCountTooLow);
    }

    Ok(())
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

fn validate_for_all_dkgs<S: Scheme>(
    current: &State<S>,
    terms: &ProposalTerms,
) -> Result<(), DBStateError> {
    if current.beacon_id != terms.beacon_id {
        return Err(DBStateError::InvalidBeaconID);
    }
    // Validate joiner signatures.
    for j in &terms.joining {
        if !j.is_valid_signature::<S>() {
            return Err(DBStateError::ParticipantSignature);
        }
    }

    let time_now = Timestamp::from(SystemTime::now());
    if time_now.seconds >= terms.timeout.seconds {
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

    // Validate epoch
    //
    // Epochs should be monotonically increasing
    if terms.epoch < current.epoch {
        return Err(DBStateError::InvalidEpoch);
    }

    if terms.epoch == current.epoch && !current.status.is_terminal() {
        return Err(DBStateError::InvalidEpoch);
    }

    // If we have some leftover state after having left the network, we can accept higher epochs
    if terms.epoch > current.epoch + 1
        && (current.status != Status::Left && current.status != Status::Fresh)
    {
        return Err(DBStateError::InvalidEpochLeftover);
    }

    Ok(())
}

impl<S: Scheme> TryFrom<ProposalTerms> for State<S> {
    type Error = PointSerDeError;

    fn try_from(p: ProposalTerms) -> Result<Self, Self::Error> {
        let state = State::<S> {
            beacon_id: p.beacon_id,
            epoch: p.epoch,
            status: Status::Proposed,
            threshold: p.threshold,
            timeout: p.timeout,
            genesis_time: p.genesis_time,
            genesis_seed: p.genesis_seed,
            catchup_period: p.catchup_period_seconds,
            beacon_period: p.beacon_period_seconds,
            leader: p.leader,
            remaining: p.remaining,
            joining: p.joining,
            leaving: p.leaving,
            acceptors: vec![],
            rejectors: vec![],
            final_group: None,
            key_share: None,
        };

        Ok(state)
    }
}

fn validate_previous_group_for_joiners<S: Scheme>(
    d: &State<S>,
    prev_group: Option<&Group<S>>,
) -> Result<(), DBStateError> {
    // joiners after the first epoch must pass a group file in order to determine
    // that the proposal is valid (e.g. the `GenesisTime` and `Remaining` group are correct)
    match prev_group {
        Some(prev_group) => {
            if prev_group.genesis_time != u64::try_from(d.genesis_time.seconds).unwrap() {
                return Err(DBStateError::GenesisTimeNotConsistentWithProposal);
            }
            if prev_group.genesis_seed != d.genesis_seed {
                return Err(DBStateError::GenesisSeedCannotChange);
            }
            Ok(())
        }
        None => {
            if d.epoch() == 1 {
                Ok(())
            } else {
                Err(DBStateError::JoiningAfterFirstEpochNeedsGroupFile)
            }
        }
    }
}

/// Used for status request
impl<S: Scheme> From<State<S>> for crate::protobuf::dkg::DkgEntry {
    fn from(s: State<S>) -> Self {
        fn convert<T, U, I>(iter: I) -> Vec<U>
        where
            I: IntoIterator<Item = T>,
            T: Into<U>,
        {
            iter.into_iter().map(Into::into).collect()
        }

        let leader = if s.status == Status::Fresh {
            None
        } else {
            Some(s.leader.into())
        };

        Self {
            beacon_id: s.beacon_id,
            state: s.status as u32,
            epoch: s.epoch,
            threshold: s.threshold,
            timeout: Some(s.timeout),
            genesis_time: Some(s.genesis_time),
            genesis_seed: s.genesis_seed,
            leader,
            remaining: convert(s.remaining),
            joining: convert(s.joining),
            leaving: convert(s.leaving),
            acceptors: convert(s.acceptors),
            rejectors: convert(s.rejectors),
            final_group: match s.final_group {
                Some(group) => group
                    .nodes()
                    .iter()
                    .map(|node| node.public().address().to_string())
                    .collect(),
                None => vec![],
            },
        }
    }
}

fn is_proposal_phase<S: Scheme>(state: &State<S>) -> bool {
    matches!(
        state.status(),
        Status::Proposed | Status::Proposing | Status::Accepted | Status::Rejected | Status::Joined
    )
}
