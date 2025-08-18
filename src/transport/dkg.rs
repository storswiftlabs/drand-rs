//! Types are re-exported directly if their fields DO NOT contain:
//!  - option<T> instead of T
//!  - protected new pattern types

pub use prost_types::Timestamp;
pub use protobuf::dkg::packet::Bundle;
pub use protobuf::dkg::AbortDkg;
pub use protobuf::dkg::AbortOptions;
pub use protobuf::dkg::AcceptOptions;
pub use protobuf::dkg::CommandMetadata;
pub use protobuf::dkg::ExecutionOptions;
pub use protobuf::dkg::JoinOptions;
pub use protobuf::dkg::RejectOptions;

use super::utils::from_vec;
use super::utils::try_from_vec;
use super::utils::ConvertProto;
use super::utils::RequireSome;
use super::utils::TransportError;
use protobuf::drand::Metadata;

use crate::net::utils::Address;
use crate::net::utils::Seconds;
use crate::protobuf;

#[derive(Debug, Default, PartialEq, Clone)]
pub struct Participant {
    pub address: Address,
    pub key: Vec<u8>,
    pub signature: Vec<u8>,
}

impl ConvertProto for crate::protobuf::dkg::Participant {
    type Inner = Participant;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        let Self {
            ref address,
            key,
            signature,
        } = self;

        Ok(Self::Inner {
            address: Address::precheck(address)?,
            key,
            signature,
        })
    }
}

impl From<Participant> for crate::protobuf::dkg::Participant {
    fn from(value: Participant) -> Self {
        let Participant {
            ref address,
            key,
            signature,
        } = value;

        Self {
            address: address.as_str().into(),
            key,
            signature,
        }
    }
}

#[derive(Clone)]
pub struct GossipMetadata {
    pub beacon_id: String,
    pub address: Address,
    pub signature: Vec<u8>,
}

impl GossipMetadata {
    pub fn address(&self) -> &Address {
        &self.address
    }
}

impl ConvertProto for protobuf::dkg::GossipMetadata {
    type Inner = GossipMetadata;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        let Self {
            beacon_id,
            ref address,
            signature,
        } = self;

        Ok(Self::Inner {
            beacon_id,
            address: Address::precheck(address)?,
            signature,
        })
    }
}

impl From<GossipMetadata> for protobuf::dkg::GossipMetadata {
    fn from(value: GossipMetadata) -> Self {
        let GossipMetadata {
            beacon_id,
            ref address,
            signature,
        } = value;

        Self {
            beacon_id,
            address: address.as_str().into(),
            signature,
        }
    }
}
#[derive(Clone)]
pub struct ProposalTerms {
    pub beacon_id: String,
    pub epoch: u32,
    pub leader: Participant,
    pub threshold: u32,
    pub timeout: Timestamp,
    pub catchup_period_seconds: Seconds,
    pub beacon_period_seconds: Seconds,
    pub scheme_id: String,
    pub genesis_time: Timestamp,
    pub genesis_seed: Vec<u8>,
    pub joining: Vec<Participant>,
    pub remaining: Vec<Participant>,
    pub leaving: Vec<Participant>,
}

impl ConvertProto for protobuf::dkg::ProposalTerms {
    type Inner = ProposalTerms;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        let Self {
            beacon_id,
            epoch,
            leader,
            threshold,
            timeout,
            catchup_period_seconds,
            beacon_period_seconds,
            scheme_id,
            genesis_time,
            genesis_seed,
            joining,
            remaining,
            leaving,
        } = self;

        Ok(Self::Inner {
            beacon_id,
            epoch,
            leader: leader.require_some()?.validate()?,
            threshold,
            timeout: timeout.require_some()?,
            catchup_period_seconds: catchup_period_seconds.into(),
            beacon_period_seconds: beacon_period_seconds.into(),
            scheme_id,
            genesis_time: genesis_time.require_some()?,
            genesis_seed,
            joining: try_from_vec(joining)?,
            remaining: try_from_vec(remaining)?,
            leaving: try_from_vec(leaving)?,
        })
    }
}

impl From<ProposalTerms> for protobuf::dkg::ProposalTerms {
    fn from(value: ProposalTerms) -> Self {
        let ProposalTerms {
            beacon_id,
            epoch,
            leader,
            threshold,
            timeout,
            catchup_period_seconds,
            beacon_period_seconds,
            scheme_id,
            genesis_time,
            genesis_seed,
            joining,
            remaining,
            leaving,
        } = value;

        Self {
            beacon_id,
            epoch,
            leader: Some(leader.into()),
            threshold,
            timeout: Some(timeout),
            catchup_period_seconds: catchup_period_seconds.into(),
            beacon_period_seconds: beacon_period_seconds.into(),
            scheme_id,
            genesis_time: Some(genesis_time),
            genesis_seed,
            joining: from_vec(joining),
            remaining: from_vec(remaining),
            leaving: from_vec(leaving),
        }
    }
}

#[derive(Clone)]
pub struct AcceptProposal {
    pub acceptor: Participant,
}

impl ConvertProto for protobuf::dkg::AcceptProposal {
    type Inner = AcceptProposal;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        Ok(Self::Inner {
            acceptor: self.acceptor.require_some()?.validate()?,
        })
    }
}

impl From<AcceptProposal> for protobuf::dkg::AcceptProposal {
    fn from(value: AcceptProposal) -> Self {
        Self {
            acceptor: Some(value.acceptor.into()),
        }
    }
}

#[derive(Clone)]
pub struct RejectProposal {
    pub rejector: Participant,
    pub reason: String,
    pub secret: Vec<u8>,
    pub previous_group_hash: Vec<u8>,
    pub proposal_hash: Vec<u8>,
}

impl ConvertProto for protobuf::dkg::RejectProposal {
    type Inner = RejectProposal;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        let Self {
            rejector,
            reason,
            secret,
            previous_group_hash,
            proposal_hash,
        } = self;

        Ok(Self::Inner {
            rejector: rejector.require_some()?.validate()?,
            reason,
            secret,
            previous_group_hash,
            proposal_hash,
        })
    }
}

impl From<RejectProposal> for protobuf::dkg::RejectProposal {
    fn from(value: RejectProposal) -> Self {
        let RejectProposal {
            rejector,
            reason,
            secret,
            previous_group_hash,
            proposal_hash,
        } = value;

        Self {
            rejector: Some(rejector.into()),
            reason,
            secret,
            previous_group_hash,
            proposal_hash,
        }
    }
}
#[derive(Clone)]
pub struct StartExecution {
    pub time: Timestamp,
}

impl ConvertProto for protobuf::dkg::StartExecution {
    type Inner = StartExecution;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        Ok(Self::Inner {
            time: self.time.require_some()?,
        })
    }
}

impl From<StartExecution> for protobuf::dkg::StartExecution {
    fn from(value: StartExecution) -> Self {
        Self {
            time: Some(value.time),
        }
    }
}

#[derive(Clone)]
pub struct Packet {
    pub metadata: Metadata,
    pub bundle: Bundle,
}

impl ConvertProto for protobuf::dkg::Packet {
    type Inner = Packet;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        let Self { metadata, bundle } = self;

        Ok(Self::Inner {
            metadata: metadata.require_some()?,
            bundle: bundle.require_some()?,
        })
    }
}

impl From<Packet> for protobuf::dkg::Packet {
    fn from(value: Packet) -> Self {
        let Packet { metadata, bundle } = value;

        Self {
            metadata: Some(metadata),
            bundle: Some(bundle),
        }
    }
}

#[derive(Clone)]
pub struct DkgPacket {
    pub dkg: Packet,
}

impl ConvertProto for protobuf::dkg::DkgPacket {
    type Inner = DkgPacket;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        Ok(Self::Inner {
            dkg: self.dkg.require_some()?.validate()?,
        })
    }
}

impl From<DkgPacket> for protobuf::dkg::DkgPacket {
    fn from(value: DkgPacket) -> Self {
        Self {
            dkg: Some(value.dkg.into()),
        }
    }
}

#[derive(Clone)]
pub enum GossipData {
    Proposal(ProposalTerms),
    Accept(AcceptProposal),
    Reject(RejectProposal),
    Execute(StartExecution),
    Abort(AbortDkg),
    Dkg(DkgPacket),
}

impl ConvertProto for protobuf::dkg::gossip_packet::Packet {
    type Inner = GossipData;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        use protobuf::dkg::gossip_packet::Packet as GpPacket;

        let data = match self {
            GpPacket::Proposal(terms) => GossipData::Proposal(terms.validate()?),
            GpPacket::Accept(accept) => GossipData::Accept(accept.validate()?),
            GpPacket::Reject(reject) => GossipData::Reject(reject.validate()?),
            GpPacket::Execute(start) => GossipData::Execute(start.validate()?),
            GpPacket::Dkg(packet) => GossipData::Dkg(packet.validate()?),
            GpPacket::Abort(abort) => GossipData::Abort(abort),
        };

        Ok(data)
    }
}

impl From<GossipData> for protobuf::dkg::gossip_packet::Packet {
    fn from(value: GossipData) -> Self {
        match value {
            GossipData::Proposal(terms) => Self::Proposal(terms.into()),
            GossipData::Accept(accept) => Self::Accept(accept.into()),
            GossipData::Reject(reject) => Self::Reject(reject.into()),
            GossipData::Execute(start) => Self::Execute(start.into()),
            GossipData::Abort(abort) => Self::Abort(abort),
            GossipData::Dkg(packet) => Self::Dkg(packet.into()),
        }
    }
}

#[derive(Clone)]
pub struct GossipPacket {
    pub data: GossipData,
    pub metadata: GossipMetadata,
}

impl ConvertProto for protobuf::dkg::GossipPacket {
    type Inner = GossipPacket;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        let Self { metadata, packet } = self;
        let data = packet.require_some()?.validate()?;
        let metadata = metadata.require_some()?.validate()?;

        Ok(Self::Inner { data, metadata })
    }
}

impl From<GossipPacket> for protobuf::dkg::GossipPacket {
    fn from(value: GossipPacket) -> Self {
        let GossipPacket { data, metadata } = value;

        Self {
            metadata: Some(metadata.into()),
            packet: Some(data.into()),
        }
    }
}

pub struct FirstProposalOptions {
    pub timeout: Timestamp,
    pub threshold: u32,
    pub period_seconds: Seconds,
    pub scheme: String,
    pub catchup_period_seconds: Seconds,
    pub genesis_time: Timestamp,
    pub joining: Vec<Participant>,
}

impl ConvertProto for protobuf::dkg::FirstProposalOptions {
    type Inner = FirstProposalOptions;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        let Self {
            timeout,
            threshold,
            period_seconds,
            scheme,
            catchup_period_seconds,
            genesis_time,
            joining,
        } = self;

        Ok(Self::Inner {
            timeout: timeout.require_some()?,
            threshold,
            period_seconds: period_seconds.into(),
            scheme,
            catchup_period_seconds: catchup_period_seconds.into(),
            genesis_time: genesis_time.require_some()?,
            joining: try_from_vec(joining)?,
        })
    }
}

impl From<FirstProposalOptions> for protobuf::dkg::FirstProposalOptions {
    fn from(value: FirstProposalOptions) -> Self {
        let FirstProposalOptions {
            timeout,
            threshold,
            period_seconds,
            scheme,
            catchup_period_seconds,
            genesis_time,
            joining,
        } = value;

        Self {
            timeout: Some(timeout),
            threshold,
            period_seconds: period_seconds.into(),
            scheme,
            catchup_period_seconds: catchup_period_seconds.into(),
            genesis_time: Some(genesis_time),
            joining: from_vec(joining),
        }
    }
}

pub struct ProposalOptions {
    pub timeout: Timestamp,
    pub threshold: u32,
    pub catchup_period_seconds: Seconds,
    pub joining: Vec<Participant>,
    pub leaving: Vec<Participant>,
    pub remaining: Vec<Participant>,
}

impl ConvertProto for protobuf::dkg::ProposalOptions {
    type Inner = ProposalOptions;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        let Self {
            timeout,
            threshold,
            catchup_period_seconds,
            joining,
            leaving,
            remaining,
        } = self;

        Ok(Self::Inner {
            timeout: timeout.require_some()?,
            threshold,
            catchup_period_seconds: catchup_period_seconds.into(),
            joining: try_from_vec(joining)?,
            leaving: try_from_vec(leaving)?,
            remaining: try_from_vec(remaining)?,
        })
    }
}

impl From<ProposalOptions> for protobuf::dkg::ProposalOptions {
    fn from(value: ProposalOptions) -> Self {
        let ProposalOptions {
            timeout,
            threshold,
            catchup_period_seconds,
            joining,
            leaving,
            remaining,
        } = value;

        Self {
            timeout: Some(timeout),
            threshold,
            catchup_period_seconds: catchup_period_seconds.into(),
            joining: from_vec(joining),
            leaving: from_vec(leaving),
            remaining: from_vec(remaining),
        }
    }
}

pub struct DkgCommand {
    pub metadata: CommandMetadata,
    pub command: Command,
}

impl ConvertProto for protobuf::dkg::DkgCommand {
    type Inner = DkgCommand;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        let Self { metadata, command } = self;

        Ok(Self::Inner {
            metadata: metadata.require_some()?,
            command: command.require_some()?.validate()?,
        })
    }
}

impl From<DkgCommand> for protobuf::dkg::DkgCommand {
    fn from(value: DkgCommand) -> Self {
        let DkgCommand { metadata, command } = value;

        Self {
            metadata: Some(metadata),
            command: Some(command.into()),
        }
    }
}

pub enum Command {
    Initial(FirstProposalOptions),
    Resharing(ProposalOptions),
    Join(JoinOptions),
    Accept(AcceptOptions),
    Reject(RejectOptions),
    Execute(ExecutionOptions),
    Abort(AbortOptions),
}

impl ConvertProto for protobuf::dkg::dkg_command::Command {
    type Inner = Command;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        use protobuf::dkg::dkg_command as dc;

        let data = match self {
            dc::Command::Initial(initial) => Command::Initial(initial.validate()?),
            dc::Command::Resharing(proposal) => Command::Resharing(proposal.validate()?),
            dc::Command::Join(join) => Command::Join(join),
            dc::Command::Accept(accept) => Command::Accept(accept),
            dc::Command::Reject(reject) => Command::Reject(reject),
            dc::Command::Execute(execution) => Command::Execute(execution),
            dc::Command::Abort(abort) => Command::Abort(abort),
        };

        Ok(data)
    }
}

impl From<Command> for protobuf::dkg::dkg_command::Command {
    fn from(value: Command) -> Self {
        match value {
            Command::Initial(initial) => Self::Initial(initial.into()),
            Command::Resharing(proposal) => Self::Resharing(proposal.into()),
            Command::Join(join) => Self::Join(join),
            Command::Accept(accept) => Self::Accept(accept),
            Command::Reject(reject) => Self::Reject(reject),
            Command::Execute(execution) => Self::Execute(execution),
            Command::Abort(abort) => Self::Abort(abort),
        }
    }
}

pub struct DkgEntry {
    pub beacon_id: String,
    pub state: u32,
    pub epoch: u32,
    pub threshold: u32,
    pub timeout: Timestamp,
    pub genesis_time: Timestamp,
    pub genesis_seed: Vec<u8>,
    pub leader: Participant,
    pub remaining: Vec<Participant>,
    pub joining: Vec<Participant>,
    pub leaving: Vec<Participant>,
    pub acceptors: Vec<Participant>,
    pub rejectors: Vec<Participant>,
    pub final_group: Vec<String>,
}

impl ConvertProto for protobuf::dkg::DkgEntry {
    type Inner = DkgEntry;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        let Self {
            beacon_id,
            state,
            epoch,
            threshold,
            timeout,
            genesis_time,
            genesis_seed,
            leader,
            remaining,
            joining,
            leaving,
            acceptors,
            rejectors,
            final_group,
        } = self;

        Ok(Self::Inner {
            beacon_id,
            state,
            epoch,
            threshold,
            timeout: timeout.require_some()?,
            genesis_time: genesis_time.require_some()?,
            genesis_seed,
            leader: leader.require_some()?.validate()?,
            remaining: try_from_vec(remaining)?,
            joining: try_from_vec(joining)?,
            leaving: try_from_vec(leaving)?,
            acceptors: try_from_vec(acceptors)?,
            rejectors: try_from_vec(rejectors)?,
            final_group,
        })
    }
}

impl From<DkgEntry> for protobuf::dkg::DkgEntry {
    fn from(value: DkgEntry) -> Self {
        let DkgEntry {
            beacon_id,
            state,
            epoch,
            threshold,
            timeout,
            genesis_time,
            genesis_seed,
            leader,
            remaining,
            joining,
            leaving,
            acceptors,
            rejectors,
            final_group,
        } = value;

        Self {
            beacon_id,
            state,
            epoch,
            threshold,
            timeout: Some(timeout),
            genesis_time: Some(genesis_time),
            genesis_seed,
            leader: Some(leader.into()),
            remaining: from_vec(remaining),
            joining: from_vec(joining),
            leaving: from_vec(leaving),
            acceptors: from_vec(acceptors),
            rejectors: from_vec(rejectors),
            final_group,
        }
    }
}

pub struct DkgStatusResponse {
    pub complete: DkgEntry,
    pub current: DkgEntry,
}

impl ConvertProto for protobuf::dkg::DkgStatusResponse {
    type Inner = DkgStatusResponse;

    fn validate(self) -> Result<Self::Inner, TransportError> {
        let Self { complete, current } = self;

        Ok(Self::Inner {
            complete: complete.require_some()?.validate()?,
            current: current.require_some()?.validate()?,
        })
    }
}

impl From<DkgStatusResponse> for protobuf::dkg::DkgStatusResponse {
    fn from(value: DkgStatusResponse) -> Self {
        let DkgStatusResponse { complete, current } = value;

        Self {
            complete: Some(complete.into()),
            current: Some(current.into()),
        }
    }
}
