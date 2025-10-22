use std::str::FromStr;

#[derive(Debug, Copy, Clone, PartialEq)]
#[repr(u8)]
/// Status is pre-auth layer for any interaction with [`DBState`]
//  DEV: partial type-state pattern might be considered
pub enum Status {
    // Fresh is the state all nodes start in - both pre-genesis, and if the network is running but they aren't
    // yet participating
    Fresh,
    // Proposed implies somebody else has sent me a proposal
    Proposed,
    // Proposing implies I have sent the others in the network a proposal
    Proposing,
    // Accepted means I have accepted a proposal received from somebody else
    // note Joiners do not accept/reject proposals
    Accepted,
    // Rejected means I have rejected a proposal received from somebody else
    // it doesn't automatically abort the DKG, but the leader is advised to abort and suggest some new terms
    Rejected,
    // Aborted means the leader has told the network to abort the proposal; a node may have rejected,
    // they may have found an error in the proposal, or any other reason could have occurred
    Aborted,
    // Executing means the leader has reviewed accepts/rejects and decided to go ahead with the DKG
    // this implies that the Kyber DKG process has been started
    Executing,
    // Complete means the DKG has finished and a new group file has been created successfully
    Complete,
    // TimedOut means the proposal timeout has been reached without entering the `Executing` state
    // any node can trigger this for themselves should they identify timeout has been reached
    // it does _not_ guarantee that other nodes have also timed out - a network error or something else
    // could have occurred. If the rest of the network continues, our node will likely transition to `Evicted`
    TimedOut,
    // Joined is the state a new proposed group member enters when they have been proposed a DKG and they run the
    // `join` DKG command to signal their acceptance to join the network
    Joined,
    // Left is used when a node has left the network by their own choice after a DKG. It's not entirely necessary,
    // an operator could just turn their node off. It's used to determine if an existing state is the current state
    // of the network, or whether epochs have happened in between times
    Left,
    // Failed signals that a key sharing execution was attempted, but this node did not see it complete successfully.
    // This could be either due to it being evicted or the DKG not completing for the whole network. Operators should
    // check the node and network status, and manually transition the node to `Left` or create a new proposal depending
    // on the outcome of the DKG
    Failed,
}

#[derive(thiserror::Error, Debug)]
pub enum StateError {
    #[error("invalid transition attempt from {from} to {to}")]
    InvalidStateChange { from: Status, to: Status },
    #[error("impossible DKG state received: {0}")]
    ImpossibleDkgState(u32),
    #[error("current DKG state is missing")]
    MissingCurrentState,
}

impl TryFrom<u32> for Status {
    type Error = StateError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Status::Fresh),
            1 => Ok(Status::Proposed),
            2 => Ok(Status::Proposing),
            3 => Ok(Status::Accepted),
            4 => Ok(Status::Rejected),
            5 => Ok(Status::Aborted),
            6 => Ok(Status::Executing),
            7 => Ok(Status::Complete),
            8 => Ok(Status::TimedOut),
            9 => Ok(Status::Joined),
            10 => Ok(Status::Left),
            11 => Ok(Status::Failed),
            _ => Err(StateError::ImpossibleDkgState(value)),
        }
    }
}

impl Status {
    #[rustfmt::skip]
    pub fn is_valid_state_change(self, next: Self) -> Result<(), StateError> {
        #[allow(clippy::enum_glob_use)]
        use self::Status::*;

        let is_valid = match self {
            Fresh => matches!(next, Proposing | Proposed),
            Joined => matches!(next, Left | Executing | Aborted | TimedOut),
            Proposing => matches!(next, Executing | Aborted | TimedOut),
            Proposed => matches!(next, Accepted | Rejected | Joined | Left | Aborted | TimedOut),
            Accepted => matches!(next, Executing | Aborted | TimedOut),
            Rejected => matches!(next, Aborted | TimedOut),
            Executing => matches!(next, Complete | TimedOut | Failed),
            Complete => matches!(next, Proposing | Proposed),
            Left => matches!(next, Joined | Aborted | Proposed),
            Aborted => matches!(next, Proposing | Proposed),
            TimedOut => matches!(next, Proposing | Proposed | Aborted),
            Failed => matches!(next, Proposing | Proposed | Left | Aborted),
        };

        if !is_valid {
            return Err(StateError::InvalidStateChange {
                from: self,
                to: next,
            });
        };

        Ok(())
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Status::Aborted | Status::TimedOut | Status::Failed)
    }
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

pub struct InvalidStatus;

impl FromStr for Status {
    type Err = InvalidStatus;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Fresh" => Ok(Status::Fresh),
            "Proposed" => Ok(Status::Proposed),
            "Proposing" => Ok(Status::Proposing),
            "Accepted" => Ok(Status::Accepted),
            "Rejected" => Ok(Status::Rejected),
            "Aborted" => Ok(Status::Aborted),
            "Executing" => Ok(Status::Executing),
            "Complete" => Ok(Status::Complete),
            "TimedOut" => Ok(Status::TimedOut),
            "Joined" => Ok(Status::Joined),
            "Left" => Ok(Status::Left),
            "Failed" => Ok(Status::Failed),
            _ => Err(InvalidStatus),
        }
    }
}
