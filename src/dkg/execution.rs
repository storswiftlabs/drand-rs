use super::{
    broadcast::Broadcast, state::State, store::DkgStoreError, utils::GateKeeper, ActionsError,
    DkgNode,
};
use crate::{
    chain::{
        time::{current_round, time_now, time_of_round, ROUNDS_UNTIL_TRANSITION},
        ChainCmd,
    },
    core::beacon::BeaconProcess,
    key::{
        group::Group,
        keys::{DistPublic, Identity},
        node::Node,
        Hash, Scheme,
    },
    transport::dkg::Participant,
};
use energon::{
    kyber::dkg::{Config, DistKeyShare, DkgOutput, Protocol},
    traits::Affine,
};
use prost_types::Timestamp;
use sha2::{Digest, Sha256};
use std::{
    future::Future,
    time::{Duration, SystemTime},
};
use tracing::{error, info, Span};

/// Default time of each DKG period by default.
///
/// DKG uses the "fast sync" mode that shorten the first phase
/// and the second phase, "as fast as possible" when the protocol runs smoothly
/// (there is no malicious party).
const DEFAULT_DKG_PHASE_TIMEOUT: Duration = Duration::from_secs(10);

pub trait ExecuteDkg {
    type Scheme: Scheme;

    fn setup_and_run_dkg(
        &self,
        execution_time: Timestamp,
        gk: &mut GateKeeper<Self::Scheme>,
    ) -> impl Future<Output = Result<(), ActionsError>>;

    fn initial_config(
        &self,
        current: &State<Self::Scheme>,
        sorted_participants: &[&Participant],
    ) -> Result<Config<Self::Scheme>, ActionsError>;

    fn reshare_config(
        &self,
        current: &State<Self::Scheme>,
        previous: State<Self::Scheme>,
        sorted_participants: &[&Participant],
    ) -> Result<Config<Self::Scheme>, ActionsError>;
}

impl<S: Scheme> ExecuteDkg for BeaconProcess<S> {
    type Scheme = S;

    async fn setup_and_run_dkg(
        &self,
        start_execution: Timestamp,
        gk: &mut GateKeeper<Self::Scheme>,
    ) -> Result<(), ActionsError> {
        // # Setup DKG #
        // Get current and last completed states
        let current = self.dkg_store().get_current::<S>()?;
        let last_completed = match self.dkg_store().get_finished::<S>() {
            Ok(state) => Some(state),
            Err(DkgStoreError::NotFound) => None,
            Err(err) => return Err(err.into()),
        };

        // Sort all participants by public_key.
        let mut sorted_participants =
            Vec::with_capacity(current.joining.len() + current.remaining.len());
        sorted_participants.extend(current.joining.iter());
        sorted_participants.extend(current.remaining.iter());
        sorted_participants.sort_by_key(|p| &p.key);

        // Map execution time into duration from now.
        let time_until_execution = SystemTime::try_from(start_execution)
            .map_err(|_| ActionsError::StartExecutionTimeNotCanonical)?
            .duration_since(SystemTime::now())
            .map_err(|_| ActionsError::StartExecutionTimeIsPassed)?;

        // Setup config for DKG protocol.
        let config = match last_completed {
            Some(previous) => self.reshare_config(&current, previous, &sorted_participants)?,
            None => self.initial_config(&current, &sorted_participants)?,
        };
        let dkg_log = config.log.clone();

        // Initialize DKG protocol instance with channels for input and output.
        let (protocol, bundles_rx, bundles_tx) =
            Protocol::new_dkg(config, DEFAULT_DKG_PHASE_TIMEOUT).map_err(ActionsError::DkgError)?;

        // Gatekeeper holds bundles sender during execution.
        gk.open_gate(bundles_tx)?;

        // Broadcast holds bundles receiver during execution.
        let broadcast = Broadcast::init(self.id(), &dkg_log);
        broadcast.register_nodes(
            self.tracker(),
            &sorted_participants,
            bundles_rx,
            &self.identity().address,
        );

        // # Run DKG #
        let bp = self.clone();
        self.tracker().spawn({ async move {
            info!(parent: &dkg_log, "waiting for execution time: {} seconds", time_until_execution.as_secs());
            tokio::time::sleep(time_until_execution).await;

            let dkg_output=protocol.run().await;
            bp.dkg_finished_notification().await;

            match dkg_output{
                Ok(Some(output)) => process_dkg_output(&bp, output, current, &dkg_log).await,
                Ok(None) => info!(parent: &dkg_log, "DKG[Leaving] finished succesfully"),
                Err(err) => error!(parent: &dkg_log, "DKG finished with error: {err}"),
            }
        }});

        Ok(())
    }

    fn initial_config(
        &self,
        current: &State<S>,
        sorted: &[&Participant],
    ) -> Result<Config<S>, ActionsError> {
        let new_nodes = sorted
            .iter()
            .enumerate()
            .map(|(index, participant)| {
                let index = u32::try_from(index).ok()?;
                DkgNode::deserialize(index, &participant.key)
            })
            .collect::<Option<Vec<DkgNode<S>>>>()
            .ok_or(ActionsError::ParticipantsToNewNodes)?;

        // Although this is an "initial" DKG, we could be a joiner, and we may need to set some things
        // from a prior DKG provided by the network
        let mut old_nodes = vec![];
        let mut public_coeffs = vec![];
        let mut old_threshold = 0;
        if let Some(group) = &current.final_group {
            old_threshold = group.threshold;
            public_coeffs = group.dist_key.commits().to_vec();
            old_nodes = group
                .nodes
                .iter()
                .map(|n| DkgNode {
                    index: n.index(),
                    public: n.public().key().to_owned(),
                })
                .collect();
        };

        let share = None;
        let threshold = current.threshold;
        let nonce = nonce_for_epoch(current.epoch());
        let dkg_index = new_nodes
            .iter()
            .find(|n| n.public() == self.identity().key())
            .map(|n| n.index)
            .expect("our node is always present in sorted nodes");

        // host.id.dkg_index
        let log = tracing::info_span!(
            "",
            dkg = format!("{}.{}.{dkg_index}", self.identity().address(), self.id())
        );
        let config = Config {
            long_term: self.private_key().to_owned(),
            old_nodes,
            new_nodes,
            public_coeffs,
            share,
            threshold,
            old_threshold,
            nonce,
            log,
        };

        Ok(config)
    }

    fn reshare_config(
        &self,
        current: &State<Self::Scheme>,
        previous: State<Self::Scheme>,
        sorted_participants: &[&Participant],
    ) -> Result<Config<Self::Scheme>, ActionsError> {
        let new_nodes = sorted_participants
            .iter()
            .enumerate()
            .map(|(index, participant)| {
                let index = u32::try_from(index).ok()?;
                DkgNode::deserialize(index, &participant.key)
            })
            .collect::<Option<Vec<DkgNode<S>>>>()
            .ok_or(ActionsError::ParticipantsToNewNodes)?;

        let dkg_index = new_nodes
            .iter()
            // Set index to "Leaving" if node piblic key is missing in `new_nodes`.
            .find(|n| n.public() == self.identity().key())
            .map_or_else(|| "Leaving".into(), |n| n.index.to_string());

        let log = tracing::info_span!(
            "",
            dkg = format!("{}.{}.{dkg_index}", self.identity().address(), self.id())
        );
        let prev_final_group = previous
            .final_group
            .as_ref()
            .ok_or(ActionsError::ResharePrevGroupRequired)?;

        let prev_keyshare = if previous.key_share.is_none() {
            return Err(ActionsError::ResharePrevShareRequired);
        } else {
            previous.key_share
        };

        let old_nodes = prev_final_group
            .nodes
            .iter()
            .map(|n| DkgNode {
                index: n.index(),
                public: n.public().key().to_owned(),
            })
            .collect();

        let config = Config {
            long_term: self.private_key().to_owned(),
            old_nodes,
            new_nodes,
            public_coeffs: prev_final_group.dist_key.commits().to_vec(),
            share: prev_keyshare,
            threshold: current.threshold,
            old_threshold: previous.threshold,
            nonce: nonce_for_epoch(current.epoch()),
            log,
        };

        Ok(config)
    }
}

// Returns round of transition to new group.
async fn process_dkg_output<S: Scheme>(
    bp: &BeaconProcess<S>,
    output: DkgOutput<S>,
    mut current: State<S>,
    l: &Span,
) {
    let is_first_epoch = current.epoch() == 1;

    let transition_time = if is_first_epoch {
        info!(parent: l, "DKG [Initial] finished succesfully");
        u64::try_from(current.genesis_time.seconds).unwrap()
    } else {
        let now = time_now().as_secs();
        info!(parent: l, "DKG [Reshape] finished succesfully");

        let beacon_period = current.beacon_period.get_value();
        let current_genesis = u64::try_from(current.genesis_time.seconds).unwrap();
        let current_round = current_round(now, beacon_period, current_genesis);
        let curr_round_add_tr = current_round + ROUNDS_UNTIL_TRANSITION;
        time_of_round(beacon_period, current_genesis, curr_round_add_tr)
    };

    let (final_group, share) = as_group(output, &current, transition_time);

    if let Err(err) = bp.fs().save_share(&share) {
        error!(parent: l, "failed to store private share: {err}");
        return;
    }
    info!(parent: l, "private share is stored at {}", bp.fs().private_share_file().display());

    if let Err(err) = bp.fs().save_group(&final_group) {
        error!(parent: l, "failed to store groupfile: {err}");
        return;
    }

    let period = final_group.period.get_value();
    let genesis_time = final_group.genesis_time;

    // Completed state is a new current and new finished state.
    if let Err(err) = current.complete(final_group, share) {
        error!(parent: l, "failed to move into compeleted state: {err}");
        return;
    };

    if let Err(err) = bp.dkg_store().save_finished(&current) {
        error!(parent: l, "failed to store the completed state: {err}");
        return;
    }

    let t_round = current_round(transition_time, period, genesis_time);
    let t_time = time_of_round(period, genesis_time, t_round);
    if t_time != transition_time {
        error!(parent: l, "transition_time: invalid_offset: expected {t_time} got_time {transition_time}");
        return;
    }
    info!(parent: l,"preparing transition to new group at_round: {t_round}");

    // Sleep until last round of current epoch.
    let now = time_now().as_secs();
    let last_round = Duration::from_secs(transition_time - u64::from(period)).as_secs();
    let delta = last_round.saturating_sub(now);

    if delta != 0 {
        info!(parent: l, "sleeping until last round before transition: {delta}s");
        tokio::time::sleep(Duration::from_secs(delta)).await;
    }

    if bp
        .chain_cmd_tx
        .send(ChainCmd::NewEpoch {
            first_round: t_round,
        })
        .await
        .is_err()
    {
        error!(parent: l, "failed to send new_epoch cmd to chain handler");
    }
}

fn as_group<S: Scheme>(
    output: DkgOutput<S>,
    current: &State<S>,
    transition_time: u64,
) -> (Group<S>, DistKeyShare<S>) {
    let DkgOutput { qual, key } = output;

    // Sort all participants by public_key.
    let mut all_sorted = Vec::with_capacity(current.joining.len() + current.remaining.len());
    all_sorted.extend(current.joining.iter());
    all_sorted.extend(current.remaining.iter());
    sort_by_public_key(&mut all_sorted);

    // Collect qualified participants using QUAL indexes
    let remaning = qual
        .into_iter()
        .map(|node| {
            let participant = all_sorted
                .get(node.index as usize)
                .expect("qualified nodes are always a subset of sorted nodes");
            let identity = Identity::<S>::new(
                participant.address.clone(),
                node.public,
                Affine::deserialize(&participant.signature)
                    .expect("signature bytes already prechecked"),
            );
            Node::new(identity, node.index)
        })
        .collect::<Vec<Node<S>>>();

    let mut group = Group {
        threshold: current.threshold,
        period: current.beacon_period,
        catchup_period: current.catchup_period,
        genesis_time: u64::try_from(current.genesis_time.seconds).unwrap(),
        transition_time,
        genesis_seed: current.genesis_seed.clone(),
        beacon_id: current.beacon_id.clone(),
        nodes: remaning,
        dist_key: DistPublic::new(key.commits.clone()),
    };

    if group.genesis_seed.is_empty() {
        group.genesis_seed.extend_from_slice(&group.hash());
    }

    let share = DistKeyShare {
        commits: key.commits,
        pri_share: key.pri_share,
    };

    (group, share)
}

fn sort_by_public_key(participants: &mut [&Participant]) {
    participants.sort_by_key(|p| &p.key);
}

fn nonce_for_epoch(epoch: u32) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(epoch.to_be_bytes());
    h.finalize().into()
}
