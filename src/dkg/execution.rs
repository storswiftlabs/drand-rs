use super::broadcast::Broadcast;
use super::state::State;
use super::store::DkgStoreError;
use super::utils::GateKeeper;
use super::ActionsError;
use super::DkgNode;

use crate::core::beacon::BeaconProcess;
use crate::key::Scheme;
use crate::transport::dkg::Participant;

use energon::kyber::dkg::protocol::SendOut;
use energon::kyber::dkg::Config;
use energon::kyber::dkg::Protocol;

use prost_types::Timestamp;
use sha2::Digest;
use sha2::Sha256;
use std::future::Future;
use std::time::SystemTime;
use tracing::error;
use tracing::info;

pub trait ExecuteDkg {
    type Scheme: Scheme;

    fn setup_dkg(
        &self,
        execution_time: Timestamp,
        gk: &mut GateKeeper<Self::Scheme>,
    ) -> impl Future<Output = Result<(), ActionsError>>;

    fn initial_config(
        &self,
        current: &State<Self::Scheme>,
        sorted: &[&Participant],
    ) -> Result<Config<Self::Scheme>, ActionsError>;
}

impl<S: Scheme> ExecuteDkg for BeaconProcess<S> {
    type Scheme = S;

    async fn setup_dkg(
        &self,
        start_execution: Timestamp,
        gatekeeper: &mut GateKeeper<Self::Scheme>,
    ) -> Result<(), ActionsError> {
        // get current and last completed states
        let current = self.dkg_store().get_current::<S>()?;
        let last_completed = match self.dkg_store().get_finished::<S>() {
            Ok(state) => Some(state),
            Err(DkgStoreError::NotFound) => None,
            Err(err) => return Err(err.into()),
        };

        let me = self.as_participant()?;
        let (sendout_tx, sendout_rx) = SendOut::new();
        let broadcast = Broadcast::init(self.id(), self.log());

        // initial dkg case
        let config = if last_completed.is_none() {
            // sort participants by public key
            let mut sorted = Vec::with_capacity(current.joining.len() + current.remaining.len());
            sorted.extend(current.joining.iter());
            sorted.extend(current.remaining.iter());
            sort_by_public_key(&mut sorted);

            let config = self.initial_config(&current, &sorted)?;
            broadcast.register_nodes(self.tracker(), &sorted, sendout_rx, &me.address);

            config
        } else {
            panic!("reshape is not implemented yet");
        };

        let dkg_log = config.log.clone();
        let protocol = Protocol::new_dkg(config, sendout_tx).map_err(ActionsError::DkgError)?;

        // map timestamp into duration to sleep until execution time.
        let until_execution = SystemTime::try_from(start_execution)
            .map_err(|_| ActionsError::StartExecutionTimeNotCanonical)?
            .duration_since(SystemTime::now())
            .map_err(|_| ActionsError::StartExecutionTimeIsPassed)?;

        let broadcast_rx = gatekeeper.create_channel()?;

        // start protocol at execution time
        self.tracker().spawn({ async move {
            info!(parent: &dkg_log, "waiting for execution time: {} seconds", until_execution.as_secs());
            tokio::time::sleep(until_execution).await;

            match protocol.start(broadcast_rx).await {
                Ok(Ok(_dkg_output)) => info!(parent: dkg_log, "DKG output received succesfully"),
                Ok(Err(err)) => error!(parent: dkg_log, "DKG is failed, reason: {err}"),
                Err(err) => error!(parent: dkg_log, "DKG protocol task failed to execute to completion, {err}")
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
            .map(|(index, participant)| DkgNode::deserialize(index as u32, &participant.key))
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
            public_coeffs,
            new_nodes,
            share,
            threshold,
            old_threshold,
            nonce,
            log,
        };

        Ok(config)
    }
}

fn sort_by_public_key(participants: &mut [&Participant]) {
    participants.sort_by_key(|p| &p.key);
}

fn nonce_for_epoch(epoch: u32) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(epoch.to_be_bytes());
    h.finalize().into()
}
