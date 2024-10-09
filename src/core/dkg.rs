// Copyright (C) 2023-2024 StorSwift Inc.
// This file is part of the Drand-RS library.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::beacon::BeaconCmd;
use super::beacon::Callback;
use super::beacon::InnerNode;
use super::chain::ChainConfig;

use energon::drand::poly::PriPoly;
use energon::drand::poly::PriShare;
use energon::drand::poly::PubPoly;
use energon::drand::Scheme;

use energon::traits::Affine;

use crate::key::group::Group;
use crate::key::keys::DistKeyShare;
use crate::key::keys::PublicKey;
use crate::net::address::DkgBroadcast;
use crate::net::address::Pool;
use crate::net::transport::Bundle;
use crate::net::transport::Deal;
use crate::net::transport::DealBundle;
use crate::net::transport::DkgInfo;
use crate::net::transport::JustificationBundle;
use crate::net::transport::Response;
use crate::net::transport::ResponseBundle;
use crate::net::transport::SetupInfo;

use anyhow::anyhow;
use anyhow::bail;
use anyhow::Result;

use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::sync::oneshot::Sender;
use tokio::time::Instant;
use tokio::time::Interval;
use tracing::*;

pub const MINIMUM_SHARE_SECRET_LENGTH: usize = 32;

pub enum DkgState {
    NonActive,
    Initiated {
        leader_key: Vec<u8>,
        dkg_secret: Vec<u8>,
        _reloader: mpsc::Sender<BeaconCmd>,
    },
    Running(mpsc::Sender<(Bundle, Callback<Result<()>>)>),
}

impl Display for DkgState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let varint = match self {
            DkgState::NonActive => "NonActive",
            DkgState::Initiated { .. } => "Initiated",
            DkgState::Running(_) => "Running",
        };
        write!(f, "{varint}")
    }
}

// TODO: make varints types readable >:)
pub enum DkgCmd {
    Broadcast(Callback<Result<()>>, Bundle),
    // callback, info, reload
    Init(Callback<Result<String>>, SetupInfo, mpsc::Sender<BeaconCmd>),
    NewGroup(Callback<Result<()>>, DkgInfo),
    Finish(Result<(ChainConfig, Vec<String>)>),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Phase {
    Deal,
    Response,
    Justification,
}

struct PhaseTimer {
    phase: Phase,
    pub interval: Interval,
}

impl PhaseTimer {
    async fn new(timeout: u32) -> Self {
        let mut interval = tokio::time::interval(Duration::from_secs(timeout.into()));
        // next line purpose is to ignore first tick which completes immediately
        interval.tick().await;
        Self {
            phase: Phase::Deal,
            interval,
        }
    }

    fn new_phase(&mut self) {
        self.phase = match self.phase {
            Phase::Deal => Phase::Response,
            Phase::Response => Phase::Justification,
            // not used currently
            Phase::Justification => Phase::Justification,
        };
        self.interval.reset();
    }

    pub fn phase_info(&self) -> Phase {
        self.phase
    }
}

pub struct DkgConfig<S: Scheme> {
    leader_key: PublicKey<S>,
    group: Group<S>,
    session_id: Vec<u8>,
    timeout: u32,
    index: u32,
}

impl<S: Scheme> DkgConfig<S> {
    pub fn new(leader_key: PublicKey<S>, group: Group<S>, timeout: u32, index: u32) -> Self {
        let session_id = group.get_nonce();
        Self {
            leader_key,
            group,
            session_id,
            timeout,
            index,
        }
    }
}

pub struct Generator<S: Scheme> {
    is_started: bool,
    node: Arc<InnerNode<S>>,
    inputs: DkgConfig<S>,
    pri_poly: PriPoly<S>,
    pub_poly: PubPoly<S>,
    sets: Sets,
    distributed: HashMap<u32, (S::Scalar, Vec<PublicKey<S>>)>,
    broadcast: DkgBroadcast,
}

impl<S: Scheme> Generator<S> {
    pub fn create(
        node: Arc<InnerNode<S>>,
        inputs: DkgConfig<S>,
        broadcast: DkgBroadcast,
    ) -> Generator<S> {
        let pri_poly = PriPoly::new(inputs.group.threshold);
        let pub_poly = pri_poly.commit();
        let sets = Sets::new(inputs.group.nodes.len(), node.span().clone());

        Generator {
            is_started: false,
            node,
            inputs,
            pri_poly,
            pub_poly,
            sets,
            distributed: HashMap::new(),
            broadcast,
        }
    }

    pub fn run(
        inputs: DkgConfig<S>,
        node: Arc<InnerNode<S>>,
        mut rx: mpsc::Receiver<(Bundle, Sender<Result<()>>)>,
        _reloader: mpsc::Sender<BeaconCmd>,
    ) -> Result<()> {
        let uris = inputs.group.get_uris(inputs.index);
        let tls = node.keypair.public().tls();
        let broadcast = DkgBroadcast::enable(node.span(), uris, tls)?;

        let timeout = inputs.timeout;
        info!(parent: node.span(), "DKG generator enabled. Local index ==== {} ===", inputs.index);
        // TODO: include local (dkg) index in logger.
        let mut gen = Self::create(Arc::clone(&node), inputs, broadcast);

        tokio::spawn(async move {
            let mut pt = PhaseTimer::new(timeout).await;

            let dkg_result = loop {
                tokio::select! {

                    // next phase [timeout]
                    _= pt.interval.tick()=> {
                        pt.new_phase();
                        info!(parent: gen.node.span(), "timeout, moving to {} phase", pt.phase);

                        if pt.phase_info()==Phase::Justification{
                            break Err(anyhow!("dkg is failed due to timeout, collected sets: {:?}", gen.sets))
                        };
                    }

                    // next phase [fast_move]
                    bundle = rx.recv() => if let Some((bundle,callback)) = bundle {

                        match gen.process_bundle(bundle).await{
                            Ok(is_added) => { let _= callback.send(Ok(()));

                                if is_added && gen.sets.is_fast_move(pt.phase_info()) {
                                    match pt.phase_info() {
                                        Phase::Deal => {
                                            gen.process_deals().await;
                                            gen.send_responses().await;
                                            pt.new_phase()
                                        }
                                        Phase::Response => {
                                            gen.process_responces();

                                            break gen.save_distributed();
                                        }
                                        Phase::Justification => unreachable!(),
                                    }
                                }
                            },
                            Err(err) =>{ let _= callback.send(Err(err)); }
                        }
                    }
                }
            };

            let _ = _reloader
                .send(BeaconCmd::Dkg(DkgCmd::Finish(dkg_result)))
                .await;
        });

        Ok(())
    }

    async fn process_bundle(&mut self, bundle: Bundle) -> Result<bool> {
        let now = Instant::now();
        let is_added = match bundle {
            Bundle::Deal(mut d) => {
                let hash = self
                    .precheck(d.hash(), &d.signature, d.dealer_index, &d.session_id)
                    .await?;

                self.sets
                    .deal
                    .push(d.dealer_index, hash, d, "deal", self.node.span())?
            }
            Bundle::Response(mut r) => {
                let hash = self
                    .precheck(r.hash(), &r.signature, r.share_index, &r.session_id)
                    .await?;

                self.sets
                    .resp
                    .push(r.share_index, hash, r, "responce", self.node.span())?
            }
            Bundle::Justification(mut j) => {
                let hash = self
                    .precheck(j.hash(), &j.signature, j.dealer_index, &j.session_id)
                    .await?;

                self.sets
                    .just
                    .push(j.dealer_index, hash, j, "justification", self.node.span())?
            }
        };
        let new_now = tokio::time::Instant::now();
        trace!(parent: self.node.span(), "MEASURE: process_bundle: {:?}", new_now.checked_duration_since(now).unwrap());
        Ok(is_added)
    }

    pub async fn send_deals(&self) -> Result<()> {
        let commits: Vec<Vec<u8>> = self
            .pub_poly
            .commits
            .iter()
            .map(|commit| commit.serialize().unwrap())
            .collect();

        let mut deals: Vec<Deal> = vec![];
        for node in &self.inputs.group.nodes {
            let si = self.pri_poly.eval(node.index()).v;
            if node.index() == self.inputs.index {
                continue;
            }

            if let Ok(cipher) = S::encrypt(node.identity().key(), &si) {
                deals.push(Deal {
                    share_index: node.index(),
                    encrypted_share: cipher,
                })
            }
        }
        let mut bundle = DealBundle {
            dealer_index: self.inputs.index,
            commits,
            deals,
            session_id: self.inputs.session_id.to_owned(),
            signature: vec![],
        };
        let hash = bundle.hash();
        bundle.signature = S::schnorr_sign(&self.node.keypair.private(), &hash)?;

        if self
            .broadcast
            .tx
            .send(Bundle::Deal(bundle).to_proto(self.node.beacon_id.to_string()))
            .is_err()
        {
            bail!("DkgBroadcast: channel closed")
        }

        Ok(())
    }

    async fn precheck(
        &mut self,
        hash: Vec<u8>,
        signature: &[u8],
        index: u32,
        session_id: &[u8],
    ) -> Result<Vec<u8>> {
        let public = self.inputs.group.get_key(index)?;
        if !self.is_started && &self.inputs.leader_key == public {
            self.is_started = true;
            self.send_deals().await?;
        }

        if self.inputs.index == index {
            debug!(parent: self.node.span(), "Ignoring broadcasted our packet, index {index}",

            );
            bail!("duplicated packed")
        }

        if self.inputs.session_id != session_id {
            bail!("invalid session_id")
        };

        if S::schnorr_verify(public, &hash, signature).is_err() {
            bail!("signature not valid")
        }
        Ok(hash)
    }

    async fn process_deals(&mut self) {
        // XXX: 'spawn_blocking' might be overkill here
        let node = Arc::clone(&self.node);
        let local_index = self.inputs.index;
        let group_node_indexes = self
            .inputs
            .group
            .nodes
            .iter()
            .map(|node| node.index())
            .collect::<Vec<u32>>();
        let self_sets_deal = self.sets.deal.vals.to_owned();
        let threshold = self.inputs.group.threshold as usize;

        let blocking_task = tokio::task::spawn_blocking(move || {
            let mut ext_deals_bad = HashSet::new();
            let mut ext_distributed = HashMap::new();

            let mut seen_index: HashSet<u32> = HashSet::new();

            for (bundle, _) in self_sets_deal.values() {
                if bundle.commits.is_empty() || bundle.commits.len() != threshold {
                    // invalid public polynomial is clearly cheating
                    ext_deals_bad.insert(bundle.dealer_index);
                    warn!(parent: node.span(), "bad deal: index: {}, bundle.commits.len: {}, threshold: {threshold}",bundle.dealer_index, bundle.commits.len());
                    continue;
                }
                if seen_index.contains(&bundle.dealer_index) {
                    // already saw a bundle from the same dealer - clear sign of cheating
                    ext_deals_bad.insert(bundle.dealer_index);
                    warn!(parent: node.span(), "bad deal: index: {}, duplicate bundle",bundle.dealer_index);
                    continue;
                }
                seen_index.insert(bundle.dealer_index);

                match PubPoly::<S>::deserialize(&bundle.commits) {
                    Ok(pub_poly) => {
                        for deal in &bundle.deals {
                            if !group_node_indexes
                                .iter()
                                .any(|index| index == &deal.share_index)
                            {
                                // invalid index for share holder is a clear sign of cheating
                                // and we don't even need to look at the rest
                                warn!(parent: node.span(), "bad deal: index: {}, invalid index for share holder", bundle.dealer_index);
                                ext_deals_bad.insert(bundle.dealer_index);
                                continue;
                            }

                            if deal.share_index != local_index {
                                // we dont look at other's shares
                                continue;
                            }

                            let Ok(share) =
                                S::decrypt(&node.keypair.private(), &deal.encrypted_share)
                            else {
                                 warn!(parent: node.span(), "bad deal: index: {}, invalid schnorr signature", bundle.dealer_index);
                                ext_deals_bad.insert(bundle.dealer_index);
                                continue;
                            };

                            // check if share is valid w.r.t. public commitment
                            let comm: PublicKey<S> = pub_poly.eval(local_index).v;
                            let comm_share = S::sk_to_pk(&share);

                            if comm != comm_share {
                                warn!(parent: node.span(),
                                    "Deal share invalid wrt public poly, dealer index: {}",
                                    bundle.dealer_index
                                );
                                continue;
                            }
                            info!(parent: node.span(), "Valid deal processed received from dealer: {}",
                                bundle.dealer_index
                            );
                            ext_distributed
                                .insert(bundle.dealer_index, (share, pub_poly.commits.to_owned()));
                        }
                    }
                    Err(err) => {
                        ext_deals_bad.insert(bundle.dealer_index);
                        error!(parent: node.span(),
                            "invalid pub_poly from dealer index: {}, err: {err:?}",
                            bundle.dealer_index
                        );
                        continue;
                    }
                };
            }
            (ext_deals_bad, ext_distributed)
        }).await;
        if let Ok((deals_bad, distributed)) = blocking_task {
            self.sets.deal.bad.extend(deals_bad);
            self.distributed = distributed
        } else {
            error!("TODO: At this error dkg must be cancelled and DkgState set to NonActive")
        }
    }

    async fn send_responses(&mut self) {
        let now = Instant::now();

        let mut responses: Vec<Response> = vec![];
        for node in &self.inputs.group.nodes {
            if self.sets.deal.bad.contains(&node.index()) {
                responses.push(Response {
                    dealer_index: node.index(),
                    status: false,
                })
            } else {
                responses.push(Response {
                    dealer_index: node.index(),
                    status: true,
                })
            }
        }

        let mut bundle = ResponseBundle {
            share_index: self.inputs.index,
            responses,
            session_id: self.inputs.session_id.to_owned(),
            signature: vec![],
        };
        let hash = bundle.hash();
        bundle.signature = S::schnorr_sign(&self.node.keypair.private(), &hash).unwrap();

        let new_now = tokio::time::Instant::now();

        if let Err(e) = self
            .broadcast
            .send(Bundle::Response(bundle), &self.node.beacon_id)
        {
            error!(parent: self.node.span(), "generator: {e}")
        }
        trace!(parent: self.node.span(), "MEASURE: send_responses: {:?}", new_now.checked_duration_since(now).unwrap());
    }

    fn process_responces(&mut self) {
        // only check that all responces are good
        for val in self.sets.resp.vals.iter() {
            for resp in val.1 .0.responses.iter() {
                if !resp.status {
                    error!(parent: self.node.span(),"invalid response for dealer index {}", resp.dealer_index)
                }
            }
        }
    }

    fn save_distributed(mut self) -> Result<(ChainConfig, Vec<String>)> {
        let mut dist_commits: Vec<<<S as Scheme>::Key as energon::traits::Group>::Projective> =
            Vec::with_capacity(self.inputs.group.threshold as usize);

        for c in self.pub_poly.commits.iter() {
            dist_commits.push(c.into())
        }

        let mut dist_share = self.pri_poly.eval(self.inputs.index).v;
        for (share, commits) in self.distributed.values() {
            for (i, value) in commits.iter().enumerate() {
                dist_commits[i] += value
            }
            dist_share += share;
        }

        self.inputs.group.dist_key = dist_commits.into_iter().map(|c| c.into()).collect();
        let dist_key_share = DistKeyShare::<S>::new(
            // TODO: it can be done without cloning
            self.inputs.group.dist_key.clone(),
            PriShare {
                i: self.inputs.index,
                v: dist_share,
            },
        );

        match self
            .node
            .fs
            .save_distributed(&self.inputs.group, &dist_key_share)
        {
            Ok(path) => {
                info!(parent: self.node.span(), "distributed share saved at {}", path.display())
            }
            Err(e) => bail!("can't save distributed share: {e:?}"),
        };
        // make config for chain
        let nodes_uri = self
            .inputs
            .group
            .nodes
            .iter()
            .filter(|node| node.index() != self.inputs.index)
            .map(|node| node.identity().address().to_owned())
            .collect::<Vec<String>>();

        let chain_config = ChainConfig {
            group_size: self.inputs.group.nodes.len(),
            threshold: self.inputs.group.threshold,
            period: self.inputs.group.period,
            genesis_time: self.inputs.group.genesis_time,
            genesis_seed: self.inputs.group.genesis_seed,
        };

        Ok((chain_config, nodes_uri))
    }
}

#[derive(Debug)]
struct Sets {
    span: Span,
    group_size: usize,
    deal: Set<DealBundle>,
    resp: Set<ResponseBundle>,
    just: Set<JustificationBundle>,
}
impl Sets {
    fn new(group_size: usize, span: Span) -> Self {
        Self {
            span,
            group_size,
            deal: Set::new(),
            resp: Set::new(),
            just: Set::new(),
        }
    }

    fn is_fast_move(&self, phase: Phase) -> bool {
        match phase {
            Phase::Deal => {
                if self.deal.vals.len() == self.group_size - 1 {
                    info!(parent: &self.span,
                        "Sets: fast moving to responce phase, collected {} deals",
                        self.deal.vals.len()
                    );
                    true
                } else {
                    false
                }
            }
            Phase::Response => {
                if self.resp.vals.len() == self.group_size - 1 {
                    info!(parent: &self.span,
                        "Sets: fast moving to justification phase, collected {} responces",
                        self.resp.vals.len()
                    );
                    true
                } else {
                    false
                }
            }
            Phase::Justification => self.just.vals.is_empty(),
        }
    }
}

#[derive(Debug)]
struct Set<B> {
    vals: HashMap<u32, (B, Vec<u8>)>,
    bad: HashSet<u32>,
}

impl<B> Set<B> {
    fn new() -> Self {
        Self {
            vals: HashMap::new(),
            bad: HashSet::new(),
        }
    }

    fn is_bad(&self, idx: u32) -> bool {
        self.bad.iter().any(|i| i == &idx)
    }

    fn push(
        &mut self,
        idx: u32,
        hash: Vec<u8>,
        bundle: B,
        bundle_name: &str,
        span: &Span,
    ) -> anyhow::Result<bool> {
        if self.is_bad(idx) {
            bail!("Packet is bad")
        }

        if let Some((_, prev_hash)) = self.vals.get(&idx) {
            if &hash != prev_hash {
                self.vals.remove(&idx);
                self.bad.insert(idx);
                bail!("Packet is bad - hashes mismatch")
            }
            info!(parent: span,
                "Sets: already seen index {idx} {bundle_name} bundle",
            );
            return Ok(false);
        }

        self.vals.insert(idx, (bundle, hash));
        info!(parent: span,
            "Sets: valid {bundle_name} bundle processed, index {}, accumulated total: {:?}", idx, self.vals.keys()
        );

        Ok(true)
    }
}

impl PartialEq for DkgState {
    fn eq(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (DkgState::NonActive, DkgState::NonActive)
                | (DkgState::Running(_), DkgState::Running(_))
                | (DkgState::Initiated { .. }, DkgState::Initiated { .. })
        )
    }
}

pub trait BundleHash {
    type Hasher;
    fn hash(&mut self) -> Vec<u8>;
}

impl BundleHash for DealBundle {
    type Hasher = Sha256;

    fn hash(&mut self) -> Vec<u8> {
        self.deals.sort_by_key(|deal| deal.share_index);

        let mut h = Self::Hasher::new();
        h.update(self.dealer_index.to_be_bytes());
        self.commits.iter().for_each(|commit| h.update(commit));

        self.deals.iter().for_each(|deal| {
            h.update(deal.share_index.to_be_bytes());
            h.update(&deal.encrypted_share);
        });
        h.update(&self.session_id);

        h.finalize().to_vec()
    }
}

impl BundleHash for ResponseBundle {
    type Hasher = Sha256;

    fn hash(&mut self) -> Vec<u8> {
        self.responses.sort_by_key(|r| r.dealer_index);

        let mut h = Self::Hasher::new();
        h.update(self.share_index.to_be_bytes());
        self.responses.iter().for_each(|responce| {
            h.update(responce.dealer_index.to_be_bytes());
            h.update((responce.status as u8).to_be_bytes())
        });
        h.update(&self.session_id);
        h.finalize().to_vec()
    }
}

impl BundleHash for JustificationBundle {
    type Hasher = Sha256;

    fn hash(&mut self) -> Vec<u8> {
        self.justifications.sort_by_key(|j| j.share_index);

        let mut h = Sha256::new();
        h.update(self.dealer_index.to_be_bytes());
        self.justifications.iter().for_each(|justification| {
            h.update(justification.share_index.to_be_bytes());
            h.update(&justification.share);
        });

        h.finalize().to_vec()
    }
}

pub fn load_secret_cmd() -> Result<String> {
    use std::env;

    let secret = env::var("DRAND_SHARE_SECRET")
        .map_err(|e| anyhow::anyhow!("can't read $DRAND_SHARE_SECRET: {e}"))?;

    if secret.is_empty() {
        bail!("no secret specified for share")
    }

    if secret.len() < MINIMUM_SHARE_SECRET_LENGTH {
        bail!("secret is insecure. Should be at least {MINIMUM_SHARE_SECRET_LENGTH} characters",)
    }
    Ok(secret)
}

use core::fmt::Display;
impl Display for Phase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let varint = match self {
            Phase::Deal => "deal",
            Phase::Response => "response",
            Phase::Justification => "justification",
        };
        write!(f, "{varint}")
    }
}
