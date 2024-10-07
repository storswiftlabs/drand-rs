use super::beacon::InnerNode;
use crate::key::keys::DistKeyShare;
use crate::net::pool::PoolCmd;
use crate::protobuf::drand::PartialBeaconPacket;
use anyhow::bail;
use anyhow::Result;
use chrono::Utc;
use core::time::Duration;
use energon::drand::tbls::SigShare;
use energon::drand::BeaconDigest;
use energon::drand::Scheme;
use energon::traits::Affine;

use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::oneshot;
use tokio::time::Instant;
use tracing::{debug, error, info, trace, Span};

const BUFFER_PARTIAL: usize = 40;
type PartialMsg = (PartialBeaconPacket, oneshot::Sender<Result<()>>);
pub type ChainSender = mpsc::Sender<PartialMsg>;

pub struct ChainHandler<S: Scheme> {
    curr_round: u64,
    share: DistKeyShare<S>,
    threshold: usize,
    // Contains verified shares
    sigs_buf: Vec<SigShare<S>>,
    last_stored: LastStored,
    node: Arc<InnerNode<S>>,
}

struct LastStored {
    round: u64,
    sig: Vec<u8>,
    info: String,
    period: u32,
    genesis_time: u64,
}

impl LastStored {
    fn store_recovered(&mut self, round: u64, recovered: Vec<u8>) {
        self.info = hex::encode(&recovered[..3]);
        self.sig = recovered;
        self.round = round;
    }
    fn time_discrepancy_ms(&self, round: u64) -> u64 {
        Utc::now().timestamp_millis() as u64
            - (self.genesis_time + self.period as u64 * (round - 1)) * 1000
    }
}

impl<S: Scheme> ChainHandler<S> {
    pub fn start(config: ChainConfig, node: Arc<InnerNode<S>>) -> Result<ChainSender> {
        let share: DistKeyShare<S> = node.fs.load_share()?;
        let info = hex::encode(config.genesis_seed.get(..3).expect("infallible"));
        let (tx_partial, mut rx_partial) = mpsc::channel::<PartialMsg>(BUFFER_PARTIAL);
        tokio::spawn(async move {
            debug!(parent: node.span(), "chain_handler started");
            let mut next_round = Clock::init(config.genesis_time, config.period, node.span());

            let mut h = Self {
                curr_round: 0,
                share,
                threshold: config.threshold as usize,
                sigs_buf: Vec::with_capacity(config.group_size),

                last_stored: LastStored {
                    info,
                    round: 0,
                    sig: config.genesis_seed,
                    period: config.period,
                    genesis_time: config.genesis_time,
                },
                node,
            };

            '_chain_loop: loop {
                tokio::select! {

                    partial = rx_partial.recv()=> {
                        match partial{
                            Some((partial, callback))=>{
                                debug!(parent: h.node.span(), "chain: Received partial");
                                let _ = callback.send(h.add_partial(partial).await);
                            },
                        None=>break
                        }
                    }

                    round = next_round.recv()=>{
                        if let Some(round)=round{
                            h.new_round(round).await
                        }
                    }
                }
            }
            trace!(parent: h.node.span(), "stopped chain");
        });
        Ok(tx_partial)
    }

    async fn add_partial(&mut self, p: PartialBeaconPacket) -> Result<()> {
        let sig_sh = SigShare::deserialize(&p.partial_sig)?;
        if self
            .sigs_buf
            .iter()
            .any(|stored| stored.index() == sig_sh.index())
        {
            debug!(parent: self.node.span(), "ignoring dublicated partial, index: {}, round: {}", sig_sh.index(), p.round);
            return Ok(());
        }

        if self.last_stored.round == p.round || self.last_stored.round + 1 != p.round {
            debug!(parent: self.node.span(), "ignoring_partial: round: {}, last_stored.round: {}", p.round, self.last_stored.round);
            return Ok(());
        }

        let msg = S::Beacon::digest(&p.previous_signature, p.round);
        let key = &self.share.public().eval(sig_sh.index()).v;
        if S::bls_verify(key, sig_sh.value(), &msg).is_err() {
            bail!(
                "invalid bls signarure, index: {}, round: {}",
                sig_sh.index(),
                p.round
            );
        }

        debug!(parent: self.node.span(), "Added valid partial, index: {}, round: {}, last_stored.round: {}", sig_sh.index(), p.round, self.last_stored.round);
        self.try_recover(sig_sh, &msg);

        Ok(())
    }

    async fn new_round(&mut self, new_round: u64) {
        debug!(parent: self.node.span(), "beacon_loop: new_round {new_round}, lastbeacon: {}",self.last_stored.round);

        if self.last_stored.round == new_round - 1 {
            self.curr_round = new_round;

            let msg = S::Beacon::digest(&self.last_stored.sig, new_round);
            let our_partial = S::tbls_sign(self.share.private(), &msg).unwrap();
            let packet = PartialBeaconPacket {
                round: new_round,
                previous_signature: self.last_stored.sig.to_owned(),
                partial_sig: our_partial.serialize().unwrap(),
                metadata: From::from(&self.node.beacon_id),
            };

            self.try_recover(our_partial, &msg);

            let _ = self.node.pool.send(PoolCmd::Partial(packet)).await;
        } else {
            // TODO: here should be sync manager logic
            error!(parent: self.node.span(), "sync manager is not implemented:  {}, collected: {:?}",self.curr_round, self
            .sigs_buf
            .iter()
            .map(|s| s.index())
            .collect::<Vec<u32>>());

            let msg = S::Beacon::digest(&self.last_stored.sig, &self.last_stored.round + 1);
            let our_partial = S::tbls_sign(self.share.private(), &msg).unwrap();
            let packet = PartialBeaconPacket {
                round: new_round,
                previous_signature: self.last_stored.sig.to_owned(),
                partial_sig: our_partial.serialize().unwrap(),
                metadata: From::from(&self.node.beacon_id),
            };

            if !self
                .sigs_buf
                .iter()
                .any(|stored| stored.index() == our_partial.index())
            {
                self.try_recover(our_partial, &msg);
            }
            let _ = self.node.pool.send(PoolCmd::Partial(packet)).await;
        }
    }

    fn try_recover(&mut self, sig_sh: SigShare<S>, msg: &[u8]) {
        self.sigs_buf.push(sig_sh);
        if self.sigs_buf.len() == self.threshold && self.last_stored.round + 1 == self.curr_round {
            let recovered =
                S::recover_sig(self.share.public(), msg, &self.sigs_buf, self.threshold)
                    .unwrap()
                    .serialize()
                    .unwrap();

            let info = hex::encode(&recovered[..3]);
            info!(parent: self.node.span(), "NEW_BEACON_STORED: round: {}, sig: {info}, prevSig: {}, time_discrepancy_ms: {}",self.curr_round, self.last_stored.info, self.last_stored.time_discrepancy_ms(self.curr_round));

            self.last_stored.store_recovered(self.curr_round, recovered);
            self.sigs_buf.clear();
        }
    }
}

pub struct ChainConfig {
    pub group_size: usize,
    pub threshold: u32,
    /// period in seconds
    // TODO: introduce type Second(u32)
    pub period: u32,
    pub genesis_time: u64,
    pub genesis_seed: Vec<u8>,
}

struct Clock {
    period: Duration,
    genesis_time: u64,
    next_round: mpsc::Sender<u64>,
    span: Span,
}

impl Clock {
    //
    fn init(genesis_time: u64, period: u32, span: &Span) -> mpsc::Receiver<u64> {
        let (tx_next_round, rx_next_round) = mpsc::channel::<u64>(1);
        let clock = Self {
            period: Duration::from_secs(period.into()),
            genesis_time,
            next_round: tx_next_round,
            span: span.to_owned(),
        };

        tokio::spawn(async move {
            loop {
                if clock.next_round().await.is_err() {
                    break;
                }
            }
        });

        rx_next_round
    }

    /// Sleeps until next round time, than sends next round value to receiver in '_chain_loop.
    pub async fn next_round(&self) -> Result<(), SendError<u64>> {
        let time_now = Utc::now().timestamp() as u64;

        if time_now < self.genesis_time {
            debug!(parent: &self.span, "clock: waiting genesis_time: {}, time now: {time_now}", self.genesis_time, );
            tokio::time::sleep_until(
                Instant::now() + Duration::from_secs(self.genesis_time - time_now),
            )
            .await; // - Duration::from_secs(self.genesis_time - time_now)).await;
            debug!(parent: &self.span, "clock: NOW genesis woke up: {}",Utc::now().timestamp());
            self.next_round.send(1).await?;
            return Ok(());
        }
        let from_genesis = time_now - self.genesis_time;
        // we take the time from genesis divided by the periods in seconds, that
        // gives us the number of periods since genesis. We add +1 since we want the
        // next round. We also add +1 because round 1 starts at genesis time.
        let next_round = (from_genesis as f64 / self.period.as_secs_f64()).floor() as u64 + 1;
        let next_time = self.genesis_time + (next_round * self.period.as_secs());
        debug!(parent: &self.span, "clock: next_round: {}, next_time: {next_time}, time now: {time_now}",next_round + 1);
        tokio::time::sleep(Duration::from_secs(next_time - time_now)).await;
        self.next_round.send(next_round + 1).await?;
        Ok(())
    }
}
