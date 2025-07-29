use super::time;
use crate::net::utils::Seconds;

use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::SendError;

struct Ticker {
    period: u32,
    genesis_time: u64,
    // RoundInfo is required for top chain abstractions (handler, syncer).
    tx_next_round: broadcast::Sender<RoundInfo>,
}

#[derive(Clone, Copy)]
pub struct RoundInfo {
    pub round: u64,
    // XXX: remove if no use case.
    #[allow(dead_code)]
    pub time: u64,
}

impl RoundInfo {
    pub fn now(period: u32, genesis: u64) -> Self {
        let now = time::time_now().as_secs();
        let (next_round, next_time) = time::next_round(now, period, genesis);

        Self {
            round: next_round - 1,
            time: next_time - period as u64,
        }
    }
}

impl Ticker {
    /// Broadcasts next round value at next round time to associated receivers.
    async fn send_next_round(&self) -> Result<(), SendError<RoundInfo>> {
        let time_now = time::time_now();
        let (next_round, next_time) =
            time::next_round(time_now.as_secs(), self.period, self.genesis_time);

        let info = RoundInfo {
            round: next_round,
            time: next_time,
        };

        let sleep_duration = Duration::from_secs(next_time)
            .checked_sub(time_now)
            .unwrap_or(Duration::ZERO);

        tokio::time::sleep(sleep_duration).await;
        let _ = self.tx_next_round.send(info)?;

        Ok(())
    }
}

/// Public handle for internal [`Ticker`].
pub struct RoundTicker {
    tx_new_round: broadcast::Sender<RoundInfo>,
}

impl RoundTicker {
    pub fn new() -> Self {
        let (tx_new_round, _) = broadcast::channel(2);
        Self { tx_new_round }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<RoundInfo> {
        self.tx_new_round.subscribe()
    }

    /// Starts the ticker for given genesis time and period.
    pub fn start(self, genesis_time: u64, period: Seconds) -> broadcast::Receiver<RoundInfo> {
        let rx_round = self.tx_new_round.subscribe();

        tokio::spawn(async move {
            let t = Ticker {
                period: period.get_value(),
                genesis_time,
                tx_next_round: self.tx_new_round,
            };

            loop {
                if t.send_next_round().await.is_err() {
                    break;
                }
            }
        });

        rx_round
    }
}
