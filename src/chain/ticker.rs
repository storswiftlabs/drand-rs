use super::time;
use crate::net::utils::Seconds;
use std::time::Duration;
use tokio::sync::mpsc;

type Round = u64;

struct RoundTicker {
    period: u32,
    genesis_time: u64,
    tx_next_round: mpsc::Sender<Round>,
}

impl RoundTicker {
    /// Sends next round value at next round time to associated receiver.
    async fn send_next_round(&self) -> Result<(), mpsc::error::SendError<Round>> {
        let utc_now = time::time_now();
        let (next_round, next_time) =
            time::next_round(utc_now.as_secs(), self.period, self.genesis_time);

        let sleep_duration = Duration::from_secs(next_time)
            .checked_sub(utc_now)
            .unwrap_or(Duration::ZERO);

        tokio::time::sleep(sleep_duration).await;
        self.tx_next_round.send(next_round).await
    }
}

/// Starts round ticker for given genesis time and period.
/// Returns associated receiver for new rounds.
pub fn start_ticker(genesis_time: u64, period: Seconds) -> mpsc::Receiver<Round> {
    let (tx_next_round, rx_next_round) = mpsc::channel(1);

    tokio::spawn(async move {
        let t = RoundTicker {
            period: period.get_value(),
            genesis_time,
            tx_next_round,
        };

        loop {
            if t.send_next_round().await.is_err() {
                break;
            }
        }
    });

    rx_next_round
}
