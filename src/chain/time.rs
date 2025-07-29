use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

pub const ROUNDS_UNTIL_TRANSITION: u64 = 10;

/// Calculates the active round at `now`.
pub fn current_round(now: u64, period: u32, genesis: u64) -> u64 {
    let (next_round, _) = next_round(now, period, genesis);
    if next_round <= 1 {
        next_round
    } else {
        next_round - 1
    }
}

/// Returns the next upcoming round and its UNIX time given the genesis
/// time and the period. Round at time genesis = round 1. Round 0 is fixed.
pub fn next_round(now: u64, period: u32, genesis: u64) -> (u64, u64) {
    if now < genesis {
        return (1, genesis);
    }
    let from_genesis = now - genesis;
    // We take the time from genesis divided by the periods in seconds, that
    // gives us the number of periods since genesis. We add +1 since we want the next round.
    // We also add +1 because round 1 starts at genesis time.
    let next_round = (from_genesis as f64 / period as f64).floor() as u64 + 1;
    let next_time = genesis + (next_round * period as u64);

    (next_round + 1, next_time)
}

/// Returns the time the `round` should happen.
pub fn time_of_round(period: u32, genesis: u64, round: u64) -> u64 {
    if round == 0 {
        return genesis;
    }

    // - 1 because genesis time is for 1st round already.
    let delta = (round - 1) * period as u64;
    genesis + delta
}

/// Returns current Unix time.
pub fn time_now() -> Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before Unix epoch")
}

#[cfg(test)]
mod test {
    use super::*;

    /// Test case from <https://github.com/drand/drand/blob/release/v2.1.0/common/time_test.go#L39-L70>
    #[test]
    fn test_chain_next_round() {
        let period = 2;
        let mut now = time_now().as_secs();
        let genesis = now + 1;

        // Move to genesis round
        now += 1;
        let (round, round_time) = next_round(now, period, genesis);
        assert_eq!(round, 2);
        let exp_time = genesis + period as u64;
        assert_eq!(exp_time, round_time);
        assert_eq!(exp_time, time_of_round(period, genesis, 2));

        // Move to one second
        now += 1;
        let (nround, nround_time) = next_round(now, period, genesis);
        assert_eq!(round, nround);
        assert_eq!(round_time, nround_time);

        // Move to next round
        now += 1;
        let (round, round_time) = next_round(now, period, genesis);
        let exp_time = genesis + period as u64 * 2;
        assert_eq!(round, 3);
        assert_eq!(round_time, exp_time);
        assert_eq!(exp_time, time_of_round(period, genesis, 3))
    }
}
