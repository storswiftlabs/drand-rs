pub const ROUNDS_UNTIL_TRANSITION: u64 = 10;

/// Calculates the active round at `now`
pub fn current_round(now: i64, period: u32, genesis: i64) -> u64 {
    let (next_round, _) = next_round(now, period, genesis);
    if next_round <= 1 {
        next_round
    } else {
        next_round - 1
    }
}

/// Returns the next upcoming round and its UNIX time given the genesis
/// time and the period. Round at time genesis = round 1. Round 0 is fixed.
pub fn next_round(now: i64, period: u32, genesis: i64) -> (u64, i64) {
    if now < genesis {
        return (1, genesis);
    }
    let from_genesis = now - genesis;
    // we take the time from genesis divided by the periods in seconds, that
    // gives us the number of periods since genesis. We add +1 since we want the
    // next round. We also add +1 because round 1 starts at genesis time.
    let next_round = (from_genesis as f64 / period as f64).floor() as u64 + 1;
    let next_time = genesis + (next_round as i64 * period as i64);

    (next_round + 1, next_time)
}

// Returns the time the `round` should happen
pub fn time_of_round(period: u32, genesis: i64, round: u64) -> i64 {
    if round == 0 {
        return genesis;
    }
    // - 1 because genesis time is for 1st round already
    let delta = (round - 1) * period as u64;

    genesis + delta as i64
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;

    #[test]
    fn time_basic() {
        let period = 3;
        let round = 22;
        let transition_round = round + ROUNDS_UNTIL_TRANSITION;

        assert_eq!(round, current_round(1745308647, period, 1745308582));
        assert_eq!(round, current_round(1745308824, period, 1745308759));
        assert_eq!(round, current_round(1745309210, period, 1745309145));
        assert!(1745308675 == time_of_round(period, 1745308582, transition_round));
        assert!(1745308852 == time_of_round(period, 1745308759, transition_round));
        assert!(1745309238 == time_of_round(period, 1745309145, transition_round));
    }

    #[test]
    fn test_chain_next_round() {
        let period = 2;
        let mut now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs() as i64;
        let genesis = now + 1;

        // move to genesis round
        now += 1;
        let (round, round_time) = next_round(now, period, genesis);
        assert_eq!(round, 2);
        let exp_time = genesis + period as i64;
        assert_eq!(exp_time, round_time);
        assert_eq!(exp_time, time_of_round(period, genesis, 2));

        // move to one second
        now += 1;
        let (nround, nround_time) = next_round(now, period, genesis);
        assert_eq!(round, nround);
        assert_eq!(round_time, nround_time);

        // move to next round
        now += 1;
        let (round, round_time) = next_round(now, period, genesis);
        let exp_time = genesis + period as i64 * 2;
        assert_eq!(round, 3);
        assert_eq!(round_time, exp_time);
        assert_eq!(exp_time, time_of_round(period, genesis, 3))
    }
}
