use super::epoch::EpochConfig;
use crate::key::Scheme;
use crate::protobuf::drand::PartialBeaconPacket;

use energon::kyber::tbls::SigShare;
use std::collections::VecDeque;
use tracing::debug;
use tracing::warn;
use tracing::Span;

/// Partials that are up to this amount of rounds more than the last
/// beacon we have - it is useful for quick catchup.
pub const CACHE_LIMIT_ROUNDS: u64 = 3;

/// Round cache contains [`PartialBeaconPacket`] received from nodes for given round.
#[derive(Debug)]
struct RoundCacheUnchecked {
    round: u64,
    packets: Vec<PartialBeaconPacket>,
}

/// Partial cache holds 2 types of shares:
///  - [`PartialBeaconPacket`] unchecked BLS signature .
///  - [`SigShare`] checked BLS signature .
///
/// Partial cache structure:
///                  n           n + 1       n + 2 .. n + 2 + `CACHE_LIMIT_ROUNDS`
///     Round: `latest_stored`|  current   |  `CACHE_LIMIT_ROUNDS`
///     Type:        -        | `SigShare` |  `PartialBeaconPacket`
#[derive(Default, Debug)]
pub struct PartialCache<S: Scheme> {
    thr: usize,
    last: u64,
    valid_sigs: Vec<SigShare<S>>,
    rounds_cache: VecDeque<RoundCacheUnchecked>,
}

impl<S: Scheme> PartialCache<S> {
    pub fn new(latest_stored: u64, thr: usize) -> Self {
        let unchecked = (latest_stored + 2..latest_stored + 2 + CACHE_LIMIT_ROUNDS)
            .map(|i| RoundCacheUnchecked {
                round: i,
                packets: Vec::with_capacity(thr),
            })
            .collect();

        Self {
            valid_sigs: Vec::with_capacity(thr),
            last: latest_stored,
            thr,
            rounds_cache: unchecked,
        }
    }

    /// Adds packet into the corresponding round cache if the round of packet is allowed
    /// and signature of packet is not duplicated.
    ///
    /// Cache _might_ contain more than one packet with same share index for given round.
    /// This is possible if leaved node is not in proper state: old index collided with new index of some valid node.
    /// Such invalid packets will be discarded at BLS signature check.
    pub fn add_packet(&mut self, packet: PartialBeaconPacket) -> Option<bool> {
        if let Some(r_cache) = self
            .rounds_cache
            .iter_mut()
            // Search within `CACHE_LIMIT_ROUNDS`.
            .find(|r_cache| r_cache.round == packet.round)
        {
            // Round is allowed, packet is duplicated (ignored).
            if r_cache
                .packets
                .iter()
                .any(|i| i.partial_sig == packet.partial_sig)
            {
                Some(false)
            } else {
                // Round allowed, packet is stored.
                r_cache.packets.push(packet);

                Some(true)
            }
        } else {
            // Round not allowed, packet is ignored.
            None
        }
    }

    /// Returns `true` if sigshare with given index is already stored in cache.
    pub fn is_share_present(&self, index: u32) -> bool {
        self.valid_sigs.iter().any(|s| s.index() == index)
    }

    /// Adds sigshare to partial cache. Returns slice of sigshares *sorted by index* if their number hits the threshold.
    ///
    /// WARNING: bls signature validity and round value must be prechecked on caller side.
    pub fn add_prechecked(&mut self, valid_sig: SigShare<S>) -> Option<&[SigShare<S>]> {
        self.valid_sigs.push(valid_sig);
        if self.valid_sigs.len() < self.thr {
            return None;
        }
        self.valid_sigs.sort_by_key(SigShare::index);
        Some(self.valid_sigs.as_slice())
    }

    /// Updates cache state if argument is bigger than cache metadata.
    /// Returns packets to verify for `latest_stored +1` round if such packets exists.
    fn update(&mut self, latest_stored: u64) -> Option<Vec<PartialBeaconPacket>> {
        // Valid case, cache is black box for caller side.
        if latest_stored <= self.last {
            return None;
        }
        // Gap between "old" and "new" latest_stored round.
        let gap = latest_stored - self.last;
        if let 1..=CACHE_LIMIT_ROUNDS = gap {
            // Clear sigshares for outdated round.
            self.valid_sigs.clear();
            self.last = latest_stored;

            // Add entries for new allowed rounds.
            for i in 1..=gap {
                let new_allowed_round = latest_stored + i + CACHE_LIMIT_ROUNDS;
                self.rounds_cache.push_back(RoundCacheUnchecked {
                    round: new_allowed_round,
                    packets: Vec::with_capacity(self.thr),
                });
            }

            let mut packets_next_round = None;
            for i in 0..gap {
                // Last removed entry is the cache for new `latest_stored +1` round.
                if i == gap - 1 {
                    packets_next_round = self.rounds_cache.pop_front();
                }
            }

            // Return packets to validate into `SigShare` if there are some.
            packets_next_round
                .filter(|r_cache| !r_cache.packets.is_empty())
                .map(|r_cache| r_cache.packets)
        } else {
            // Gap is too big, cache is completely outdated.
            *self = Self::new(latest_stored, self.thr);
            None
        }
    }

    /// Aligns cache for new `latest_stored` round, verifying unchecked packets for next round.
    pub fn align(&mut self, ec: &EpochConfig<S>, latest_stored: u64, l: &Span) {
        if let Some(next_round_packets) = self.update(latest_stored) {
            for packet in next_round_packets {
                let Some(idx) = get_partial_index::<S>(&packet.partial_sig) else {
                    warn!(parent: l, "align_cache: ignoring packet with invalid data");
                    continue;
                };

                if !self.valid_sigs.iter().any(|s| s.index() == idx) {
                    match ec.verify_partial(&packet) {
                        Ok((valid_sigshare, node_addr)) => {
                            self.valid_sigs.push(valid_sigshare);
                            debug!(parent: l, "align_cache: added valid share from {node_addr} for round {}, latest_stored {}", packet.round, latest_stored);
                        }
                        Err(err) => {
                            debug!(parent: l, "align_cache: {err}, latest_stored {}, packet_round {}", self.last, packet.round,);
                            continue;
                        }
                    }
                }
            }
        }
    }
}

pub fn get_partial_index<S: Scheme>(partial: &[u8]) -> Option<u32> {
    let expected = <S::Sig as energon::traits::Group>::POINT_SIZE + 2;
    let received = partial.len();
    if received != expected {
        return None;
    }
    // XXX: consider u8 instead of u32.
    Some(u32::from_be_bytes([0, 0, partial[0], partial[1]]))
}

#[cfg(test)]
mod test {
    use super::*;
    use energon::drand::schemes::DefaultScheme;

    /// Returns rounds acceptable by cache.
    fn allowed_rounds(cache: &VecDeque<RoundCacheUnchecked>) -> Vec<u64> {
        cache.iter().map(|r_cache| r_cache.round).collect()
    }

    /// Inserts all packets to cache.
    fn insert_all<S: Scheme>(cache: &mut PartialCache<S>, packets: Vec<PartialBeaconPacket>) {
        for p in packets {
            cache.add_packet(p);
        }
    }

    /// Panics if 3 expected packets are not present in cache.
    fn all_present<S: Scheme>(cache: &PartialCache<S>, expected: [&PartialBeaconPacket; 3]) {
        assert!(cache.rounds_cache.len() == usize::try_from(CACHE_LIMIT_ROUNDS).unwrap());

        for beacon in expected {
            let found = cache.rounds_cache.iter().any(|r_cache| {
                assert!(r_cache.packets.len() == 1);
                // Packets are checked against round and partial signature.
                r_cache.round == beacon.round && r_cache.packets.iter().any(|p| p == beacon)
            });
            assert!(
                found,
                "expected beacon with round {:?} not found in cache.",
                beacon.round
            );
        }
    }

    fn continuous_cache_update<S: Scheme>(
        p_cache: &mut PartialCache<S>,
        packets: &[PartialBeaconPacket],
        rounds: std::ops::RangeInclusive<u64>,
    ) {
        for latest_stored in rounds {
            assert_eq!(
                p_cache.update(latest_stored).unwrap()[0],
                packets[usize::try_from(latest_stored).unwrap() + 1]
            );

            assert_eq!(
                allowed_rounds(&p_cache.rounds_cache),
                vec![latest_stored + 2, latest_stored + 3, latest_stored + 4]
            );

            for p in packets {
                p_cache.add_packet(p.clone());
            }
            let latest = usize::try_from(latest_stored).unwrap();

            let expected = [
                &packets[latest + 2],
                &packets[latest + 3],
                &packets[latest + 4],
            ];
            all_present(p_cache, expected);
        }
    }

    #[test]
    fn cache_no_gaps() {
        let packets_total = 15u8;
        let p: Vec<PartialBeaconPacket> = (0..=packets_total)
            .map(|i| PartialBeaconPacket {
                round: u64::from(i),
                partial_sig: vec![i, i],
                previous_signature: vec![],
                metadata: None,
            })
            .collect();

        let thr = 2;
        let latest_stored = 0;
        let mut p_cache = PartialCache::<DefaultScheme>::new(latest_stored, thr);
        insert_all(&mut p_cache, p.clone());

        continuous_cache_update::<DefaultScheme>(
            &mut p_cache,
            &p,
            1..=(u64::from(packets_total) - 4),
        );
    }

    #[test]
    fn cache_with_gaps() {
        let thr = 7;

        let mut new_stored = 0;
        let mut p_cache = PartialCache::<DefaultScheme>::new(new_stored, thr);
        assert_eq!(allowed_rounds(&p_cache.rounds_cache), vec![2, 3, 4]);

        new_stored = 1;
        p_cache.update(new_stored);
        assert_eq!(p_cache.last, new_stored);
        assert_eq!(allowed_rounds(&p_cache.rounds_cache), vec![3, 4, 5]);

        new_stored = 2;
        p_cache.update(new_stored);
        assert_eq!(p_cache.last, new_stored);
        assert_eq!(allowed_rounds(&p_cache.rounds_cache), vec![4, 5, 6]);

        // Gaps are expected at syncing:
        // - new joiner may need to sync up to latest chain height.
        // - sync always started once node is 2 rounds behing the expected height.
        new_stored = 513;
        p_cache.update(new_stored);
        assert_eq!(allowed_rounds(&p_cache.rounds_cache), vec![515, 516, 517]);

        new_stored = 520;
        p_cache.update(new_stored);
        assert_eq!(allowed_rounds(&p_cache.rounds_cache), vec![522, 523, 524]);
    }
}
