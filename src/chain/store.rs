// Copyright 2023-2025 StorSwift Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module implements an actor pattern for a *trimmed* chain store
//! using [rusqlite] under the hood.
use crate::{
    core::beacon::BeaconID,
    log::Logger,
    net::utils::Callback,
    protobuf::drand::{BeaconPacket, Metadata},
    {error, warn},
};
use rusqlite::{params, Connection, Error, OpenFlags};
use std::path::{Path, PathBuf};
use tokio::{sync::mpsc, task};

/// Number of beacons retrieved in a single query from chain DB.
const BATCH_SIZE: u64 = 300;
/// File is stored under `<base_folder>/multibeacon/<beacon_id>/db/DB_NAME`
const DB_NAME: &str = "rusqlite.db";

pub type StoreStreamResponse = Result<BeaconPacket, tonic::Status>;

/// Inner beacon representation for chained schemes.
#[derive(Clone, PartialEq)]
pub struct ChainedBeacon {
    round: u64,
    signature: Vec<u8>,
    previous_signature: Vec<u8>,
}

/// Inner beacon representation for unchained schemes.
#[derive(Clone, PartialEq)]
pub struct UnChainedBeacon {
    round: u64,
    signature: Vec<u8>,
}

/// Abstracts chained / unchained beacons into same API.
#[allow(private_bounds)]
pub trait BeaconRepr: 'static + Executor + Sized + Send + Sync + Clone {
    fn new(prev: &Self, recovered_sig: Vec<u8>) -> Self;
    fn round(&self) -> u64;
    fn signature(&self) -> &[u8];
    fn prev_signature(&self) -> Option<&[u8]>;
    fn from_packet(p: BeaconPacket) -> Self;
    fn from_seed(genesis_seed: Vec<u8>) -> Self;
    fn short_sig(&self) -> String {
        hex::encode(self.signature().get(..3).unwrap_or_default())
    }
    fn short_prev_sig(&self) -> Option<String> {
        self.prev_signature()
            .map(|p_sig| hex::encode(p_sig.get(..3).unwrap_or_default()))
    }
}

impl BeaconRepr for ChainedBeacon {
    /// WARNING: monotonic round check for previous beacon is shifted to caller side.
    fn new(prev: &Self, new_sig: Vec<u8>) -> Self {
        Self {
            round: prev.round + 1,
            signature: new_sig,
            previous_signature: prev.signature.clone(),
        }
    }

    fn round(&self) -> u64 {
        self.round
    }

    fn signature(&self) -> &[u8] {
        &self.signature
    }

    fn prev_signature(&self) -> Option<&[u8]> {
        Some(&self.previous_signature)
    }

    fn from_packet(p: BeaconPacket) -> Self {
        let BeaconPacket {
            previous_signature,
            round,
            signature,
            metadata: _,
        } = p;
        Self {
            round,
            signature,
            previous_signature,
        }
    }

    fn from_seed(genesis_seed: Vec<u8>) -> Self {
        Self {
            round: 0,
            signature: genesis_seed,
            previous_signature: vec![0],
        }
    }
}

impl BeaconRepr for UnChainedBeacon {
    /// WARNING: monotonic round check for previous beacon is shifted to caller side.
    fn new(prev: &Self, new_sig: Vec<u8>) -> Self {
        Self {
            round: prev.round + 1,
            signature: new_sig,
        }
    }

    fn round(&self) -> u64 {
        self.round
    }

    fn signature(&self) -> &[u8] {
        &self.signature
    }

    fn prev_signature(&self) -> Option<&[u8]> {
        None
    }

    fn from_packet(p: BeaconPacket) -> Self {
        Self {
            round: p.round,
            signature: p.signature,
        }
    }

    fn from_seed(genesis_seed: Vec<u8>) -> Self {
        Self {
            round: 0,
            signature: genesis_seed,
        }
    }
}

/// SQL statement executor for [`BeaconRepr`].
trait Executor: Sized {
    // This is the **trimmed** DB implementation: tables are the same for all schemes.
    fn open(path: &Path) -> Result<Connection, Error> {
        let conn = Connection::open(path.join(DB_NAME))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS beacons (
            round INTEGER PRIMARY KEY,
            signature BLOB NOT NULL
        ) WITHOUT ROWID",
            [],
        )?;

        Ok(conn)
    }
    fn get(conn: &Connection, round: u64) -> Result<Self, Error>;
    fn put(self, conn: &mut Connection) -> Result<(), Error>;
    fn last(conn: &Connection) -> Result<Self, Error>;
    fn get_batch_proto(
        conn: &Connection,
        from_round: u64,
        id: &str,
    ) -> Result<Vec<BeaconPacket>, Error>;
}

impl Executor for ChainedBeacon {
    fn get(conn: &Connection, round: u64) -> Result<Self, Error> {
        if round > 0 {
            let mut stmt = conn.prepare_cached(
                "SELECT 
            signature,
            (SELECT signature FROM beacons WHERE round = b.round - 1)
         FROM beacons b
         WHERE round = ?1",
            )?;
            stmt.query_row([round], |row| {
                Ok(Self {
                    round,
                    signature: row.get(0)?,
                    previous_signature: row.get(1)?,
                })
            })
        } else {
            let mut stmt = conn.prepare_cached(
                "SELECT 
            signature
         FROM beacons b
         WHERE round = ?1",
            )?;
            stmt.query_row([round], |row| {
                Ok(Self {
                    round,
                    signature: row.get(0)?,
                    previous_signature: vec![],
                })
            })
        }
    }

    fn put(self, conn: &mut Connection) -> Result<(), Error> {
        let tr = conn.transaction()?;
        {
            let mut stmt =
                tr.prepare_cached("INSERT INTO beacons (round, signature) VALUES (?1, ?2)")?;
            stmt.execute(params![self.round, &self.signature,])?;
        }
        tr.commit()
    }

    fn last(conn: &Connection) -> Result<Self, Error> {
        let mut stmt = conn.prepare_cached(
            "SELECT 
            round, 
            signature,
            (SELECT signature FROM beacons WHERE round = b.round - 1)
         FROM beacons b
         WHERE round = (SELECT MAX(round) FROM beacons)",
        )?;

        stmt.query_row([], |row| {
            let round = row.get(0)?;
            let signature = row.get(1)?;
            let previous_signature = row.get(2).unwrap_or_default();

            Ok(Self {
                round,
                signature,
                previous_signature,
            })
        })
    }

    fn get_batch_proto(
        conn: &Connection,
        from_round: u64,
        id: &str,
    ) -> Result<Vec<BeaconPacket>, Error> {
        let mut prev_sig = if from_round == 0 {
            vec![]
        } else {
            let prev_round = from_round - 1;
            let mut stmt = conn.prepare_cached("SELECT signature FROM beacons WHERE round = ?1")?;
            stmt.query_row([prev_round], |row| row.get(0))?
        };

        conn.prepare_cached(
            "SELECT round, signature 
         FROM beacons 
         WHERE round >= ?1 
         ORDER BY round ASC 
         LIMIT ?2",
        )?
        .query_map([from_round, BATCH_SIZE], |row| {
            let round = row.get(0)?;
            let signature: Vec<u8> = row.get(1)?;

            let packet = BeaconPacket {
                round,
                signature: signature.clone(),
                previous_signature: std::mem::replace(&mut prev_sig, signature),
                metadata: Some(Metadata {
                    node_version: None,
                    beacon_id: id.to_string(),
                    chain_hash: vec![],
                }),
            };

            Ok(packet)
        })?
        .collect()
    }
}

impl Executor for UnChainedBeacon {
    fn get(conn: &Connection, round: u64) -> Result<Self, Error> {
        let mut stmt = conn.prepare_cached("SELECT signature FROM beacons WHERE round = ?1")?;

        stmt.query_row([round], |row| {
            Ok(Self {
                round,
                signature: row.get(0)?,
            })
        })
    }

    fn put(self, conn: &mut Connection) -> Result<(), Error> {
        let tr = conn.transaction()?;
        {
            let mut stmt =
                tr.prepare_cached("INSERT INTO beacons (round, signature) VALUES (?1, ?2)")?;
            stmt.execute(params![self.round, &self.signature])?;
        }
        tr.commit()
    }

    fn last(conn: &Connection) -> Result<Self, Error> {
        let mut stmt = conn.prepare_cached(
            "SELECT round, signature
         FROM beacons 
         WHERE round = (SELECT MAX(round) FROM beacons)",
        )?;

        stmt.query_row([], |row| {
            Ok(Self {
                round: row.get(0)?,
                signature: row.get(1)?,
            })
        })
    }

    fn get_batch_proto(
        conn: &Connection,
        from_round: u64,
        id: &str,
    ) -> Result<Vec<BeaconPacket>, Error> {
        conn.prepare_cached(
            "SELECT round, signature 
         FROM beacons 
         WHERE round >= ?1 
         ORDER BY round ASC 
         LIMIT ?2",
        )?
        .query_map([from_round, BATCH_SIZE], |row| {
            Ok(BeaconPacket {
                round: row.get(0)?,
                signature: row.get(1)?,
                previous_signature: vec![],
                metadata: Some(Metadata {
                    node_version: None,
                    beacon_id: id.to_string(),
                    chain_hash: vec![],
                }),
            })
        })?
        .collect()
    }
}

/// Handle for chain store actor.
#[derive(Clone)]
pub struct ChainStore<B: BeaconRepr> {
    sender: mpsc::Sender<Cmd<B>>,
}

/// Commands for chain store actor.
enum Cmd<B: BeaconRepr> {
    Put {
        beacon: B,
        cb: Callback<(), StoreError>,
    },
    Last {
        cb: Callback<B, StoreError>,
    },
    Get {
        round: u64,
        cb: Callback<B, StoreError>,
    },
    Sync {
        from_round: u64,
        cb: Callback<mpsc::Receiver<StoreStreamResponse>, StoreError>,
    },
}

/// Error details are traced within chain store actor (see: [`ChainStore::start`]).
#[derive(thiserror::Error, Debug)]
pub enum StoreError {
    #[error("internal error")]
    Internal,
    #[error("beacon not found in chain store")]
    NotFound,
    #[error("genesis mismatch")]
    GenesisMismatch,
    #[error("actor receiver has been closed unexpectedly")]
    ActorClosedRx,
    #[error("cb sender has been closed unexpectedly")]
    CbClosedTx(#[from] tokio::sync::oneshot::error::RecvError),
}

impl<B: BeaconRepr> ChainStore<B> {
    /// Starts chain store actor and returns its handle.
    ///
    /// Current implementation is [rusqlite] specific for connection management and execution.
    pub async fn start(path: PathBuf, id: BeaconID, log: Logger) -> Result<Self, StoreError> {
        // Callback for the current request.
        let (cb_tx, cb_rx) = Callback::new();
        // Channel for communicating with storage actor.
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<Cmd<B>>(1);

        task::spawn_blocking(move || {
            // Open a single RW connection to be reused for all actor requests except for [sync].
            let mut rw_conn = match B::open(&path) {
                Ok(conn) => {
                    cb_tx.reply(Ok(()));
                    conn
                }
                Err(err) => {
                    error!(&log, "failed to open RW connection: {err}");
                    cb_tx.reply(Err(StoreError::Internal));
                    return;
                }
            };
            while let Some(cmd) = cmd_rx.blocking_recv() {
                match cmd {
                    Cmd::Put { beacon, cb } => match beacon.put(&mut rw_conn) {
                        Ok(()) => cb.reply(Ok(())),
                        Err(err) => {
                            error!(&log, "failed to put beacon: {err}");
                            cb.reply(Err(StoreError::Internal));
                            return;
                        }
                    },
                    Cmd::Last { cb } => match B::last(&rw_conn) {
                        Ok(beacon) => cb.reply(Ok(beacon)),
                        Err(Error::QueryReturnedNoRows) => cb.reply(Err(StoreError::NotFound)),
                        Err(err) => {
                            error!(&log, "failed to get last beacon: {err}");
                            cb.reply(Err(StoreError::Internal));
                            return;
                        }
                    },
                    Cmd::Get { round, cb } => match B::get(&rw_conn, round) {
                        Ok(beacon) => cb.reply(Ok(beacon)),
                        Err(Error::QueryReturnedNoRows) => cb.reply(Err(StoreError::NotFound)),
                        Err(err) => {
                            error!(&log, "failed to get beacon of round {round}: {err}");
                            cb.reply(Err(StoreError::Internal));
                            return;
                        }
                    },
                    Cmd::Sync { from_round, cb } => {
                        match sync::<B>(&path, from_round, id, log.clone()) {
                            Ok(client_rx) => cb.reply(Ok(client_rx)),
                            Err(err) => {
                                error!(&log, "sync: failed to open RO connection: {err}");
                                cb.reply(Err(StoreError::Internal));
                                return;
                            }
                        }
                    }
                }
            }
        });

        cb_rx.await??;

        Ok(Self { sender: cmd_tx })
    }

    pub async fn put(&self, beacon: B) -> Result<(), StoreError> {
        let (cb_tx, cb_rx) = Callback::new();
        self.sender
            .send(Cmd::Put { beacon, cb: cb_tx })
            .await
            .map_err(|_| StoreError::ActorClosedRx)?;

        cb_rx.await?
    }

    pub async fn get(&self, round: u64) -> Result<B, StoreError> {
        let (cb_tx, cb_rx) = Callback::new();
        self.sender
            .send(Cmd::Get { round, cb: cb_tx })
            .await
            .map_err(|_| StoreError::ActorClosedRx)?;

        cb_rx.await?
    }

    pub async fn last(&self) -> Result<B, StoreError> {
        let (cb_tx, cb_rx) = Callback::new();
        self.sender
            .send(Cmd::Last { cb: cb_tx })
            .await
            .map_err(|_| StoreError::ActorClosedRx)?;

        cb_rx.await?
    }

    pub async fn sync(
        &self,
        from_round: u64,
        cb: Callback<mpsc::Receiver<StoreStreamResponse>, StoreError>,
    ) {
        // Catch callback if actor in failed state.
        if let Err(mpsc::error::SendError(Cmd::Sync { from_round: _, cb })) =
            self.sender.send(Cmd::Sync { from_round, cb }).await
        {
            cb.reply(Err(StoreError::Internal));
        }
    }

    /// Inserts genesis beacon if chain store is empty or asserts that `genesis_seed` is equal to already stored.
    pub async fn check_genesis(&self, genesis_seed: &[u8], log: &Logger) -> Result<(), StoreError> {
        match self.get(0).await {
            Ok(beacon) => {
                if beacon.signature() == genesis_seed {
                    Ok(())
                } else {
                    error!(
                        log,
                        "genesis mismatch: already stored {} != {}",
                        hex::encode(beacon.signature()),
                        hex::encode(genesis_seed)
                    );

                    Err(StoreError::GenesisMismatch)
                }
            }
            Err(StoreError::NotFound) => {
                warn!(
                    log,
                    "chain store is empty, adding genesis {}",
                    hex::encode(genesis_seed)
                );
                self.put(B::from_seed(genesis_seed.to_vec())).await?;
                Ok(())
            }
            Err(err) => Err(err),
        }
    }
}

/// Note: Store abstraction is intentionally leaked (see [`StoreStreamResponse`]) for purpose of single channel usage.
#[allow(unused_assignments)]
fn sync<B: BeaconRepr>(
    path: &Path,
    start_from: u64,
    id: BeaconID,
    log: Logger,
) -> Result<mpsc::Receiver<StoreStreamResponse>, Error> {
    let ro_conn =
        Connection::open_with_flags(path.join(DB_NAME), OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let batch_size = usize::try_from(BATCH_SIZE).unwrap();
    let (tx, rx) = mpsc::channel::<StoreStreamResponse>(batch_size);

    let mut from = start_from;
    let mut sent_total = 0;
    let mut received_len = 0;
    tokio::task::spawn_blocking(move || loop {
        match B::get_batch_proto(&ro_conn, from, id.as_str()) {
            Ok(beacons) => {
                received_len = beacons.len();
                sent_total += received_len;

                for b in beacons {
                    if tx.blocking_send(Ok(b)).is_err() {
                        break;
                    };
                }
                if received_len < batch_size {
                    let _ = tx.blocking_send(Err(tonic::Status::not_found(format!(
                        "no beacons stored above {} round",
                        sent_total as u64 + start_from - 1
                    ))));
                    break;
                }
                from += BATCH_SIZE;
            }
            Err(err) => {
                error!(log, "failed to get_batch_proto: {err}");
                break;
            }
        };
    });

    Ok(rx)
}

#[cfg(test)]
mod test {
    use super::*;

    /// Generates test beacons for 0..=`rounds`.
    trait BeaconGenerator: BeaconRepr + PartialEq {
        fn generate(rounds: u64) -> Vec<Self>;
    }

    impl BeaconGenerator for UnChainedBeacon {
        fn generate(rounds: u64) -> Vec<Self> {
            (0..=rounds)
                .map(|r| UnChainedBeacon {
                    round: r,
                    signature: r.to_be_bytes().into(),
                })
                .collect()
        }
    }

    impl BeaconGenerator for ChainedBeacon {
        fn generate(rounds: u64) -> Vec<Self> {
            (0..=rounds)
                .map(|r| {
                    if r == 0 {
                        ChainedBeacon {
                            round: r,
                            signature: r.to_be_bytes().into(),
                            previous_signature: vec![],
                        }
                    } else {
                        ChainedBeacon {
                            round: r,
                            signature: r.to_be_bytes().into(),
                            previous_signature: (r - 1).to_be_bytes().into(),
                        }
                    }
                })
                .collect()
        }
    }

    async fn test_store<B: BeaconGenerator>() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let id = BeaconID::from_static("some_id");
        let log = crate::log::set_id("", id.as_str());

        // Initialize chain store.
        let store = ChainStore::<B>::start(temp_dir.path().to_path_buf(), id, log.clone())
            .await
            .unwrap();

        // Generate beacons and add into the store.
        let total_beacons = 555;
        let beacons = B::generate(total_beacons);
        for b in &beacons {
            store.put(b.clone()).await.unwrap();
        }

        // Last beacon should match the expected one.
        let last = store.last().await.unwrap();
        let expected = beacons[usize::try_from(total_beacons).unwrap()].clone();
        assert!(last.round() == expected.round());
        assert!(last.signature() == expected.signature());
        assert!(last.prev_signature() == expected.prev_signature());

        // Assert all beacons from the store using [ChainStore::get].
        for i in 0..=total_beacons {
            assert!(store.get(i).await.unwrap() == beacons[usize::try_from(i).unwrap()]);
        }

        // Assert all beacons from the store using [sync].
        let from_round = 0;
        let mut stream_rx = sync::<B>(temp_dir.path(), from_round, id, log).unwrap();
        for i in 0..=total_beacons {
            let packet = stream_rx.recv().await.unwrap().unwrap();
            let i_beacon = &beacons[usize::try_from(i).unwrap()];

            assert!(packet.round == i_beacon.round());
            assert!(packet.signature == i_beacon.signature());
            assert!(&packet.previous_signature == i_beacon.prev_signature().unwrap_or_default());
        }

        // Next value from stream should be this error.
        let expected_err = format!("no beacons stored above {total_beacons} round");
        match stream_rx.recv().await.unwrap() {
            Ok(_) => panic!(),
            Err(err) => {
                assert!(err.message() == expected_err);
            }
        }
    }

    #[tokio::test]
    async fn unchained_store() {
        test_store::<UnChainedBeacon>().await
    }

    #[tokio::test]
    async fn chained_store() {
        test_store::<ChainedBeacon>().await
    }
}
