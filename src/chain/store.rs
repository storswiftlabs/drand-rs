use crate::net::utils::Callback;
use crate::protobuf::drand::BeaconPacket;
use crate::protobuf::drand::Metadata;

use rusqlite::params;
use rusqlite::Connection;
use rusqlite::Error;
use rusqlite::OpenFlags;

use std::path::Path;
use std::path::PathBuf;
use tokio::sync::mpsc;
use tokio::task;
use tracing::*;

/// Number of beacons retrieved in a single query from chain DB.
const BATCH_SIZE: u64 = 300;
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

#[allow(private_bounds/*:Executor*/)]
pub trait BeaconRepr: Executor + 'static + Sized + Send + Sync + Clone {
    fn new(prev: Self, recovered_sig: Vec<u8>) -> Self;
    fn round(&self) -> u64;
    fn signature(&self) -> &[u8];
    fn prev_signature(&self) -> Option<&[u8]>;
    fn from_packet(p: BeaconPacket) -> Self;
    fn from_seed(genesis_seed: Vec<u8>) -> Self;
}

impl BeaconRepr for ChainedBeacon {
    /// WARNING: monotonic round check shifted to caller side.
    fn new(prev: Self, new_sig: Vec<u8>) -> Self {
        Self {
            round: prev.round + 1,
            signature: new_sig,
            previous_signature: prev.signature,
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
    /// WARNING: monotonic round check shifted to caller side.
    fn new(prev: Self, new_sig: Vec<u8>) -> Self {
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

/// SQL statement executor for [BeaconRepr].
trait Executor: Sized {
    fn open(path: &Path) -> Result<Connection, Error>;
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
    fn open(path: &Path) -> Result<Connection, Error> {
        let conn = Connection::open(path.join(DB_NAME))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS beacons (
            round INTEGER PRIMARY KEY,
            signature BLOB NOT NULL,
            previous_sig BLOB NOT NULL
        ) WITHOUT ROWID",
            [],
        )?;

        Ok(conn)
    }

    fn get(conn: &Connection, round: u64) -> Result<Self, Error> {
        let mut stmt = conn.prepare_cached(
            "SELECT round, signature, previous_sig FROM beacons WHERE round = ?1",
        )?;
        stmt.query_row([round], |row| {
            Ok(Self {
                round: row.get(0)?,
                signature: row.get(1)?,
                previous_signature: row.get(2)?,
            })
        })
    }

    fn put(self, conn: &mut Connection) -> Result<(), Error> {
        let tr = conn.transaction()?;

        {
            let mut stmt = tr.prepare_cached(
                "INSERT INTO beacons (round, signature, previous_sig) VALUES (?1, ?2, ?3)",
            )?;
            stmt.execute(params![
                self.round,
                &self.signature,
                &self.previous_signature,
            ])?;
        }

        tr.commit()
    }

    fn last(conn: &Connection) -> Result<Self, Error> {
        let mut stmt = conn.prepare_cached(
            "SELECT round, signature, previous_sig
         FROM beacons 
         WHERE round = (SELECT MAX(round) FROM beacons)",
        )?;

        stmt.query_row([], |row| {
            Ok(Self {
                round: row.get(0)?,
                signature: row.get(1)?,
                previous_signature: row.get(2)?,
            })
        })
    }

    fn get_batch_proto(
        conn: &Connection,
        from_round: u64,
        id: &str,
    ) -> Result<Vec<BeaconPacket>, Error> {
        conn.prepare_cached(
            "SELECT round, signature, previous_sig  
         FROM beacons 
         WHERE round >= ?1 
         ORDER BY round ASC 
         LIMIT ?2",
        )?
        .query_map([from_round, BATCH_SIZE], |row| {
            Ok(BeaconPacket {
                round: row.get(0)?,
                signature: row.get(1)?,
                previous_signature: row.get(2)?,
                metadata: Some(Metadata {
                    node_version: None,
                    beacon_id: id.to_string(),
                    chain_hash: vec![],
                }),
            })
        })?
        .collect::<Result<Vec<BeaconPacket>, _>>()
    }
}

impl Executor for UnChainedBeacon {
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

    fn get(conn: &Connection, round: u64) -> Result<Self, Error> {
        let mut stmt =
            conn.prepare_cached("SELECT round, signature FROM beacons WHERE round = ?1")?;

        stmt.query_row([round], |row| {
            Ok(Self {
                round: row.get(0)?,
                signature: row.get(1)?,
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
        .collect::<Result<Vec<BeaconPacket>, _>>()
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

/// Error details are traced within chain store actor (see: [ChainStore::start]).
#[derive(thiserror::Error, Debug)]
pub enum StoreError {
    #[error("DB request is failed")]
    Internal,
    #[error("beacon not found")]
    NotFound,
    #[error("genesis mismatch")]
    GenesisMismatch,
    #[error("actor receiver has been closed unexpectedly")]
    ActorClosedRx,
    #[error("callback sender has been closed unexpectedly")]
    CbClosedTx(#[from] tokio::sync::oneshot::error::RecvError),
}

impl<B: BeaconRepr> ChainStore<B> {
    /// Starts chain store actor and returns its handle.
    ///
    /// Current implementation is [rusqlite] specific for connection management and execution.
    pub async fn start(path: PathBuf, beacon_id: String) -> Result<Self, StoreError> {
        // Callback for the current request.
        let (cb_tx, cb_rx) = Callback::new();
        // Channel for communicating with storage actor.
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<Cmd<B>>(1);
        let l = tracing::info_span!("", chain_store = beacon_id);

        task::spawn_blocking(move || {
            // Open a single RW connection to be reused for all actor requests except for [sync].
            let mut rw_conn = match B::open(&path) {
                Ok(conn) => {
                    cb_tx.reply(Ok(()));
                    conn
                }
                Err(err) => {
                    error!(parent: &l, "failed to open RW connection: {err}");
                    cb_tx.reply(Err(StoreError::Internal));
                    return;
                }
            };
            while let Some(cmd) = cmd_rx.blocking_recv() {
                match cmd {
                    Cmd::Put { beacon, cb } => match B::put(beacon, &mut rw_conn) {
                        Ok(_) => cb.reply(Ok(())),
                        Err(err) => {
                            error!(parent: &l, "failed to put beacon: {err}");
                            cb.reply(Err(StoreError::Internal));
                            return;
                        }
                    },
                    Cmd::Last { cb } => match B::last(&rw_conn) {
                        Ok(beacon) => cb.reply(Ok(beacon)),
                        Err(Error::QueryReturnedNoRows) => cb.reply(Err(StoreError::NotFound)),
                        Err(err) => {
                            error!(parent: &l, "failed to get last beacon: {err}");
                            cb.reply(Err(StoreError::Internal));
                            return;
                        }
                    },
                    Cmd::Get { round, cb } => match B::get(&rw_conn, round) {
                        Ok(beacon) => cb.reply(Ok(beacon)),
                        Err(Error::QueryReturnedNoRows) => cb.reply(Err(StoreError::NotFound)),
                        Err(err) => {
                            error!(parent: &l, "failed to get beacon for round {round}: {err}");
                            cb.reply(Err(StoreError::Internal));
                            return;
                        }
                    },
                    Cmd::Sync { from_round, cb } => {
                        match sync::<B>(&path, from_round, &beacon_id) {
                            Ok(client_rx) => cb.reply(Ok(client_rx)),
                            Err(err) => {
                                error!(parent: &l, "sync: failed to open RO connection: {err}");
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

    /// Inserts genesis beacon if chain store is empty or asserts that genesis_seed is equal to already stored.
    pub async fn check_genesis(&self, genesis_seed: &[u8], l: &Span) -> Result<(), StoreError> {
        match self.get(0).await {
            Ok(beacon) => {
                if beacon.signature() != genesis_seed {
                    error!(parent: l, "genesis mismatch: already stored: {} != received {}",
                        hex::encode(beacon.signature()),
                        hex::encode(genesis_seed));

                    Err(StoreError::GenesisMismatch)
                } else {
                    Ok(())
                }
            }
            Err(StoreError::NotFound) => {
                warn!(parent: l, "chain store is empty, adding genesis {}", hex::encode(genesis_seed));
                self.put(B::from_seed(genesis_seed.to_vec())).await?;
                Ok(())
            }
            Err(err) => Err(err),
        }
    }
}

/// Note: Store abstraction is intentionally leaked (see [StoreStreamResponse]) for purpose of single channel usage.
#[allow(unused_assignments)]
fn sync<B: BeaconRepr>(
    path: &Path,
    start_from: u64,
    id: &str,
) -> Result<mpsc::Receiver<StoreStreamResponse>, Error> {
    let ro_conn =
        Connection::open_with_flags(path.join(DB_NAME), OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let (tx, rx) = mpsc::channel::<StoreStreamResponse>(BATCH_SIZE as usize);
    let id = id.to_string();

    let mut from = start_from;
    let mut sent_total = 0;
    let mut received_len = 0;
    tokio::task::spawn_blocking(move || loop {
        match B::get_batch_proto(&ro_conn, from, &id) {
            Ok(beacons) => {
                received_len = beacons.len();
                sent_total += received_len;

                for b in beacons {
                    if tx.blocking_send(Ok(b)).is_err() {
                        break;
                    };
                }
                // No more beacons stored.
                if received_len < BATCH_SIZE as usize {
                    let _ = tx.blocking_send(Err(tonic::Status::not_found(format!(
                        "no beacons stored above {} round",
                        sent_total as u64 + start_from - 1
                    ))));
                    break;
                }
                from += BATCH_SIZE;
            }
            Err(err) => {
                error!("failed to get batch proto for [{id}]: {err}");
                break;
            }
        };
    });

    Ok(rx)
}

#[cfg(test)]
mod test {
    use super::*;

    fn generate_unchained(rounds: u64) -> Vec<UnChainedBeacon> {
        (0..=rounds)
            .map(|r| UnChainedBeacon {
                round: r,
                signature: r.to_be_bytes().into(),
            })
            .collect()
    }

    fn generate_chained(rounds: u64) -> Vec<ChainedBeacon> {
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

    #[tokio::test]
    async fn unchained_store() {
        crate::log::init_log(true).unwrap();
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path();
        let id = "some_id";

        let total_beacons = 555;
        let beacons = generate_unchained(total_beacons);
        let store = ChainStore::<UnChainedBeacon>::start(db_path.to_path_buf(), id.to_string())
            .await
            .unwrap();

        // Add all beacons to the store.
        for b in &beacons {
            store.put(b.clone()).await.unwrap();
        }
        assert!(store.last().await.unwrap().round == total_beacons);

        // Get all beacons as internal type.
        for i in 0..=total_beacons {
            assert!(store.get(i).await.unwrap() == beacons[i as usize])
        }

        // Sync from this store; get all beacons as protobuf packets.
        let from_round = 1;
        let mut stream_rx = sync::<UnChainedBeacon>(db_path, from_round, id).unwrap();

        // Streamed data should match internal repr.
        let expected_prev_sig: Vec<u8> = vec![];
        for i in 1..=total_beacons {
            let packet = stream_rx.recv().await.unwrap().unwrap();
            assert!(packet.round == i);
            let i_beacon = &beacons[i as usize];

            assert!(packet.round == i_beacon.round);
            assert!(packet.signature == i_beacon.signature);
            assert!(packet.previous_signature == expected_prev_sig)
        }

        // Next value from the stream should be expected error.
        let expected = format!("no beacons stored above {total_beacons} round");
        match stream_rx.recv().await.unwrap() {
            Ok(_) => panic!(),
            Err(err) => {
                assert!(err.message() == expected)
            }
        }
    }

    #[tokio::test]
    async fn chained_store() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let db_path = temp_dir.path();
        let id = "some_id";

        let total_beacons = 555;
        let beacons = generate_chained(total_beacons);
        let store = ChainStore::<ChainedBeacon>::start(db_path.to_path_buf(), id.to_string())
            .await
            .unwrap();

        // Add all beacons to the store.
        for b in &beacons {
            store.put(b.clone()).await.unwrap();
        }
        assert!(store.last().await.unwrap().round == total_beacons);

        // Get all beacons as internal type.
        for i in 0..=total_beacons {
            assert!(store.get(i).await.unwrap() == beacons[i as usize])
        }

        // Sync from this store; get all beacons as protobuf packets.
        let from_round = 1;
        let mut stream_rx = sync::<ChainedBeacon>(db_path, from_round, id).unwrap();

        // Streamed data should match internal repr.
        for i in 1..=total_beacons {
            let packet = stream_rx.recv().await.unwrap().unwrap();
            assert!(packet.round == i);
            let i_beacon = &beacons[i as usize];

            assert!(packet.round == i_beacon.round);
            assert!(packet.signature == i_beacon.signature);
            assert!(packet.previous_signature == i_beacon.previous_signature)
        }

        // Next value from the stream should be expected error.
        let expected = format!("no beacons stored above {total_beacons} round");
        match stream_rx.recv().await.unwrap() {
            Ok(_) => panic!(),
            Err(err) => {
                assert!(err.message() == expected)
            }
        }
    }
}
