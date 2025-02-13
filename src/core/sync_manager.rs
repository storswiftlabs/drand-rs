use std::{sync::Arc, time::Duration};

use tokio::sync::oneshot;
use tracing::{debug, error, warn};

use crate::{
    net::utils::Address,
    protobuf::drand::{protocol_client::ProtocolClient, ChainInfoPacket, Metadata},
    store::{AppendStore, Beacon, ChainStore, StorageError, Store},
    transport::drand::{StartSyncRequest, SyncRequest},
};

pub struct SyncManager {
    insecure_store: Arc<ChainStore>,
    store: AppendStore,
    // client: ProtocolClient<Channel>,
    node_addr: Address,

    info: ChainInfoPacket,

    new_synced_beacon: tokio::sync::broadcast::Sender<Beacon>,
}

pub struct RequestInfo {
    pub from: u64,
    pub to: u64,
    pub address: Vec<Address>,
}

impl SyncManager {
    pub async fn sync(
        &self,
        req: RequestInfo,
        mut recv: oneshot::Receiver<()>,
    ) -> Result<(), SyncError> {
        // TODO: ramdomize the order of the nodes
        for addr in req.address {
            if self.node_addr == addr {
                continue;
            }

            if let Ok(()) = recv.try_recv() {
                return Ok(());
            }

            if self.try_node(addr, req.from, req.to).await {
                return Ok(());
            }
        }

        Ok(())
    }

    pub async fn try_node(&self, addr: Address, mut from: u64, to: u64) -> bool {
        let is_resync = from > 0;

        let last = match self.store.last().await {
            Ok(last) => last,
            Err(e) => {
                error!("unable to fetch from store: {}", e);
                return false;
            }
        };

        if from == 0 {
            from = last.round + 1;
        } else if from > to {
            error!("Invalid request: from > upTo, from = {from}, upTo = {to}");
            return false;
        }

        let req = SyncRequest {
            from_round: from,
            metadata: Metadata {
                beacon_id: self.info.metadata.as_ref().unwrap().beacon_id.clone(),
                ..Default::default()
            },
        };

        let mut c = match ProtocolClient::connect(addr.to_uri()).await {
            Ok(c) => c,
            Err(e) => {
                error!("unable to connect to node: {}", e);
                return false;
            }
        };

        debug!("sync_manager syncing with_peer {}", addr);

        let stream = match c
            .sync_chain(Into::<crate::protobuf::drand::SyncRequest>::into(req))
            .await
        {
            Ok(stream) => stream,
            Err(e) => {
                error!("unable to sync chain: {}", e);
                return false;
            }
        };

        // target := commonutils.CurrentRound(s.clock.Now().Unix(), s.info.Period, s.info.GenesisTime)
        // if upTo > 0 {
        //     target = upTo
        // }

        let target = current_round(
            current_timestamp_as_i64(),
            Duration::from_secs(self.info.period.into()),
            self.info.genesis_time,
        );

        let mut chan = stream.into_inner();

        loop {
            match chan.message().await {
                Ok(Some(resp)) => {
                    if let Some(metadata) = resp.clone().metadata {
                        if metadata.beacon_id != self.info.metadata.as_ref().unwrap().beacon_id {
                            error!(
                                "invalid beacon id: {} expect {}",
                                metadata.beacon_id,
                                self.info.metadata.as_ref().unwrap().beacon_id,
                            );
                            return false;
                        }
                    }

                    let beacon = Beacon::from(resp.clone());

                    //TODO: // verify the signature validity
                    let round = beacon.round;
                    if is_resync {
                        if let Err(e) = self.insecure_store.put(beacon.clone()).await {
                            error!("resync: unable to write to store: {}", e);
                        };
                    } else {
                        match self.store.put(beacon.clone()).await {
                            Ok(_) => {}
                            Err(StorageError::AlreadyExists) => {
                                debug!("resync: race with aggregation: with_peer {}", addr);
                                return round == to;
                            }
                            Err(e) => {
                                error!("sync: unable to write to store: {}", e);
                            }
                        };
                    }

                    if let Err(e) = self.new_synced_beacon.send(beacon) {
                        error!("sync: unable to send beacon to channel: {}", e);
                    };

                    if round == to {
                        debug!("sync_manager finished syncing up to round {}", to);
                        return true;
                    }
                }
                Ok(None) => {
                    warn!("SyncChain channel closed: peer {}", addr);
                    return false;
                }
                Err(e) => {
                    error!("unable to sync chain: {}", e);
                    return false;
                }
            }
        }

        true
    }
}

#[derive(thiserror::Error, Debug)]
pub enum SyncError {
    #[error("data store disconnected")]
    Disconnect(#[from] std::io::Error),
    #[error("unknown error")]
    Unknown(String),
}

fn current_round(now: i64, period: Duration, genesis: i64) -> u64 {
    let (next_round, _) = next_round(now, period, genesis);
    if next_round <= 1 {
        return next_round;
    }
    next_round - 1
}

fn next_round(now: i64, period: Duration, genesis: i64) -> (u64, i64) {
    if now < genesis {
        return (1, genesis);
    }

    let from_genesis = now - genesis;
    // we take the time from genesis divided by the periods in seconds, that
    // gives us the number of periods since genesis. We add +1 since we want the
    // next round. We also add +1 because round 1 starts at genesis time.
    let next_round = (from_genesis as f64 / period.as_secs_f64()) as u64 + 1;
    let next_time = genesis + (period.as_secs() * next_round) as i64;
    (next_round + 1, next_time)
}

fn current_timestamp_as_i64() -> i64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(duration) => duration.as_secs() as i64,
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}

#[cfg(feature = "memstore")]
//TODO: this is only for testing purposes
pub async fn start_follow_chain(
    store: Arc<ChainStore>,
    req: StartSyncRequest,
) -> anyhow::Result<tokio::sync::broadcast::Receiver<Beacon>> {
    let info = info_from_peers(req.metadata.beacon_id, req.nodes.clone()).await?;

    let genesis_seed = info.group_hash.clone();

    let genesis_beacon = Beacon {
        round: 0,
        signature: genesis_seed,
        previous_sig: vec![],
    };

    if store.len().await? == 0 {
        store.put(genesis_beacon).await?;
    }

    let insecure_store = Arc::new(crate::store::memstore::MemStore::new(true));

    let store = AppendStore::new(insecure_store.clone()).await.unwrap();

    let (beacon_tx, mut beacon_rx) = tokio::sync::broadcast::channel(1024);

    let sync_manager = SyncManager {
        insecure_store: insecure_store.clone(),
        store,
        node_addr: Address::precheck("127.0.0.1:1111").unwrap(),
        info,
        new_synced_beacon: beacon_tx,
    };

    let (tx, rx) = tokio::sync::oneshot::channel();

    sync_manager
        .sync(
            RequestInfo {
                from: 0,
                to: req.up_to,
                address: req
                    .nodes
                    .iter()
                    .map(|v| Address::precheck(v).unwrap())
                    .collect(),
            },
            rx,
        )
        .await?;

    Ok(beacon_rx)
}

pub async fn info_from_peers(
    beacon_id: String,
    peers: Vec<String>,
) -> anyhow::Result<ChainInfoPacket> {
    for peer in peers {
        if let Ok(mut public) =
            crate::protobuf::drand::public_client::PublicClient::connect(peer).await
        {
            if let Ok(info) = public
                .chain_info(crate::protobuf::drand::ChainInfoRequest {
                    metadata: Some(Metadata {
                        beacon_id: beacon_id.clone(),
                        ..Default::default()
                    }),
                })
                .await
            {
                return Ok(info.into_inner());
            }
        }
    }

    Err(anyhow::anyhow!("unable to get info from peers"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        log::init_log,
        protobuf::drand::ChainInfoRequest,
        store::{append_store::AppendStore, memstore::MemStore},
    };

    use super::*;

    #[test]
    fn test_sync() {
        let rt = tokio::runtime::Runtime::new().unwrap();

        const BEACON_ID: &str = "evmnet-t";
        const ENDPOINT: &str = "http://127.0.0.1:8269";
        const ENDPOIT_ADDRESS: &str = "127.0.0.1:8269";
        const UP_TO: u64 = 1024;

        init_log(true).unwrap();

        let (beacon_tx, mut beacon_rx) = tokio::sync::broadcast::channel(1024);

        rt.spawn(async {
            let mut public = crate::protobuf::drand::public_client::PublicClient::connect(ENDPOINT)
                .await
                .unwrap();

            let info = public
                .chain_info(ChainInfoRequest {
                    metadata: Some(Metadata {
                        beacon_id: BEACON_ID.to_string(),
                        ..Default::default()
                    }),
                })
                .await
                .unwrap()
                .into_inner();

            // tracing::info!("info: {:#?}", info);

            let genesis_seed = info.group_hash.clone();

            let genesis_beacon = Beacon {
                round: 0,
                signature: genesis_seed,
                previous_sig: vec![],
            };

            let insecure_store = Arc::new(MemStore::new(true));

            insecure_store.put(genesis_beacon).await.unwrap();

            let store = AppendStore::new(insecure_store.clone()).await.unwrap();

            let sync_manager = SyncManager {
                insecure_store: insecure_store.clone(),
                store,
                node_addr: Address::precheck("127.0.0.1:1111").unwrap(),
                info,
                new_synced_beacon: beacon_tx,
            };

            let (tx, rx) = tokio::sync::oneshot::channel();

            sync_manager
                .sync(
                    RequestInfo {
                        from: 0,
                        to: UP_TO,
                        address: vec![Address::precheck(ENDPOIT_ADDRESS).unwrap()],
                    },
                    rx,
                )
                .await
                .unwrap();

            let last = insecure_store.last().await.unwrap();

            assert_eq!(last.round, UP_TO);
            tracing::info!("last: {:#?}", last);
        });

        rt.block_on(async {
            tracing::info!("starting sync");
            while let Ok(beacon) = beacon_rx.recv().await {
                tracing::info!("beacon: {}", beacon.round);
                if beacon.round == UP_TO {
                    tracing::info!("sync finished: {:#?}", beacon);
                    break;
                }
            }
        })
    }
}
