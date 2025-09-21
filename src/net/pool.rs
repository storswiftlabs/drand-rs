//! Basic implementation of connection pool for sending `PartialBeaconPacket`.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::time::Duration;

use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::{debug, error, trace, warn, Span};

use super::utils::Address;
use crate::core::beacon::BeaconID;
use crate::net::protocol::ProtocolClient;
use crate::protobuf::drand::PartialBeaconPacket;

pub enum PoolCmd {
    Partial(PartialBeaconPacket),
    AddID(BeaconID, Vec<Address>),
    RemoveID(BeaconID),
}

pub struct Connection {
    conn: ProtocolClient,
    beacon_ids: BTreeSet<BeaconID>,
}

pub struct PendingConnection {
    beacon_ids: BTreeSet<BeaconID>,
    cancel: oneshot::Sender<()>,
}

pub struct Pool {
    shutdown: bool,
    active: BTreeMap<Address, Connection>,
    pending: BTreeMap<Address, PendingConnection>,
    enabled_beacons: BTreeMap<BeaconID, broadcast::Sender<PartialBeaconPacket>>,
    l: Span,
}

impl Pool {
    pub fn start(l: Span) -> PoolSender {
        let (tx_cmd, mut rx_cmd) = mpsc::channel::<PoolCmd>(1);
        let (tx_new_conn, mut rx_new_conn) = mpsc::channel::<(Address, ProtocolClient)>(1);

        debug!(parent: &l, "pool initialized");
        tokio::spawn(async move {
            let mut pool = Self {
                shutdown: false,
                active: BTreeMap::new(),
                pending: BTreeMap::new(),
                enabled_beacons: BTreeMap::new(),
                l,
            };

            loop {
                tokio::select! {
                    new_conn = rx_new_conn.recv()=> {
                        if let Some((uri, client))=new_conn{
                            pool.add_connection(uri, client);
                        }
                    }

                    cmd = rx_cmd.recv()=> {
                        if pool.shutdown {
                            warn!(parent: &pool.l, "PoolCmd::AddID: pool is shutting down, ignoring message");
                            continue;
                        }

                        if let Some(cmd)=cmd{
                            match cmd{
                                PoolCmd::Partial(msg) =>{
                                    pool.broadcast_msg(msg);

                                }
                                PoolCmd::AddID(id, peers) => {
                                    let (tx_broadcast, _) = tokio::sync::broadcast::channel::<PartialBeaconPacket>(1);
                                    if pool.enabled_beacons.insert(id, tx_broadcast).is_some() {
                                        error!(parent: &pool.l, "beacon ID [{id}] is already active");
                                        continue;
                                    }
                                    for peer in peers {
                                        // check if pool already has been connected to endpoint
                                        if let Some(active)=pool.active.get(&peer){
                                            pool.subscribe_client(id, &peer, active.conn.clone());
                                        }else{
                                            pool.add_pending(id, peer, tx_new_conn.clone());
                                        }
                                    }
                                }
                                PoolCmd::RemoveID(id) => {
                                    pool.remove_beacon_id(&id);
                                    debug!(parent: &pool.l,"beacon ID [{id}] is removed from pool");
                                }
                            }
                        }
                    }
                }
            }
        });

        PoolSender { sender: tx_cmd }
    }

    fn add_connection(&mut self, uri: Address, conn: ProtocolClient) {
        // remove conn from pending list
        if let Some(pending_conn) = self.pending.remove(&uri) {
            // subscribe conn to registered beacons
            pending_conn
                .beacon_ids
                .iter()
                .for_each(|beacon_id| self.subscribe_client(*beacon_id, &uri, conn.clone()));

            debug!(parent: &self.l, "established connection: {uri}");
            self.active.insert(
                uri,
                Connection {
                    conn,
                    beacon_ids: pending_conn.beacon_ids,
                },
            );
        } else {
            error!(parent: &self.l, "add_connection:: this should not be possible");
        }
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "this might be done more elegantly"
    )]
    fn broadcast_msg(&self, msg: PartialBeaconPacket) {
        let beacon_id = match msg.metadata.as_ref() {
            Some(metadata) => metadata.beacon_id.clone(),
            None => return,
        };

        if self.enabled_beacons.is_empty() {
            error!(parent: &self.l, "failed to broadcast: no any ID enabled");
        }
        for (id, sender) in &self.enabled_beacons {
            if beacon_id == id.as_str() {
                if let Err(e) = sender.send(msg.clone()) {
                    error!(parent: &self.l,
                        "broadcast: {e}, id: {}",
                        msg.metadata.as_ref().unwrap().beacon_id
                    );
                }
            }
        }
    }

    fn subscribe_client(&mut self, id: BeaconID, uri: &Address, mut conn: ProtocolClient) {
        if let Some(active) = self.active.get_mut(uri) {
            if !active.beacon_ids.contains(&id) {
                active.beacon_ids.insert(id);
            }
        }
        if let Some(sender) = self.enabled_beacons.get(&id) {
            let mut receiver = sender.subscribe();
            debug!(parent: &self.l, "connection {uri:?} is subscribed for [{id}]");
            tokio::spawn({
                let peer = uri.as_str().to_owned();
                let ll = self.l.clone();
                async move {
                    let l = &ll;
                    while let Ok(msg) = receiver.recv().await {
                        let round = msg.round;
                        if let Err(err) = conn.partial_beacon(msg).await {
                            error!(parent: l, "sending partial: round {round} to: {peer}, error: {}", err.root_cause());
                        } else {
                            debug!(parent: l, "sending partial {{\"round\": {round}, \"to\": \"{peer}\"}}");
                        }
                    }
                    debug!(parent: l, "disabled subscription: {peer}");
                }
            });
        } else {
            error!(parent: &self.l, "unable to subscribe connection {uri:?}, beacon_id {id} is disabled");
        }
    }

    fn add_pending(
        &mut self,
        id: BeaconID,
        peer: Address,
        sender: mpsc::Sender<(Address, ProtocolClient)>,
    ) {
        trace!(parent: &self.l,"pending: add_connection {peer}");
        // update pending list
        // todo: add check that map not contains this kv
        let mut beacons = BTreeSet::new();
        beacons.insert(id);
        let (tx, mut rx) = oneshot::channel();
        self.pending.insert(
            peer.clone(),
            PendingConnection {
                beacon_ids: beacons,
                cancel: tx,
            },
        );

        let l = self.l.clone();
        tokio::spawn(async move {
            let client = loop {
                match ProtocolClient::new(&peer).await {
                    Ok(client) => {
                        debug!(parent: &l, "connected to {peer}");
                        if let Ok(()) = rx.try_recv() {
                            debug!(parent: &l,"pending connection {peer} canceled");
                            break None;
                        }
                        break Some(client);
                    }
                    Err(err) => {
                        error!(parent: &l,"connecting to {peer}: {err}");
                    }
                };

                if let Ok(()) = rx.try_recv() {
                    debug!(parent: &l,"pending connection {peer} canceled");
                    break None;
                }

                tokio::time::sleep(Duration::from_secs(5)).await;
            };

            if client.is_some() && sender.send((peer.clone(), client.unwrap())).await.is_err() {
                error!(parent: &l, "pending: pool receiver is dropped");
            }
        });
    }

    fn remove_beacon_id(&mut self, beacon_id: &BeaconID) {
        self.enabled_beacons.remove(beacon_id);

        // cancel pending beacon
        let mut cancel = vec![];
        for (k, v) in &mut self.pending {
            if v.beacon_ids.remove(beacon_id) && v.beacon_ids.is_empty() {
                cancel.push(k.clone());
            }
        }

        for k in cancel {
            if let Some(pending) = self.pending.remove(&k) {
                let _ = pending.cancel.send(());
            };
        }

        let mut disconnect = vec![];
        for (k, v) in &mut self.active {
            if v.beacon_ids.remove(beacon_id) && v.beacon_ids.is_empty() {
                disconnect.push(k.clone());
            }
        }

        for k in disconnect {
            self.active.remove(&k);
        }
    }
}

#[derive(Clone)]
pub struct PoolSender {
    sender: mpsc::Sender<PoolCmd>,
}

#[derive(thiserror::Error, Debug)]
#[error("connection pool is closed")]
pub struct PoolError;
use tokio::sync::mpsc::error::SendError;

impl From<SendError<PoolCmd>> for PoolError {
    fn from(_: SendError<PoolCmd>) -> Self {
        PoolError
    }
}

impl PoolSender {
    pub async fn add_id(&self, id: BeaconID, uri: Vec<Address>) -> Result<(), PoolError> {
        self.sender.send(PoolCmd::AddID(id, uri)).await?;

        Ok(())
    }

    pub async fn remove_id(&self, id: BeaconID) -> Result<(), PoolError> {
        self.sender.send(PoolCmd::RemoveID(id)).await?;

        Ok(())
    }

    pub async fn broadcast_partial(&self, packet: PartialBeaconPacket) -> Result<(), PoolError> {
        self.sender.send(PoolCmd::Partial(packet)).await?;

        Ok(())
    }
}
