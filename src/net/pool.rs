use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use tokio::sync::{broadcast, mpsc, oneshot};
use tonic::transport::Channel;
use tracing::{error, trace, warn};

use crate::protobuf::drand::{protocol_client::ProtocolClient, PartialBeaconPacket};

use super::utils::Address;

#[derive(Debug)]
pub struct PoolStatus {
    pub tls: bool,
    pub pending: Vec<(String, Vec<String>)>,
    pub connected: Vec<(String, Vec<String>)>,
    pub active_beacons: Vec<String>,
}

impl std::fmt::Display for PoolStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "PoolStatus {{ tls: {}", self.tls)?;
        writeln!(f, "\tpending: ")?;
        for (id, uris) in &self.pending {
            writeln!(f, "\t\t{}: {:?}", id, uris)?;
        }

        writeln!(f, "\tconnected: ")?;
        for (id, beacons) in &self.connected {
            writeln!(f, "\t\t{}: {:?}", id, beacons)?;
        }
        writeln!(f, "\tactive_beacons: {:?}", self.active_beacons)?;
        write!(f, "}}")
    }
}

pub enum PoolCmd {
    Status(tokio::sync::oneshot::Sender<PoolStatus>),
    Partial(PartialBeaconPacket),
    AddID(BeaconID, Vec<String>), // beacon_id, uris
    RemoveID(BeaconID),
}

type BeaconID = String;

pub trait PoolPartial {
    fn start(tls: bool) -> PoolHandler;
}

pub struct Connection {
    conn: ProtocolClient<Channel>,
    beacon_ids: BTreeSet<String>,
}

#[derive(Debug)]
pub struct PendingConnection {
    beacon_ids: BTreeSet<BeaconID>,
    cancel: oneshot::Sender<()>,
}

pub struct Pool {
    tls: bool,
    active: BTreeMap<Address, Connection>,
    pending: BTreeMap<Address, PendingConnection>,
    enabled_beacons: BTreeMap<BeaconID, broadcast::Sender<PartialBeaconPacket>>,
}

impl PoolPartial for Pool {
    fn start(tls: bool) -> PoolHandler {
        let (tx_cmd, mut rx_cmd) = mpsc::channel::<PoolCmd>(3);
        let (tx_new_conn, mut rx_new_conn) = mpsc::channel::<(Address, ProtocolClient<Channel>)>(1);

        tokio::spawn(async move {
            let mut pool = Self {
                tls,
                active: BTreeMap::new(),
                pending: BTreeMap::new(),
                enabled_beacons: BTreeMap::new(),
            };

            loop {
                tokio::select! {

                    new_conn = rx_new_conn.recv()=> {
                        if let Some((uri, client))=new_conn{
                            pool.add_connection(uri, client);
                        }
                    }

                    cmd = rx_cmd.recv()=> {
                        if let Some(cmd)=cmd{
                            match cmd{
                                PoolCmd::Partial(msg) =>{
                                    pool.broadcast_msg(msg);

                                }
                                PoolCmd::AddID(id, uris_unchecked) => {
                                    trace!("PoolCmd::AddID: reseived id {id}, nodes: {uris_unchecked:#?}");
                                    // Add id to enabled_beacons
                                    let (tx_broadcast, _) = tokio::sync::broadcast::channel::<PartialBeaconPacket>(1);
                                            if pool.enabled_beacons.insert(id.clone(), tx_broadcast).is_some() {
                                                panic!("reshape not implemeted yet")
                                            }

                                    let mut valid: Vec<Address>=Vec::with_capacity(uris_unchecked.len());

                                    for uri in uris_unchecked.into_iter(){
                                        if let Ok(address)=Address::precheck(&uri){
                                            valid.push(address);
                                        }
                                    };

                                    for uri in valid.into_iter() {
                                        // check if pool already has been connected to endpoint
                                        if let Some(active)=pool.active.get(&uri){
                                            pool.subscribe_client(&id,&uri, active.conn.clone());
                                        }else{
                                            pool.add_pending(id.clone(), uri, tx_new_conn.clone())
                                        }
                                    }
                                }
                                PoolCmd::RemoveID(id) => {
                                    pool.remove_beacon_id(&id);
                                    trace!("PoolCmd::RemoveID:: {id}");
                                }
                                PoolCmd::Status(callback)=>{
                                    let _=callback.send(pool.get_status());
                                }
                            }
                        }
                    }
                }
            }
        });

        PoolHandler { sender: tx_cmd }
    }
}

impl Pool {
    fn add_connection(&mut self, uri: Address, conn: ProtocolClient<Channel>) {
        trace!(
            "Established connection: {uri}, current pending: {:#?}",
            self.pending
        );
        // remove conn from pending list
        if let Some(pending_conn) = self.pending.remove(&uri) {
            // subscribe conn to registered beacons
            pending_conn
                .beacon_ids
                .iter()
                .for_each(|beacon_id| self.subscribe_client(beacon_id, &uri, conn.clone()));

            self.active.insert(
                uri,
                Connection {
                    conn,
                    beacon_ids: pending_conn.beacon_ids,
                },
            );
        } else {
            error!("add_connection:: this should not be possible")
        }
    }

    fn broadcast_msg(&self, msg: PartialBeaconPacket) {
        let beacon_id = match msg.metadata.as_ref() {
            Some(metadata) => metadata.beacon_id.clone(),
            None => return,
        };

        if self.enabled_beacons.is_empty() {
            error!("broadcast: EnabledID: no any id enabled");
        }
        for (id, sender) in self.enabled_beacons.iter() {
            if beacon_id == id.as_str() {
                if let Err(e) = sender.send(msg.clone()) {
                    error!(
                        "broadcast: {e}, id: {}",
                        msg.metadata.as_ref().unwrap().beacon_id
                    )
                }
            }
        }
    }

    fn subscribe_client(
        &mut self,
        id: &BeaconID,
        uri: &Address,
        mut conn: ProtocolClient<Channel>,
    ) {
        if let Some(active) = self.active.get_mut(uri) {
            if !active.beacon_ids.contains(id) {
                active.beacon_ids.insert(id.clone());
            }
        }
        if let Some(sender) = self.enabled_beacons.get(id) {
            let mut receiver = sender.subscribe();
            trace!("connection {uri:?} is subscribed for {id}");
            tokio::spawn({
                let pin_address = format!("{uri:?}");

                async move {
                    while let Ok(msg) = receiver.recv().await {
                        let round = msg.round;

                        if let Err(e) = conn.partial_beacon(msg).await {
                            error!(
                                "sending partial: round {round} to: {pin_address}, status: {e:?}"
                            )
                        } else {
                            trace!("sending partial: round {round} to: {pin_address}, status: OK")
                        }
                    }
                    warn!("disabled subscription: {pin_address}")
                }
            });
        } else {
            // should not happens
            warn!("unable to subscribe connection {uri:?}, beacon_id {id} is disabled")
        }
    }

    fn add_pending(
        &mut self,
        id: BeaconID,
        uri: Address,
        sender: mpsc::Sender<(Address, ProtocolClient<Channel>)>,
    ) {
        trace!("pool: pending: add_connection {uri}");
        // update pending list
        // todo: add check that map not contains this kv
        let mut beacons = BTreeSet::new();
        beacons.insert(id);
        let (tx, mut rx) = oneshot::channel();
        self.pending.insert(
            uri.clone(),
            PendingConnection {
                beacon_ids: beacons,
                cancel: tx,
            },
        );

        let scheme = match self.tls {
            true => "https",
            false => "http",
        };

        // attempt to connect
        tokio::spawn(async move {
            let client = loop {
                let uri = format!("{}://{}", scheme, uri.as_str());

                match ProtocolClient::connect(uri.to_string()).await {
                    Ok(client) => {
                        trace!("pool: connected to {uri}");
                        break Some(client);
                    }
                    Err(e) => {
                        error!("pool: connecting to {uri}: {e}")
                    }
                };

                if let Ok(()) = rx.try_recv() {
                    trace!("pool: pending connection {uri} canceled");
                    break None;
                }

                tokio::time::sleep(Duration::from_secs(5)).await
            };

            if client.is_some()
                && sender
                    .send((uri.to_owned(), client.unwrap()))
                    .await
                    .is_err()
            {
                error!("pool: pending: pool receiver is dropped")
            }
        });
    }
    fn get_status(&self) -> PoolStatus {
        let tls = self.tls;
        let mut connected = vec![];
        for (k, v) in self.active.iter() {
            connected.push((
                k.to_string(),
                v.beacon_ids
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<String>>(),
            ));
        }

        let mut pending = vec![];
        for (k, v) in self.pending.iter() {
            pending.push((
                k.to_string(),
                v.beacon_ids
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<String>>(),
            ));
        }

        let active_beacons = self
            .enabled_beacons
            .keys()
            .map(|v| v.to_string())
            .collect::<Vec<String>>();

        PoolStatus {
            tls,
            pending,
            connected,
            active_beacons,
        }
    }

    fn remove_beacon_id(&mut self, beacon_id: &BeaconID) {
        self.enabled_beacons.remove(beacon_id);

        // cancel pending beacon
        let mut cancel = vec![];
        for (k, v) in self.pending.iter_mut() {
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
        for (k, v) in self.active.iter_mut() {
            if v.beacon_ids.remove(beacon_id) && v.beacon_ids.is_empty() {
                disconnect.push(k.clone());
            }
        }

        for k in disconnect {
            self.active.remove(&k);
        }
    }
}

pub struct PoolHandler {
    sender: mpsc::Sender<PoolCmd>,
}

#[derive(Debug)]
pub enum PoolHandlerError {
    Closed,
}

impl PoolHandler {
    pub async fn status(&self) -> Result<PoolStatus, PoolHandlerError> {
        let (tx, rx) = oneshot::channel::<PoolStatus>();
        self.sender
            .send(PoolCmd::Status(tx))
            .await
            .map_err(|_| PoolHandlerError::Closed)?;

        let status = rx.await.map_err(|_| PoolHandlerError::Closed)?;
        Ok(status)
    }

    pub async fn add_id(&self, id: String, uri: Vec<String>) -> Result<(), PoolHandlerError> {
        self.sender
            .send(PoolCmd::AddID(id, uri))
            .await
            .map_err(|_| PoolHandlerError::Closed)
    }
    pub async fn remove_id(&self, id: String) -> Result<(), PoolHandlerError> {
        self.sender
            .send(PoolCmd::RemoveID(id))
            .await
            .map_err(|_| PoolHandlerError::Closed)
    }
    pub async fn partial(&self, packet: PartialBeaconPacket) -> Result<(), PoolHandlerError> {
        self.sender
            .send(PoolCmd::Partial(packet))
            .await
            .map_err(|_| PoolHandlerError::Closed)
    }
}
