use crate::core::beacon::BeaconID;
use crate::core::beacon::Callback;
use crate::log::Logger;
use crate::protobuf::drand::protocol_client::ProtocolClient;
use crate::protobuf::drand::PartialBeaconPacket;
use anyhow::bail;
use http::uri::Authority;
use http::uri::PathAndQuery;
use http::uri::Scheme;
use http::Uri;
use indexmap::IndexMap;
use std::cmp::Eq;
use std::collections::HashSet;

use std::fmt::Debug;
use std::fmt::Display;
use std::hash::Hash;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tonic::transport::Channel;
use tracing::error;
use tracing::*;

/// Wrapper over URI which satisfies required prechecks
// TODO: This is dublicate
#[derive(Hash, PartialEq, Eq, Clone)]
pub struct Address(pub Uri);

impl Address {
    pub fn new_checked(addr: &str, tls: bool) -> anyhow::Result<Self> {
        let mut parts = http::uri::Parts::default();

        if let Ok(authority) = addr.parse::<Authority>() {
            if !authority.host().is_empty() && authority.port().is_some() {
                parts.authority = Some(authority)
            }
        };

        if parts.authority.is_none() {
            bail!("authority: expected $HOST:PORT, received {addr}",)
        }

        parts.scheme = match tls {
            true => Some(Scheme::HTTPS),
            false => Some(Scheme::HTTP),
        };

        parts.path_and_query = Some(PathAndQuery::from_static("/"));
        let uri = Uri::from_parts(parts)?;

        Ok(Self(uri))
    }
}

// usage: DST
impl Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let authority = self.0.authority().expect("always contains authority");
        let scheme = self.0.scheme_str().expect("always contains scheme");
        write!(f, "{scheme}://{authority}")
    }
}

// usage: trace
impl Debug for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let authority = self.0.authority().expect("always contains authority");
        write!(f, "{authority}")
    }
}

pub enum PoolCmd {
    Status(Callback<String>),
    Partial(PartialBeaconPacket),
    AddID(BeaconID, Vec<String>),
    RemoveID(BeaconID),
}

// Contains sender associated with gived beacon_id
#[derive(Debug, Default)]
pub struct EnabledID(IndexMap<BeaconID, broadcast::Sender<PartialBeaconPacket>>);

pub trait PoolPartial {
    type Pending: PendingConnections;
    type Beacons;
    type MsgPartial;

    // fn new_empty(logger: Logger) -> Self;
    fn start(tls: bool, logger: Logger) -> mpsc::Sender<PoolCmd>;
    fn add_connection(&mut self, uri: Address, conn: ProtocolClient<Channel>);

    fn broadcast_msg(&self, msg: PartialBeaconPacket);
    fn subscribe_client(&mut self, id: &BeaconID, uri: &Address, conn: ProtocolClient<Channel>);
    fn add_pending(
        &mut self,
        id: BeaconID,
        uri: Address,
        sender: mpsc::Sender<(Address, ProtocolClient<Channel>)>,
    );
}

impl PoolPartial for Pool {
    type Pending = Pending;
    type Beacons = EnabledID;
    type MsgPartial = PartialBeaconPacket;

    fn add_connection(&mut self, uri: Address, conn: ProtocolClient<Channel>) {
        trace!(parent: &self.logger.span, "Established connection: {uri}, current pending: {:#?}",self.pending.0);
        // remove conn from pending list
        if let Some(beacon_ids) = self.pending.0.swap_remove(&uri) {
            // subscribe conn to registered beacons
            beacon_ids
                .iter()
                .for_each(|beacon_id| self.subscribe_client(beacon_id, &uri, conn.clone()));

            self.active
                .0
                .insert(uri, ActiveConnection { conn, beacon_ids });
        } else {
            error!(parent: &self.logger.span,"add_connection:: this should not be possible")
        }
    }

    fn add_pending(
        &mut self,
        id: BeaconID,
        uri: Address,
        sender: mpsc::Sender<(Address, ProtocolClient<Channel>)>,
    ) {
        trace!(parent: &self.logger.span, "pool: pending: add_connection {uri}");
        // update pending list
        // todo: add check that map not contains this kv
        let mut beacons = HashSet::new();
        beacons.insert(id);
        self.pending.0.insert(uri.clone(), beacons);
        // attempt to connect
        let node_span = self.logger.span.to_owned();
        tokio::spawn(async move {
            let client = loop {
                match ProtocolClient::connect(uri.to_string()).await {
                    Ok(client) => {
                        trace!(parent: &node_span, "pool: connected to {uri}");
                        break client;
                    }
                    Err(e) => {
                        error!(parent: &node_span, "pool: connecting to {uri}: {e}")
                    }
                };

                tokio::time::sleep(Duration::from_secs(5)).await
            };
            if sender.send((uri.to_owned(), client)).await.is_err() {
                error!(parent: &node_span, "pool: pending: pool receiver is dropped")
            }
        });
    }

    fn start(tls: bool, logger: Logger) -> mpsc::Sender<PoolCmd> {
        let (tx_cmd, mut rx_cmd) = mpsc::channel::<PoolCmd>(3);
        let (tx_new_conn, mut rx_new_conn) = mpsc::channel::<(Address, ProtocolClient<Channel>)>(1);

        tokio::spawn(async move {
            let mut pool = Self {
                tls,
                active: Connected::default(),
                pending: Pending::default(),
                enabled_beacons: EnabledID::default(),
                logger,
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
                                    pool.broadcast_msg(msg); // dbg

                                }
                                PoolCmd::AddID(id, uris_unchecked) => {
                                    trace!(parent: &pool.logger.span, "PoolCmd::AddID: reseived id {id}, nodes: {uris_unchecked:#?}");
                                    // Add id to enabled_beacons
                                    let (tx_broadcast, _) = tokio::sync::broadcast::channel::<PartialBeaconPacket>(1);
                                            if pool.enabled_beacons.0.insert(id.clone(), tx_broadcast).is_some() {
                                                panic!("reshape not implemeted yet")
                                            }

                                    let mut valid: Vec<Address>=Vec::with_capacity(uris_unchecked.len());

                                    for uri in uris_unchecked.into_iter(){
                                        if let Ok(address)=Address::new_checked(&uri, tls){
                                            valid.push(address);
                                        }
                                    };

                                    for uri in valid.into_iter() {
                                        // check if pool already has been connected to endpoint
                                        if let Some(active)=pool.active.0.get(&uri){
                                            pool.subscribe_client(&id,&uri, active.conn.clone());
                                        }else{
                                            pool.add_pending(id.clone(), uri, tx_new_conn.clone())
                                        }
                                    }
                                }
                                PoolCmd::RemoveID(id) => {
                                    trace!(parent: &pool.logger.span, "PoolCmd::RemoveID:: {id}  == DOING NOTHING ==");
                                }
                                PoolCmd::Status(callback)=>{
                                    let _=callback.send(pool.to_string());
                                }
                            }
                        }
                    }
                }
            }
        });
        tx_cmd
    }

    fn broadcast_msg(&self, msg: PartialBeaconPacket) {
        if self.enabled_beacons.0.is_empty() {
            error!(parent: &self.logger.span,"broadcast: EnabledID: no any id enabled");
        }
        for (id, sender) in self.enabled_beacons.0.iter() {
            if let Some(meta) = &msg.metadata.as_ref() {
                if meta.beacon_id == id.as_str() {
                    if let Err(e) = sender.send(msg.clone()) {
                        error!(parent: &self.logger.span,"broadcast: {e}, id: {}",msg.metadata.as_ref().unwrap().beacon_id)
                    }
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
        if let Some(active) = self.active.0.get_mut(uri) {
            if !active.beacon_ids.contains(id) {
                active.beacon_ids.insert(id.clone());
            }
        }
        if let Some(sender) = self.enabled_beacons.0.get(id) {
            let mut receiver = sender.subscribe();
            trace!(parent: &self.logger.span, "connection {uri:?} is subscribed for {id}");
            tokio::spawn({
                let pin_address = format!("{uri:?}");
                let span = self.logger.new_child(format!(".{id}")).span;

                async move {
                    while let Ok(msg) = receiver.recv().await {
                        let round = msg.round;

                        if let Err(e) = conn.partial_beacon(msg).await {
                            error!(parent: &span, "sending partial: round {round} to: {pin_address}, status: {e:?}")
                        } else {
                            trace!(parent: &span, "sending partial: round {round} to: {pin_address}, status: OK")
                        }
                    }
                    warn!(parent: &span, "disabled subscription: {pin_address}")
                }
            });
        } else {
            // should not happens
            warn!(parent: &self.logger.span, "unable to subscribe connection {uri:?}, beacon_id {id} is disabled")
        }
    }
}
pub struct Pool {
    tls: bool,
    active: Connected,
    pending: Pending,
    enabled_beacons: EnabledID,
    logger: Logger,
}

pub struct ActiveConnection {
    conn: ProtocolClient<Channel>,
    beacon_ids: HashSet<BeaconID>,
}

#[derive(Default)]
pub struct Connected(IndexMap<Address, ActiveConnection>);
#[derive(Debug, Default)]
pub struct Pending(IndexMap<Address, HashSet<BeaconID>>);

// TODO: remove this trait
pub trait PendingConnections: Sized {
    type Uri: Eq + Hash;

    fn new_empty() -> Self;
    fn remove_beacon_id(&mut self, id: &BeaconID);
    fn remove_conn(&mut self, uri: Self::Uri);
}

impl PendingConnections for Pending {
    type Uri = Uri;

    fn new_empty() -> Self {
        Self(IndexMap::new())
    }

    fn remove_beacon_id(&mut self, id: &BeaconID) {
        for (_, conn) in self.0.iter_mut() {
            conn.remove(id);
        }
    }

    fn remove_conn(&mut self, _uri: Self::Uri) {}
}

impl Debug for Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pool")
            .field("tls", &self.tls)
            .field("active", &self.active.0)
            .field("pending", &self.pending.0)
            .field("enabled_beacons", &self.enabled_beacons.0.keys())
            .finish()
    }
}
impl Debug for ActiveConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", &self.beacon_ids)
    }
}

impl Debug for Connected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Connected").field(&self.0).finish()
    }
}

impl Display for Connected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.is_empty() {
            write!(f, "empty")?
        } else {
            for (k, v) in self.0.iter() {
                let mut sorted = Vec::from_iter(v.beacon_ids.iter());
                sorted.sort();
                write!(f, "\n   {k:#?}: {:?}", sorted)?
            }
        }
        Ok(())
    }
}
impl Display for Pending {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.is_empty() {
            write!(f, "empty")?
        } else {
            for (k, v) in self.0.iter() {
                write!(f, "\n   {k:#?}: {:?}", v)?
            }
        }

        Ok(())
    }
}
impl Display for Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "tls: {}\nconnected:{} \npending: {}\nactive beacons: {:?}",
            self.tls,
            self.active,
            self.pending,
            self.enabled_beacons.0.keys()
        )
    }
}
