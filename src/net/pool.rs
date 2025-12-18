// Copyright 2023-2025 StorSwift Inc.
// SPDX-License-Identifier: Apache-2.0

//! Connection pool for sending [`PartialBeaconPacket`]s.
use super::utils::Address;
use crate::{
    core::beacon::BeaconID,
    log::Logger,
    net::protocol::ProtocolClient,
    protobuf::drand::PartialBeaconPacket,
    {debug, error, info},
};
use tokio::sync::mpsc::{self, error::SendError};

#[derive(thiserror::Error, Debug)]
pub enum PoolError {
    #[error("beacon id is already enabled")]
    DuplicatedID,
    #[error("received new beacon id with empty peers list")]
    InvalidPeersConfig,
    #[error("failed to remove, id is not found")]
    UnknownIDToRemove,
    #[error("received packet without metadata")]
    MetadataNotFound,
    #[error("received packet for unknown id")]
    UnknownID,
    #[error("received invalid peer address")]
    InvalidURI,
    #[error("partial pool is closed")]
    Closed,
}

pub enum PoolCmd {
    Broadcast(PartialBeaconPacket),
    AddID(BeaconID, Vec<Address>),
    RemoveID(BeaconID),
}

#[derive(Clone)]
struct Peer {
    address: String,
    client: ProtocolClient,
}

pub struct Pool {
    beacons: Vec<(BeaconID, Vec<Peer>)>,
    log: Logger,
}

#[derive(Clone)]
pub struct PoolSender {
    sender: mpsc::Sender<PoolCmd>,
}

impl Pool {
    /// Adds new beacon id with its peers into the pool.
    fn add_id(&mut self, new_id: BeaconID, peers: Vec<Address>) -> Result<(), PoolError> {
        if peers.is_empty() {
            return Err(PoolError::InvalidPeersConfig);
        }

        if self.beacons.iter().any(|(id, _)| *id == new_id) {
            return Err(PoolError::DuplicatedID);
        };

        let mut peers_for_id: Vec<Peer> = Vec::with_capacity(peers.len());
        for peer in peers {
            if let Some(connection) = self
                .beacons
                .iter()
                .flat_map(|(_, connected)| connected)
                .find(|conn| conn.address == peer.as_str())
            {
                debug!(
                    &self.log,
                    "add_id [{new_id}]: reusing connection for {}", connection.address
                );
                peers_for_id.push(connection.clone());
            } else {
                let address = peer.to_string();
                debug!(
                    &self.log,
                    "add_id [{new_id}]: adding new peer into the pool: {address}",
                );
                let client = ProtocolClient::new_lazy(&peer).map_err(|_| PoolError::InvalidURI)?;
                peers_for_id.push(Peer { address, client });
            }
        }
        let peers_info: Vec<&str> = peers_for_id.iter().map(|p| p.address.as_str()).collect();
        debug!(
            &self.log,
            "add_id [{new_id}]: configuration is finished, peers: {peers_info:#?}"
        );
        self.beacons.push((new_id, peers_for_id));

        Ok(())
    }

    /// Removes ID with its peers from the pool.
    fn remove_id(&mut self, id: BeaconID) -> Result<(), PoolError> {
        let pos = self
            .beacons
            .iter()
            .position(|(other, _)| *other == id)
            .ok_or(PoolError::UnknownIDToRemove)?;
        self.beacons.swap_remove(pos);

        Ok(())
    }

    #[allow(
        clippy::needless_range_loop,
        reason = "indexing required to send last packet without cloning"
    )]
    fn broadcast(&self, packet: PartialBeaconPacket) -> Result<(), PoolError> {
        let metadata = packet
            .metadata
            .as_ref()
            .ok_or(PoolError::MetadataNotFound)?;

        let (id, conn) = self
            .beacons
            .iter()
            .find(|(id, _)| id.is_eq(metadata.beacon_id.as_str()))
            .ok_or(PoolError::UnknownID)?;

        // Clone and send partial for n-1 connections.
        let last_idx = conn.len() - 1;
        let round = packet.round;
        for i in 0..last_idx {
            let mut peer = conn[i].clone();
            let packet = packet.clone();
            let id = id.to_string();
            let log = self.log.clone();
            debug!(&log, "sending partial: round {round}, to {}", peer.address);
            tokio::task::spawn(async move {
                if let Err(err) = peer.client.partial_beacon(packet).await {
                    error!(
                        log,
                        "failed to send partial: id {id}, round {round}, to {} {}",
                        peer.address,
                        err.root_cause()
                    );
                }
            });
        }
        // Send partial to last connection by value.
        let mut peer = conn[last_idx].clone();
        let id = id.to_string();
        let log = self.log.clone();
        debug!(&log, "sending partial: round {round}, to {}", peer.address);
        tokio::task::spawn(async move {
            if let Err(err) = peer.client.partial_beacon(packet).await {
                error!(
                    log,
                    "failed to send partial: id {id}, round {round} to {}, {}",
                    peer.address,
                    err.root_cause()
                );
            }
        });
        Ok(())
    }

    pub fn start(log: Logger) -> PoolSender {
        let (tx_cmd, mut rx_cmd) = mpsc::channel::<PoolCmd>(1);
        tokio::spawn(async move {
            let mut pool = Self {
                beacons: Vec::with_capacity(3),
                log,
            };
            info!(&pool.log, "initializing empty pool for partial packets ...");

            loop {
                tokio::select! {
                    cmd = rx_cmd.recv()=> {
                        if let Some(cmd) = cmd{
                            match cmd{
                                PoolCmd::Broadcast(packet) =>{
                                    if let Err(err) = pool.broadcast(packet){
                                        error!(&pool.log, "cmd broadcast: {err}");
                                    }
                                }
                                PoolCmd::AddID(id, peers) => {
                                    if let Err(err) = pool.add_id(id, peers){
                                        error!(&pool.log, "cmd add_id: {err}");
                                    }
                                }
                                PoolCmd::RemoveID(id) => {
                                    if let Err(err) = pool.remove_id(id){
                                        error!(&pool.log, "cmd remove_id: {err}");
                                    };
                                    debug!(&pool.log, "beacon ID [{id}] is removed from pool");
                                }
                            }
                        }
                    }
                }
            }
        });

        PoolSender { sender: tx_cmd }
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
        self.sender.send(PoolCmd::Broadcast(packet)).await?;

        Ok(())
    }
}

impl From<SendError<PoolCmd>> for PoolError {
    fn from(_: SendError<PoolCmd>) -> Self {
        PoolError::Closed
    }
}
