use super::beacon::BeaconCmd;
use super::beacon::BeaconID;
use super::dkg::DkgCmd;
use super::multibeacon::BeaconHandler;
use super::multibeacon::MultiBeacon;
use crate::log::Logger;
use crate::net::pool::Pool;
use crate::net::pool::PoolCmd;
use crate::net::pool::PoolPartial;
use crate::net::transport::Bundle;
use crate::net::transport::DkgInfo;
use crate::net::transport::IdentityResponse;
use crate::net::utils::INTERNAL_ERR;
use crate::protobuf::drand::PartialBeaconPacket;

use anyhow::bail;
use anyhow::Result;
use core::time::Duration;
use std::sync::Arc;
use tokio::sync::broadcast::Sender;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tonic::Status;

pub struct Daemon {
    beacons: MultiBeacon,
    logger: Logger,
    pub stop_daemon: tokio::sync::broadcast::Sender<()>,
    private_listen: String,
    pool: mpsc::Sender<PoolCmd>,
}

impl Daemon {
    pub fn new(
        private_listen: String,
        base_folder: &str,
        beacon_id: Option<&String>,
        logger: Logger,
    ) -> Result<Arc<Self>> {
        let pool = Pool::start(false, logger.register_pool());
        let beacons = MultiBeacon::new(beacon_id, base_folder, &private_listen, pool.clone())?;
        let (stop_daemon, _) = tokio::sync::broadcast::channel::<()>(1);
        let daemon = Self {
            beacons,
            stop_daemon,
            logger,
            private_listen,
            pool,
        };

        Ok(Arc::new(daemon))
    }

    /// Returns true if no loaded beacons left
    pub async fn stop_id(&self, id: &BeaconID) -> Result<bool> {
        let snapshot = self.beacons().shapshot();

        // Stop daemon if ONLY target id is running now
        if snapshot.len() == 1 && snapshot[0].id() == id {
            self.stop_daemon().await?;
            Ok(true)
        }
        // otherwise stop id and update MultiBeacon state
        else if let Some(handler) = snapshot.iter().find(|h| h.id() == id) {
            let (tx, rx) = oneshot::channel();
            handler.sender().send(BeaconCmd::Shutdown(tx)).await?;
            rx.await??;

            let new_store = snapshot
                .iter()
                .filter(|x| x.id() != handler.id())
                .cloned()
                .collect::<Vec<BeaconHandler>>();

            self.beacons().replace_store(Arc::new(new_store));
            return Ok(false);
        } else {
            bail!("beacon [{id}] is not loaded")
        }
    }

    /// Stops all beacons and shutdown daemon
    pub async fn stop_daemon(&self) -> Result<(), anyhow::Error> {
        for handler in self.beacons().shapshot().iter() {
            let (tx, rx) = oneshot::channel();
            handler.sender().send(BeaconCmd::Shutdown(tx)).await?;
            rx.await??;
        }
        let shutdown = Sender::clone(&self.stop_daemon);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = shutdown.send(());
        });
        Ok(())
    }

    pub fn beacons(&self) -> &MultiBeacon {
        &self.beacons
    }

    pub fn span(&self) -> &tracing::Span {
        &self.logger.span
    }
    pub fn private_listen(&self) -> &str {
        &self.private_listen
    }

    pub async fn identity_request(&self, id: &str) -> Result<IdentityResponse, Status> {
        let (tx, callback) = oneshot::channel();

        self.beacons()
            .cmd(BeaconCmd::IdentityRequest(tx), id)
            .await
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        callback
            .await
            .map_or_else(|_| Err(Status::internal(INTERNAL_ERR)), Ok)
    }

    pub async fn broadcast_dkg_bundle(&self, bundle: Bundle, id: &str) -> Result<(), Status> {
        let (tx, callback) = oneshot::channel();
        // TODO: move tx from DkgCmd into BeaconCmd: otherwise it might return INTERNAL_ERR but actually nothing is brocken and logs should be less dramatic
        self.beacons()
            .cmd(BeaconCmd::Dkg(DkgCmd::Broadcast(tx, bundle)), id)
            .await
            .map_err(|e| Status::from_error(e.into()))?;

        resolve(callback).await
    }

    pub async fn push_new_group(&self, info: DkgInfo, id: &str) -> Result<(), Status> {
        let (tx, callback) = oneshot::channel();

        self.beacons()
            .cmd(BeaconCmd::Dkg(DkgCmd::NewGroup(tx, info)), id)
            .await
            .map_err(|e| Status::from_error(e.into()))?;

        resolve(callback).await
    }

    pub async fn new_partial(&self, packet: PartialBeaconPacket, id: &str) -> Result<(), Status> {
        let (sender, callback) = oneshot::channel();
        self.beacons()
            .cmd(BeaconCmd::Partial(packet, sender), id)
            .await
            .map_err(|e| Status::from_error(e.into()))?;

        resolve(callback).await
    }

    pub async fn pool_status(&self) -> Result<String, Status> {
        let (sender, callback) = oneshot::channel();
        self.pool
            .send(PoolCmd::Status(sender))
            .await
            .map_err(|e| Status::from_error(e.into()))?;

        callback
            .await
            .map_or_else(|_| Err(Status::internal(INTERNAL_ERR)), Ok)
    }
}

async fn resolve(callback: oneshot::Receiver<Result<()>>) -> Result<(), Status> {
    // TODO: all of [`Status::from_error`] should be limited within enum errorr
    //       At border [transport -> protobuf] anyhow should not be used
    callback
        .await
        // callback is dropped
        .map_err(|_| Status::internal(INTERNAL_ERR))?
        // received error from callback
        .map_err(|e| Status::from_error(e.into()))
}
