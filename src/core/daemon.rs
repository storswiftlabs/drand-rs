use super::beacon::BeaconCmd;
use super::multibeacon::BeaconHandler;
use super::multibeacon::BeaconHandlerError;
use super::multibeacon::MultiBeacon;

use crate::cli::Config;
use crate::key::store::FileStore;
use crate::key::store::FileStoreError;
use crate::net::utils::Callback;
use crate::net::utils::StartServerError;

use tokio::sync::oneshot;
use tokio::time::sleep;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use tracing::debug;
use tracing::error;
use tracing::info;

use std::path::PathBuf;
use std::sync::Arc;

#[derive(thiserror::Error, Debug)]
pub enum DaemonError {
    #[error(transparent)]
    FileStore(#[from] FileStoreError),
    #[error(transparent)]
    BeaconHandler(#[from] BeaconHandlerError),
    #[error(transparent)]
    ServerError(#[from] StartServerError),
}

pub struct Daemon {
    private_listen: String,
    pub tracker: TaskTracker,
    pub token: CancellationToken,
    pub beacons: MultiBeacon,
    multibeacon_path: PathBuf,
}

impl Daemon {
    pub fn new(config: Config) -> Result<Arc<Self>, DaemonError> {
        let tracker: TaskTracker = TaskTracker::new();
        let token: CancellationToken = CancellationToken::new();
        let private_listen = config.private_listen.clone();

        info!(
            "Drand daemon initializing: private_listen: {}, control_port: {}, folder: {}",
            config.private_listen, config.control, config.folder,
        );

        let (multibeacon_path, beacons) = MultiBeacon::new(config)?;
        let daemon = Arc::new(Self {
            private_listen,
            tracker,
            token,
            beacons,
            multibeacon_path,
        });

        Ok(daemon)
    }

    /// Returns true if provided id is the last one in daemon.
    pub fn stop_id(
        &self,
        id: &str,
        tx_graceful: oneshot::Sender<bool>,
    ) -> Result<bool, BeaconHandlerError> {
        let snapshot = self.beacons().snapshot();

        // Stop daemon if ONLY given id is running now
        if snapshot.len() == 1 && snapshot[0].id().is_eq(id) {
            self.stop_daemon(tx_graceful);
            Ok(true)
        } else
        // Otherwise stop id and update multibeacon state
        {
            let handler = snapshot
                .iter()
                .find(|h| h.id().is_eq(id))
                .ok_or(BeaconHandlerError::UnknownID)?;
            // TODO: this should be moved into MultiBeacon method
            let new_store = snapshot
                .iter()
                .filter(|x| x.id() != handler.id())
                .cloned()
                .collect::<Vec<BeaconHandler>>();

            self.beacons().replace_store(Arc::new(new_store));

            let process_tx = handler.process_tx.clone();
            tokio::spawn(async move {
                let (tx, rx) = Callback::new();
                // Shutdown is graceful:
                //  - beacon receiver is not dropped,
                //  - callback awaited is_ok
                //  - result from callback is_ok
                let is_graceful = process_tx.send(BeaconCmd::Shutdown(tx)).await.is_ok()
                    && rx.await.is_ok_and(|result| result.is_ok());
                let _ = tx_graceful.send(is_graceful);
            });

            Ok(false)
        }
    }

    /// Stops all beacons and shutdown daemon
    pub fn stop_daemon(&self, tx_graceful: tokio::sync::oneshot::Sender<bool>) {
        let snapshot = self.beacons().snapshot();
        // non-async: Disable new network requests for beacon(s)
        self.beacons().replace_store(Arc::new(vec![]));

        let tracker = self.tracker.clone();
        let token = self.token.clone();
        tokio::spawn(async move {
            let mut is_graceful = true;
            for h in snapshot.iter() {
                let (beacon_tx, beacon_rx) = Callback::new();
                if let Err(send_err) = h.process_tx.send(BeaconCmd::Shutdown(beacon_tx)).await {
                    error!("should not be possible, id '{}', err: {send_err}", h.id());
                    is_graceful = false;
                }
                if let Err(_) | Ok(Err(_)) = beacon_rx.await {
                    error!("should not be possible, id '{}'", h.id());
                    is_graceful = false;
                }
            }
            debug!("waiting shutdown to complete for daemon");
            tracker.close();
            token.cancel();

            sleep(Duration::from_millis(100)).await;

            // Number of tasks == 1 means the only control server is still
            // running - awaiting the `tx_graceful` callback
            if tracker.len() == 1 && is_graceful {
                let _ = tx_graceful.send(true);
                tracker.wait().await;
                info!("graceful shutdown completed for daemon");
            } else {
                error!(
                    "shutdown is_graceful: {is_graceful}, tasks left: {}",
                    tracker.len()
                );
                let _ = tx_graceful.send(false);
                tracker.wait().await;
            }
        });
    }

    pub fn load_id(&self, id: &str) -> Result<(), BeaconHandlerError> {
        let store = self.beacons.snapshot();
        // Return error if given id is already loaded
        if store.iter().any(|h| h.beacon_id.is_eq(id)) {
            return Err(BeaconHandlerError::AlreadyLoaded);
        }
        let store = FileStore {
            beacon_path: self.multibeacon_path.join(id),
        };
        if let Err(err) = store.validate() {
            error!("failed to validate store: {err}, beacon id: {id}");
            return Err(BeaconHandlerError::UnknownID);
        };

        let new_handler =
            BeaconHandler::new(store, self.beacons.get_pool(), self.private_listen.clone())
                .map_err(|err| {
                    error!("failed to initialize BeaconHandler: {err}, beacon id: {id}");
                    BeaconHandlerError::UnknownID
                })?;

        // Update multibeacon storage with new handler
        // TODO: this should be moved into MultiBeacon method
        let mut handlers = self
            .beacons()
            .snapshot()
            .iter()
            .cloned()
            .collect::<Vec<BeaconHandler>>();
        handlers.push(new_handler);
        self.beacons().replace_store(Arc::new(handlers));

        Ok(())
    }

    pub fn beacons(&self) -> &MultiBeacon {
        &self.beacons
    }
}
