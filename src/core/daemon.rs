use super::{
    beacon::BeaconCmd,
    multibeacon::{BeaconHandler, BeaconHandlerError, MultiBeacon},
};
use crate::{
    cli::Config,
    key::store::{FileStore, FileStoreError},
    net::utils::{Callback, StartServerError},
};
use std::{path::PathBuf, sync::Arc};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{error, info};

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
    tracker: TaskTracker,
    token: CancellationToken,
    beacons: MultiBeacon,
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

    pub fn is_last_id_to_shutdown(&self, id: &str) -> bool {
        let snapshot = self.beacons().snapshot();
        snapshot.len() == 1 && snapshot[0].id().is_eq(id)
    }

    pub async fn stop_id(&self, id: &str) -> Result<(), BeaconHandlerError> {
        let snapshot = self.beacons().snapshot();
        let handler = snapshot
            .iter()
            .find(|h| h.id().is_eq(id))
            .ok_or(BeaconHandlerError::UnknownID)?;

        info!("processing stop_id request for [{id}] ...");
        let new_store = snapshot
            .iter()
            .filter(|x| x.id() != handler.id())
            .cloned()
            .collect::<Vec<BeaconHandler>>();
        self.beacons().replace_store(Arc::new(new_store));

        let (tx, rx) = Callback::new();
        handler
            .process_tx
            .send(BeaconCmd::Shutdown(tx))
            .await
            .map_err(|_| BeaconHandlerError::ClosedBpRx)?;

        rx.await
            .map_err(|_| BeaconHandlerError::DroppedCallback)?
            .map_err(BeaconHandlerError::ShutdownError)?;

        Ok(())
    }

    pub async fn stop_daemon(&self) -> Result<(), BeaconHandlerError> {
        info!("processing stop request for daemon ...");
        let snapshot = self.beacons().snapshot();
        self.beacons().replace_store(Arc::new(vec![]));

        for h in snapshot.iter() {
            let (tx, rx) = Callback::new();
            h.process_tx
                .send(BeaconCmd::Shutdown(tx))
                .await
                .map_err(|_| BeaconHandlerError::ClosedBpRx)?;

            rx.await
                .map_err(|_| BeaconHandlerError::DroppedCallback)?
                .map_err(BeaconHandlerError::ShutdownError)?;
        }
        self.tracker.close();
        self.token.cancel();

        Ok(())
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

    pub fn tracker(&self) -> TaskTracker {
        self.tracker.clone()
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.token.clone()
    }
}
