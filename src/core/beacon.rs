use super::chain::ChainHandler;
use super::multibeacon::BeaconHandler;

use crate::key::keys::Pair;
use crate::key::store::FileStore;
use crate::key::store::FileStoreError;
use crate::key::toml::PairToml;
use crate::key::toml::Toml;
use crate::key::ConversionError;
use crate::key::Scheme;
use crate::protobuf::drand::ChainInfoPacket;
use crate::protobuf::drand::IdentityResponse;
use crate::store::ChainStore;
use crate::store::NewStore;
use crate::store::StorageConfig;

use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::info_span;
use tracing::Span;

use energon::drand::traits::BeaconDigest;
use std::io::ErrorKind;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::task::TaskTracker;

pub const DEFAULT_BEACON_ID: &str = "default";

pub fn is_default_beacon_id(beacon_id: &str) -> bool {
    beacon_id == DEFAULT_BEACON_ID || beacon_id.is_empty()
}

pub type Callback<T, E> = tokio::sync::oneshot::Sender<Result<T, E>>;

pub struct InnerNode<S: Scheme> {
    pub beacon_id: BeaconID,
    pub fs: FileStore,
    pub keypair: Pair<S>,
    pub tracker: TaskTracker,
    pub span: Span,
}

pub enum BeaconCmd {
    Shutdown(Callback<(), ()>),
    IdentityRequest(Callback<IdentityResponse, ConversionError>),
    Sync(Callback<Arc<ChainStore>, &'static str>),
    ChainInfo(Callback<ChainInfoPacket, &'static str>),
}

#[derive(thiserror::Error, Debug)]
/// For diagnostic, should not be possible.
#[error("callback receiver has already been dropped")]
struct SendError;

#[derive(PartialEq, Eq, Clone)]
pub struct BeaconID {
    inner: Arc<str>,
}

impl std::fmt::Display for BeaconID {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl BeaconID {
    pub fn is_default(&self) -> bool {
        &*self.inner == DEFAULT_BEACON_ID
    }

    pub fn new(id: impl Into<Arc<str>>) -> Self {
        Self { inner: id.into() }
    }

    pub fn as_str(&self) -> &str {
        self.inner.as_ref()
    }

    pub fn is_eq(&self, id: &str) -> bool {
        self.inner.as_ref() == id
    }
}

pub struct BeaconProcess<S: Scheme>(Arc<InnerNode<S>>);

impl<S: Scheme> BeaconProcess<S> {
    fn from_arc(node: Arc<InnerNode<S>>) -> Self {
        Self(node)
    }

    pub fn tracker(&self) -> &TaskTracker {
        &self.tracker
    }

    pub fn run(fs: FileStore, pair: PairToml, id: &str) -> Result<BeaconHandler, FileStoreError> {
        let keypair: Pair<S> = Toml::toml_decode(&pair).ok_or(FileStoreError::TomlError)?;
        let (tx, mut rx) = mpsc::channel::<BeaconCmd>(30);

        let chain: Option<ChainHandler> = match fs.load_group() {
            // TODO: load share, db
            Ok(groupfile) => Some(ChainHandler::new(groupfile)),
            Err(err) => match err {
                // Ignore IO error `NotFound`.
                FileStoreError::IO(error) => {
                    if error.kind() == ErrorKind::NotFound {
                        info!("beacon id [{id}]: will run as fresh install -> expect to run DKG.");
                        None
                    } else {
                        return Err(FileStoreError::IO(error));
                    }
                }
                _ => return Err(err),
            },
        };

        let span = info_span!(
            "",
            id = format!("{}.{id}", keypair.public_identity().address())
        );

        let db_path = fs.db_path();

        let beacon_node = Self(Arc::new(InnerNode {
            beacon_id: BeaconID::new(id),
            fs,
            keypair,
            tracker: TaskTracker::new(),
            span,
        }));

        let node = Self::from_arc(beacon_node.0.clone());
        beacon_node.tracker().spawn(async move {
            // Attempt to load database
            let mut store = if chain.is_some() {
                let store = ChainStore::new(
                    StorageConfig {
                        path: Some(db_path.clone()), // TODO: postgres config
                        ..Default::default()
                    },
                    S::Beacon::is_chained(),
                )
                .await
                .unwrap();
                Some(Arc::new(store))
            } else {
                None
            };

            while let Some(cmd) = rx.recv().await {
                match cmd {
                    BeaconCmd::Shutdown(callback) => {
                        if let Err(err) = node.shutdown(callback) {
                            error!("failed to shutdown gracefully, {err}")
                        };
                        break;
                    }
                    BeaconCmd::IdentityRequest(callback) => {
                        if let Err(err) = node.identity_request(callback) {
                            error!("failed to send identity responce, {err}")
                        }
                    }
                    BeaconCmd::Sync(callback) => match &store {
                        Some(store) => {
                            if callback.send(Ok(Arc::clone(store))).is_err() {
                                error!("failed to proceed sync request")
                            }
                        }
                        None => {
                            let new_store = Arc::new(
                                ChainStore::new(
                                    StorageConfig {
                                        path: Some(db_path.clone()), // TODO: postgres config
                                        ..Default::default()
                                    },
                                    S::Beacon::is_chained(),
                                )
                                .await
                                .unwrap(),
                            );
                            store = Some(new_store.clone());

                            if callback.send(Ok(new_store)).is_err() {
                                error!("failed to send chain_info, receiver is dropped")
                            }
                        }
                    },
                    BeaconCmd::ChainInfo(callback) => match chain.as_ref() {
                        Some(chain_handler) => {
                            if callback.send(Ok(chain_handler.chain_info())).is_err() {
                                error!("failed to send chain_info, receiver is dropped")
                            }
                        }
                        None => {
                            if callback.send(Err("no dkg group setup yet")).is_err() {
                                error!("failed to send chain_info, receiver is dropped")
                            }
                        }
                    },
                }
            }
        });

        Ok(BeaconHandler {
            beacon_id: BeaconID::new(id),
            tracker: beacon_node.tracker().clone(),
            tx,
        })
    }

    fn shutdown(&self, sender: Callback<(), ()>) -> anyhow::Result<()> {
        debug!("received shutdown for {}", self.beacon_id);

        // TODO: remove 'result' once implemented:
        //  - beacon_id removed from connection pool is_ok
        //  - database closed correctly is_ok
        //  - chain handler stopped correctly is_ok
        //  - any other tasks which might be spawned by tracker.
        let result = Ok(());
        sender.send(result).map_err(|_| SendError)?;

        Ok(())
    }

    fn identity_request(
        &self,
        sender: Callback<IdentityResponse, ConversionError>,
    ) -> anyhow::Result<()> {
        sender
            // From generic into raw types: crate::key::conversion
            // TODO: Error details should be logged from backends, well-defined err values
            .send(self.keypair.public_identity().try_into())
            .map_err(|_| SendError)?;

        Ok(())
    }
}

impl<S: Scheme> std::ops::Deref for BeaconProcess<S> {
    type Target = InnerNode<S>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
