use crate::core::beacon::ChainInfoError;
use crate::core::beacon::SyncError;
use crate::core::multibeacon::BeaconHandlerError;
use crate::dkg::ActionsError;
use crate::key::PointSerDeError;
use crate::net::control::CONTROL_HOST;
use crate::protobuf::drand::Metadata;
use crate::protobuf::drand::NodeVersion;

use http::uri::Authority;
use std::error::Error;
use std::fmt::Display;
use std::str::FromStr;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tonic::Status;

pub(super) const ERR_METADATA_IS_MISSING: &str = "metadata is missing";
// TODO: tls enabled by default, const value will be gated behind default/insecure features.
pub const URI_SCHEME: &str = "http";

/// Implementation of authority component of a URI which is always contain host and port.
/// For validation rules, see [`Address::precheck`].
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Address(Authority);

impl Address {
    pub fn precheck(data: &str) -> Result<Self, InvalidAddress> {
        let authority = data
            .parse::<http::uri::Authority>()
            .map_err(|err| InvalidAddress(format!("{data}, source: {err:?}")))?;

        if authority.host().is_empty() || authority.port().is_none() {
            return Err(InvalidAddress(data.into()));
        }

        Ok(Self(authority))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl PartialOrd for Address {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Address {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.as_str().cmp(other.0.as_str())
    }
}

impl Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.as_str())
    }
}

#[derive(thiserror::Error, Debug)]
#[error("expected valid host:port, received {0}")]
pub struct InvalidAddress(String);

const VERSION: NodeVersion = NodeVersion {
    major: 0,
    minor: 2,
    patch: 0,
    prerelease: String::new(),
};

#[derive(Debug, Default, PartialEq, Clone)]
pub struct Seconds {
    value: u32,
}

impl Seconds {
    pub fn new(value: u32) -> Self {
        Self { value }
    }

    pub fn get_value(&self) -> u32 {
        self.value
    }
}

impl From<u32> for Seconds {
    fn from(value: u32) -> Self {
        Seconds { value }
    }
}

impl From<Seconds> for u32 {
    fn from(seconds: Seconds) -> Self {
        seconds.value
    }
}

impl std::fmt::Display for Seconds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}s", self.value)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ParseSecondsError;

impl FromStr for Seconds {
    type Err = ParseSecondsError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value = value
            .strip_suffix("s")
            .ok_or(ParseSecondsError)?
            .parse()
            .map_err(|_| ParseSecondsError)?;

        Ok(Self::new(value))
    }
}

/// Error type for failed connection attempt, contains address and underlying error
#[derive(thiserror::Error, Debug)]
pub struct ConnectionError {
    pub address: String,
    pub error: tonic::transport::Error,
}

impl std::fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "could not establish connection to {},", self.address)?;
        if let Some(source) = self.error.source() {
            write!(f, " {source}")
        } else {
            write!(f, " {}", self.error)
        }
    }
}

impl Metadata {
    /// Default implementation of `Metadata` which always contains [`NodeVersion`]
    ///
    /// Note: This function  should be used instead of default impl provided by [`::prost::Message`]
    pub(super) fn with_default() -> Option<Self> {
        let metadata = Self {
            node_version: Some(VERSION),
            ..Default::default()
        };

        Some(metadata)
    }

    pub fn with_id(beacon_id: &str) -> Option<Self> {
        let metadata = Self {
            node_version: Some(VERSION),
            beacon_id: beacon_id.into(),
            chain_hash: vec![],
        };

        Some(metadata)
    }

    pub fn with_chain_hash(beacon_id: &str, chain_hash: &str) -> anyhow::Result<Self> {
        let metadata = Self {
            node_version: Some(VERSION),
            beacon_id: beacon_id.into(),
            chain_hash: hex::decode(chain_hash)?,
        };

        Ok(metadata)
    }

    /// Bypass go-version check. This is weird and should be aligned.
    pub fn mimic_version(beacon_id: &str, chain_hash: &[u8]) -> Self {
        Metadata {
            node_version: Some(NodeVersion {
                major: 2,
                minor: 1,
                patch: 0,
                prerelease: String::new(),
            }),
            beacon_id: beacon_id.into(),
            chain_hash: chain_hash.into(),
        }
    }
}

/// Helper trait for binding TCP listeners.
pub trait NewTcpListener {
    type Error: Display;
    type Config;

    fn bind(
        config: Self::Config,
    ) -> impl std::future::Future<Output = Result<TcpListener, Self::Error>>;
}

pub struct ControlListener;
pub struct NodeListener;
pub struct TestListener;

impl NewTcpListener for ControlListener {
    type Error = std::io::Error;
    // control port from cli agrs
    type Config = String;

    /// Attempt to bind a listener for localhost control server.
    async fn bind(port: Self::Config) -> Result<TcpListener, Self::Error> {
        TcpListener::bind(format!("{CONTROL_HOST}:{port}")).await
    }
}

impl NewTcpListener for NodeListener {
    type Error = std::io::Error;
    // Prechecked Authority
    type Config = Address;

    /// Attempt to bind a listener for internet-facing IPv4 node address.
    async fn bind(address: Self::Config) -> Result<TcpListener, std::io::Error> {
        TcpListener::bind(address.as_str()).await
    }
}

#[cfg(test)]
impl NewTcpListener for TestListener {
    type Error = std::convert::Infallible;
    type Config = TcpListener;

    async fn bind(test: Self::Config) -> Result<TcpListener, Self::Error> {
        Ok(test)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum StartServerError {
    #[error("failed to start control server")]
    FailedToStartControl,
    #[error("failed to start node server")]
    FailedToStartNode,
}

/// Converts the underlying error into a [`Status`], including the provided beacon id.
pub trait ToStatus {
    fn to_status(&self, id: &str) -> Status;
}

impl ToStatus for tokio::sync::oneshot::error::RecvError {
    /// This error should not be possible. Means that callback sender is dropped without sending.
    fn to_status(&self, id: &str) -> Status {
        Status::internal(format!("beacon id '{id}' internal RecvError"))
    }
}

impl ToStatus for ChainInfoError {
    fn to_status(&self, id: &str) -> Status {
        Status::aborted(format!("beacon id '{id}', {self}"))
    }
}

impl ToStatus for SyncError {
    fn to_status(&self, id: &str) -> Status {
        Status::aborted(format!("beacon id '{id}', {self}"))
    }
}

impl ToStatus for BeaconHandlerError {
    fn to_status(&self, id: &str) -> Status {
        match self {
            BeaconHandlerError::UnknownID => {
                Status::invalid_argument(format!("beacon id '{id}' is not running"))
            }
            // This error should not be possible. Means that beacon cmd receiver has been dropped.
            BeaconHandlerError::SendError => {
                Status::internal(format!("beacon id '{id}' internal SendError"))
            }
            BeaconHandlerError::AlreadyLoaded => {
                Status::invalid_argument(format!("beacon id '{id}' is already running"))
            }
        }
    }
}

impl ToStatus for PointSerDeError {
    /// TODO: well-define error values, see [`ConversionError`]
    fn to_status(&self, id: &str) -> Status {
        Status::invalid_argument(format!("beacon id '{id}', conversion error: {self}"))
    }
}

impl ToStatus for InvalidAddress {
    fn to_status(&self, id: &str) -> Status {
        Status::invalid_argument(format!("beacon id '{id}', {}", self.0))
    }
}

impl ToStatus for ActionsError {
    fn to_status(&self, id: &str) -> Status {
        Status::aborted(format!("beacon id '{id}', {self}",))
    }
}

impl Default for Address {
    fn default() -> Self {
        Self(Authority::from_static("default:1"))
    }
}

pub struct Callback<T, E: Error> {
    inner: oneshot::Sender<Result<T, E>>,
}

pub const ERR_SEND: &str = "callback receiver is dropped";

impl<T, E: Error> Callback<T, E> {
    pub fn new() -> (Self, oneshot::Receiver<Result<T, E>>) {
        let (tx, rx) = oneshot::channel();
        (Self { inner: tx }, rx)
    }

    /// Sends a response back and tracks all outcoming errors
    #[inline(always)]
    pub fn reply(self, result: Result<T, E>) {
        if let Err(err) = &result {
            tracing::error!("failed to proceed request: {err}")
        }

        if self.inner.send(result).is_err() {
            tracing::error!("{ERR_SEND}");
        };
    }
}
