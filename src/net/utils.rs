use crate::chain::ChainError;
use crate::chain::StoreError;
use crate::core::multibeacon::BeaconHandlerError;
use crate::dkg::ActionsError;
use crate::key::PointSerDeError;
use crate::net::control::CONTROL_HOST;
use crate::protobuf::drand::Metadata;
use crate::protobuf::drand::NodeVersion;

use http::uri::Authority;
use std::error::Error;
use std::fmt::Debug;
use std::fmt::Display;
use std::str::FromStr;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tonic::transport::Channel;
use tonic::Status;

pub(super) const ERR_METADATA_IS_MISSING: &str = "metadata is missing";

/// Connection timeout for transport channel.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(not(any(test, feature = "insecure")))]
/// Returns a channel for a generic Tonic client with TLS configuration.
/// Returns an error if the connection cannot be established.
pub async fn connect(peer: &Address) -> anyhow::Result<Channel> {
    let channel = Channel::from_shared(format!("https://{peer}"))?
        .tls_config(tonic::transport::ClientTlsConfig::new().with_native_roots())?
        .connect_timeout(CONNECT_TIMEOUT)
        .connect()
        .await?;
    Ok(channel)
}

#[cfg(any(test, feature = "insecure"))]
/// Returns a channel for a generic Tonic client without TLS configuration.
/// Returns an error if the connection cannot be established.
pub async fn connect(peer: &Address) -> anyhow::Result<Channel> {
    let channel = Channel::from_shared(format!("http://{peer}"))?
        .connect_timeout(CONNECT_TIMEOUT)
        .connect()
        .await?;
    Ok(channel)
}

/// Address is protected type of URI Authority which always contains host:port (see [`Address::precheck`]).
#[derive(Eq, PartialEq, Clone)]
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

impl Debug for Address {
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

#[derive(Debug, Default, PartialEq, Copy, Clone)]
pub struct Seconds {
    value: u32,
}

impl Seconds {
    pub fn new(value: u32) -> Self {
        Self { value }
    }

    pub fn get_value(self) -> u32 {
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
    pub(super) fn with_default() -> Self {
        Self {
            node_version: Some(VERSION),
            ..Default::default()
        }
    }

    pub fn with_id(beacon_id: String) -> Self {
        Self {
            node_version: Some(VERSION),
            beacon_id,
            chain_hash: vec![],
        }
    }

    pub fn with_chain_hash(beacon_id: &str, chain_hash: &str) -> anyhow::Result<Self> {
        let metadata = Self {
            node_version: Some(VERSION),
            beacon_id: beacon_id.into(),
            chain_hash: hex::decode(chain_hash)?,
        };

        Ok(metadata)
    }

    /// Bypass version check.
    pub fn golang_node_version(beacon_id: String, chain_hash: Option<&[u8]>) -> Self {
        Metadata {
            node_version: Some(NodeVersion {
                major: 2,
                minor: 1,
                patch: 2,
                prerelease: String::new(),
            }),
            beacon_id,
            chain_hash: chain_hash.unwrap_or_default().into(),
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
/// TODO: random sockets for e2e.
#[allow(dead_code, reason = "tests")]
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

impl ToStatus for ChainError {
    fn to_status(&self, id: &str) -> Status {
        Status::aborted(format!("beacon id '{id}', {self}"))
    }
}

impl ToStatus for StoreError {
    fn to_status(&self, id: &str) -> Status {
        Status::aborted(format!("beacon id '{id}', {self}"))
    }
}

impl ToStatus for BeaconHandlerError {
    fn to_status(&self, id: &str) -> Status {
        Status::unknown(format!("beacon id '{id}', {self}"))
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

    /// Sends a response back and tracks all outcoming errors if verbose flag is set.
    pub fn reply(self, result: Result<T, E>) {
        if tracing::enabled!(tracing::Level::DEBUG) {
            if let Err(err) = &result {
                tracing::error!("callback returns with the error: {err}");
            }
        }

        if self.inner.send(result).is_err() {
            tracing::error!("{ERR_SEND}");
        };
    }
}
