use crate::{
    chain::{ChainError, StoreError},
    core::{beacon::BeaconProcessError, multibeacon::BeaconHandlerError},
    dkg::ActionsError,
    net::control::CONTROL_HOST,
    protobuf::drand::{Metadata, NodeVersion},
};
use http::uri::Authority;
use std::{
    error::Error,
    fmt::{Debug, Display},
    str::FromStr,
    time::Duration,
};
use tokio::{net::TcpListener, sync::oneshot};
use tonic::{transport::Channel, Status};

pub(super) const ERR_METADATA_IS_MISSING: &str = "metadata is missing";

/// Connection timeout for transport channel.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(not(any(test, feature = "insecure")))]
/// Returns a channel for a generic Tonic client with TLS configuration.
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
pub async fn connect(peer: &Address) -> anyhow::Result<Channel> {
    let channel = Channel::from_shared(format!("http://{peer}"))?
        .connect_timeout(CONNECT_TIMEOUT)
        .connect()
        .await?;
    Ok(channel)
}

#[cfg(not(any(test, feature = "insecure")))]
/// Returns a channel for a generic Tonic client with TLS configuration.
/// Does not attempt to connect to the endpoint until first use.
pub fn connect_lazy(peer: &Address) -> anyhow::Result<Channel> {
    let channel = Channel::from_shared(format!("https://{peer}"))?
        .tls_config(tonic::transport::ClientTlsConfig::new().with_native_roots())?
        .connect_timeout(CONNECT_TIMEOUT)
        .keep_alive_while_idle(true)
        .keep_alive_timeout(Duration::from_secs(60))
        .connect_lazy();
    Ok(channel)
}

#[cfg(any(test, feature = "insecure"))]
/// Returns a channel for a generic Tonic client without TLS configuration.
/// Does not attempt to connect to the endpoint until first use.
pub fn connect_lazy(peer: &Address) -> anyhow::Result<Channel> {
    let channel = Channel::from_shared(format!("http://{peer}"))?
        .connect_timeout(CONNECT_TIMEOUT)
        .keep_alive_while_idle(true)
        .keep_alive_timeout(Duration::from_secs(60))
        .connect_lazy();
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

impl Metadata {
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
                patch: 3,
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

#[allow(dead_code, reason = "reserved for tests")]
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

/// Converts the underlying error into a [`Status`], including the provided beacon id.
pub trait ToStatus {
    fn to_status(&self, id: &str) -> Status;
}

impl ToStatus for tokio::sync::oneshot::error::RecvError {
    /// This error should not be possible. Means that callback sender is dropped without sending.
    fn to_status(&self, id: &str) -> Status {
        Status::internal(format!("{id}: internal error*"))
    }
}

impl ToStatus for ChainError {
    fn to_status(&self, id: &str) -> Status {
        Status::aborted(format!("{id}: {self}"))
    }
}

impl ToStatus for StoreError {
    fn to_status(&self, id: &str) -> Status {
        Status::aborted(format!("{id}: {self}"))
    }
}

impl ToStatus for BeaconHandlerError {
    fn to_status(&self, id: &str) -> Status {
        Status::unknown(format!("{id}: {self}"))
    }
}

impl ToStatus for BeaconProcessError {
    /// TODO: well-define error values, see [`ConversionError`]
    fn to_status(&self, id: &str) -> Status {
        Status::unknown(format!("{id}: {self}"))
    }
}

impl ToStatus for InvalidAddress {
    fn to_status(&self, id: &str) -> Status {
        Status::invalid_argument(format!("{id}: {}", self.0))
    }
}

impl ToStatus for ActionsError {
    fn to_status(&self, id: &str) -> Status {
        Status::aborted(format!("{id}: {self}",))
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

impl<T, E: Error> Callback<T, E> {
    pub fn new() -> (Self, oneshot::Receiver<Result<T, E>>) {
        let (tx, rx) = oneshot::channel();
        (Self { inner: tx }, rx)
    }

    #[inline]
    pub fn reply(self, result: Result<T, E>) {
        // TODO: monitoring on server side.
        let _ = self.inner.send(result);
    }
}
