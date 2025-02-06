use http::uri::Authority;
use std::fmt::Display;
use std::str::FromStr;

/// Implementation of authority component of a URI which is always contain host and port.
/// For validation rules, see [`Address::precheck`].
#[derive(Debug, PartialEq)]
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

impl Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.as_str())
    }
}

#[derive(thiserror::Error, Debug)]
#[error("expected valid host:port, received {0}")]
pub struct InvalidAddress(String);

#[derive(Debug, Default, PartialEq)]
pub struct Seconds {
    value: u32,
}

impl Seconds {
    pub fn new(value: u32) -> Self {
        Self { value }
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

#[cfg(test)]
mod default_impl {
    use super::*;

    impl Default for Address {
        fn default() -> Self {
            Self(Authority::from_static("test:1"))
        }
    }
}
