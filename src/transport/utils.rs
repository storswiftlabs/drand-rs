use crate::dkg::state_machine::state::StateError;
use crate::net::utils::InvalidAddress;

use tonic::Status;

/// Trait for conversion from protobuf.
///
/// Abstracts validation boundary for *inner non-generic types*.
pub trait ConvertProto: Sized {
    type Inner;

    /// Returns inner type if data is valid.
    fn validate(self) -> Result<Self::Inner, TransportError>;
}

/// Helper trait to handle optional fields related to prost implementation
///  <https://github.com/tokio-rs/prost?tab=readme-ov-file#field-modifiers>.
pub(super) trait RequireSome<T> {
    /// Returns error [`TransportError::DataIsMissing`] if `Option<T>` is `None`
    ///
    /// Used to bypass <https://github.com/tokio-rs/prost/issues/521>
    fn require_some(self) -> Result<T, TransportError>;
}

impl<T> RequireSome<T> for Option<T> {
    #[inline(always)]
    fn require_some(self) -> Result<T, TransportError> {
        self.ok_or_else(|| TransportError::DataIsMissing(core::any::type_name::<T>().into()))
    }
}

#[inline(always)]
pub(super) fn from_vec<I, T, U>(data: I) -> Vec<U>
where
    I: IntoIterator<Item = T>,
    T: Into<U>,
{
    data.into_iter().map(Into::into).collect()
}

#[inline(always)]
pub(super) fn try_from_vec<I, T, U>(data: I) -> Result<Vec<U>, TransportError>
where
    I: IntoIterator<Item = T>,
    T: ConvertProto<Inner = U>,
{
    data.into_iter().map(ConvertProto::validate).collect()
}

#[derive(thiserror::Error, Debug)]
#[error("transport error: {0}")]
pub enum TransportError {
    #[error(transparent)]
    InvalidAddress(#[from] InvalidAddress),
    #[error(transparent)]
    DkgState(#[from] StateError),
    #[error("data is missing: {0}")]
    DataIsMissing(String),
}

impl From<TransportError> for Status {
    fn from(value: TransportError) -> Self {
        match value {
            TransportError::DataIsMissing(msg) => Status::data_loss(msg),
            _ => Status::invalid_argument(value.to_string()),
        }
    }
}
