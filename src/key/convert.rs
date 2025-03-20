//! Conversion between inner generic types and their raw representation

use super::keys::Identity;
use super::Scheme;
use crate::protobuf::drand::IdentityResponse;

use energon::backends::error::BackendsError;
use energon::traits::Affine;

/// Error originates from points serialize/deserialize.
#[derive(thiserror::Error, Debug)]
pub enum PointSerDeError {
    #[error("key_point error: {0}")]
    KeyPoint(BackendsError),
    #[error("sig_point error: {0}")]
    SigPoint(BackendsError),
}

// Transport::Identity -> Identity<S>
impl<S: Scheme> TryFrom<crate::transport::drand::Identity> for Identity<S> {
    type Error = PointSerDeError;

    fn try_from(identity: crate::transport::drand::Identity) -> Result<Self, Self::Error> {
        let key = Affine::deserialize(&identity.key).map_err(PointSerDeError::KeyPoint)?;
        let signature =
            Affine::deserialize(&identity.signature).map_err(PointSerDeError::SigPoint)?;

        Ok(Self::new(identity.address, key, signature))
    }
}

// Identity<S> -> Transport::Identity
impl<S: Scheme> TryFrom<&Identity<S>> for crate::transport::drand::Identity {
    type Error = PointSerDeError;

    fn try_from(identity: &Identity<S>) -> Result<Self, Self::Error> {
        Ok(Self {
            address: identity.address.to_owned(),
            key: identity
                .key()
                .serialize()
                .map_err(PointSerDeError::KeyPoint)?
                .into(),
            signature: identity
                .signature()
                .serialize()
                .map_err(PointSerDeError::SigPoint)?
                .into(),
        })
    }
}

// Identity<S> -> proto::IdentityResponse
impl<S: Scheme> TryFrom<&Identity<S>> for IdentityResponse {
    type Error = PointSerDeError;

    fn try_from(identity: &Identity<S>) -> Result<Self, Self::Error> {
        Ok(Self {
            address: identity.address().to_string(),
            key: identity
                .key()
                .serialize()
                .map_err(PointSerDeError::KeyPoint)?
                .into(),
            signature: identity
                .signature()
                .serialize()
                .map_err(PointSerDeError::SigPoint)?
                .into(),
            metadata: None,
            scheme_name: S::ID.to_string(),
        })
    }
}

// Identity<S> -> proto::Participant
impl<S: Scheme> TryFrom<&Identity<S>> for crate::transport::dkg::Participant {
    type Error = PointSerDeError;

    fn try_from(identity: &Identity<S>) -> Result<Self, Self::Error> {
        Ok(Self {
            address: identity.address.to_owned(),
            key: identity
                .key()
                .serialize()
                .map_err(PointSerDeError::KeyPoint)?
                .into(),
            signature: identity
                .signature()
                .serialize()
                .map_err(PointSerDeError::SigPoint)?
                .into(),
        })
    }
}
