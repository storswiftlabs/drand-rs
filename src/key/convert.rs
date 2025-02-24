//! Converts between inner generic types and their raw representation

use super::Scheme;
use crate::protobuf::drand::IdentityResponse;

use energon::traits::Affine;

pub type ConversionError = energon::drand::traits::SchemeError;

// Transport::Identity -> Identity<S>
impl<S: Scheme> TryFrom<crate::transport::drand::Identity> for super::keys::Identity<S> {
    type Error = ConversionError;

    fn try_from(value: crate::transport::drand::Identity) -> Result<Self, Self::Error> {
        let crate::transport::drand::Identity {
            address,
            ref key,
            ref signature,
        } = value;

        let key = Affine::deserialize(key)?;
        let signature = Affine::deserialize(signature)?;

        Ok(Self::new(address, key, signature))
    }
}

// Identity<S> -> proto::IdentityResponse
impl<S: Scheme> TryFrom<&super::keys::Identity<S>> for IdentityResponse {
    type Error = ConversionError;

    fn try_from(identity: &super::keys::Identity<S>) -> Result<Self, Self::Error> {
        let address = identity.address().to_string();
        let scheme_name = S::ID.to_string();
        let key = identity.key().serialize()?.into();
        let signature = identity.signature().serialize()?.into();

        Ok(Self {
            address,
            key,
            signature,
            metadata: None,
            scheme_name,
        })
    }
}
