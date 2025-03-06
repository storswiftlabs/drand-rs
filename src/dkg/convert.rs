use crate::key::keys;
use crate::key::ConversionError;
use crate::key::Scheme;

use energon::traits::Affine;

// Identity<S> -> proto::Participant
impl<S: Scheme> TryFrom<&keys::Identity<S>> for crate::protobuf::dkg::Participant {
    type Error = ConversionError;

    fn try_from(identity: &keys::Identity<S>) -> Result<Self, Self::Error> {
        let address = identity.address().to_string();
        let key = identity.key().serialize()?.into();
        let signature = identity.signature().serialize()?.into();

        Ok(Self {
            address,
            key,
            signature,
        })
    }
}
