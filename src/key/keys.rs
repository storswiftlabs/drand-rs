use super::Scheme;
use crate::net::utils::Address;
use energon::{
    drand::error::SchemeError,
    kyber::poly::PriShare,
    points::{KeyPoint, SigPoint},
    traits::{Affine, ScalarField},
};

/// Pair is a wrapper around a random scalar and the corresponding public key.
#[derive(Debug, PartialEq)]
pub struct Pair<S: Scheme> {
    private: S::Scalar,
    public: Identity<S>,
}

impl<S: Scheme> Pair<S> {
    pub fn new(private: S::Scalar, public: Identity<S>) -> Self {
        Self { private, public }
    }

    /// Returns a freshly created private / public key pair.
    pub fn generate(address: Address) -> Result<Self, SchemeError> {
        let private = S::Scalar::random();
        let key = S::sk_to_pk(&private);
        let mut msg = S::ID.as_bytes().to_vec();
        let hashed_key = key.hash().map_err(SchemeError::Backends)?;
        msg.extend_from_slice(hashed_key.as_slice());
        let signature = S::bls_sign(&msg, &private)?;
        let public = Identity::new(address, key, signature);

        Ok(Self::new(private, public))
    }

    pub fn private_key(&self) -> &S::Scalar {
        &self.private
    }

    pub fn public_identity(&self) -> &Identity<S> {
        &self.public
    }
}

/// Identity holds the corresponding public key of a Private. It also includes a
/// valid internet facing ipv4 address where to this reach the node holding the
/// public / private key pair.
#[derive(Debug, PartialEq)]
pub struct Identity<S: Scheme> {
    pub address: Address,
    key: KeyPoint<S>,
    signature: SigPoint<S>,
}

impl<S: Scheme> Identity<S> {
    pub fn new(address: Address, key: KeyPoint<S>, signature: SigPoint<S>) -> Self {
        Self {
            address,
            key,
            signature,
        }
    }

    pub fn address(&self) -> &str {
        self.address.as_str()
    }

    pub fn key(&self) -> &KeyPoint<S> {
        &self.key
    }

    pub fn signature(&self) -> &SigPoint<S> {
        &self.signature
    }
}

// DistPublic represents the distributed public key generated during a DKG. This
// is the information that can be safely exported to end users verifying a
// drand signature. It is the list of all commitments of the coefficients of the
// private distributed polynomial.
#[derive(Debug, Default, PartialEq)]
pub struct DistPublic<S: Scheme> {
    pub commits: Vec<KeyPoint<S>>,
}

impl<S: Scheme> DistPublic<S> {
    pub fn new(commits: Vec<KeyPoint<S>>) -> Self {
        Self { commits }
    }

    pub fn commits(&self) -> &[KeyPoint<S>] {
        &self.commits
    }
}

/// Share represents the private information that a node holds
/// after a successful DKG. This information MUST stay private!
#[derive(PartialEq)]
pub struct Share<S: Scheme> {
    /// Coefficients of the public polynomial holding the public key.
    commits: DistPublic<S>,
    /// Share of the distributed secret which is private information.
    pri_share: PriShare<S>,
}

impl<S: Scheme> Share<S> {
    pub fn new(commits: DistPublic<S>, pri_share: PriShare<S>) -> Self {
        Self { commits, pri_share }
    }

    /// Public returns the distributed public key associated with the distributed key share.
    pub fn public(&self) -> &DistPublic<S> {
        &self.commits
    }

    /// Private returns the private share used to produce a partial signature.
    pub fn private(&self) -> &PriShare<S> {
        &self.pri_share
    }
}

#[cfg(test)]
mod default_impl {
    use super::*;

    impl<S: Scheme> Default for Pair<S> {
        fn default() -> Self {
            let public = Identity::new(
                Address::default(),
                KeyPoint::<S>::default(),
                SigPoint::<S>::default(),
            );

            Self::new(Default::default(), public)
        }
    }

    impl<S: Scheme> Default for Share<S> {
        fn default() -> Self {
            Self {
                commits: DistPublic { commits: vec![] },
                pri_share: PriShare::new(0, S::Scalar::default()),
            }
        }
    }
}
