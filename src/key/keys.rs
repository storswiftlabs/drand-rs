use super::Hash;
use super::Scheme;
use crate::net::utils::Address;

use energon::backends::error::PointError;
use energon::drand::SchemeError;
use energon::kyber::poly::PriShare;
use energon::points::KeyPoint;
use energon::points::SigPoint;
use energon::traits::Affine;
use energon::traits::ScalarField;
use sha2::Digest;

/// Pair is a wrapper around a random scalar and the corresponding public key
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
        msg.extend_from_slice(key.hash()?.as_slice());

        let signature = S::bls_sign(&msg, &private)?;
        let public = Identity::new(address, key, signature);

        Ok(Self::new(private, public))
    }

    /// Signs the public key with the key pair
    pub fn self_sign(&mut self) -> Result<(), SchemeError> {
        let mut msg = S::ID.as_bytes().to_vec();
        let pk_hash = self.public_identity().hash()?;
        msg.extend_from_slice(&pk_hash);
        self.public.signature = S::bls_sign(&msg, self.private_key())?;

        Ok(())
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
    address: Address,
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

    /// Hash returns the hash of the public key without signing the signature. The hash
    /// is the input to the signature Scheme. It does *not* hash the address field as
    /// this may need to change while the node keeps the same key.
    pub fn hash(&self) -> Result<[u8; 32], PointError> {
        self.key.hash()
    }

    pub fn is_valid_signature(&self) -> bool {
        if let Ok(hash) = self.hash() {
            let mut msg = S::ID.as_bytes().to_vec();
            msg.extend_from_slice(hash.as_slice());
            return S::bls_verify(&self.key, &self.signature, &msg).is_ok();
        }

        false
    }
}

impl<S: Scheme> Hash for Identity<S> {
    type Hasher = crev_common::Blake2b256;

    fn hash(&self) -> Result<[u8; 32], PointError> {
        let mut h = Self::Hasher::new();
        let msg = self.key().serialize()?;
        h.update(msg);

        Ok(h.finalize().into())
    }
}

// DistPublic represents the distributed public key generated during a DKG. This
// is the information that can be safely exported to end users verifying a
// drand signature. It is the list of all commitments of the coefficients of the
// private distributed polynomial.
#[derive(Debug, Default, PartialEq)]
pub struct DistPublic<S: Scheme> {
    commits: Vec<KeyPoint<S>>,
}

impl<S: Scheme> DistPublic<S> {
    pub fn new(commits: Vec<KeyPoint<S>>) -> Self {
        Self { commits }
    }

    pub fn from_bytes(bytes: &[Vec<u8>]) -> Result<Self, PointError> {
        let mut commits = Vec::with_capacity(bytes.len());

        for commit in bytes.iter() {
            commits.push(Affine::deserialize(commit)?);
        }

        Ok(Self::new(commits))
    }

    pub fn commits(&self) -> &[KeyPoint<S>] {
        &self.commits
    }
}

/// Share represents the private information that a node holds after a successful
/// DKG. This information MUST stay private !
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

    /// Public returns the distributed public key associated with the distributed key share
    pub fn public(&self) -> &DistPublic<S> {
        &self.commits
    }

    /// Private returns the private share used to produce a partial signature
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
