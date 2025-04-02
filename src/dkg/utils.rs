use crate::key::KeyPoint;
use crate::key::Scheme;
use crate::key::SigPoint;
use crate::transport::dkg::Participant;

use energon::traits::Affine;

impl Participant {
    pub fn is_valid_signature<S: Scheme>(&self) -> bool {
        use crev_common::Blake2b256;
        use sha2::Digest;

        if let Ok(key) = KeyPoint::<S>::deserialize(&self.key) {
            if let Ok(signature) = SigPoint::<S>::deserialize(&self.signature) {
                let mut hasher = Blake2b256::new();
                hasher.update(&self.key);
                let msg = [S::ID.as_bytes(), hasher.finalize().as_slice()].concat();

                if S::bls_verify(&key, &signature, &msg).is_ok() {
                    return true;
                }
            }
        }

        false
    }
}
