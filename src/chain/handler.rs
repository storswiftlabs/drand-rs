use energon::kyber::tbls;

#[derive(thiserror::Error, Debug)]
pub enum ChainError {
    #[error("attempt to process beacon from node of index {0}, but it was not in the group file")]
    UnknownIndex(u32),
    #[error("received partial with invalid signature")]
    InvalidPartialSignature,
    #[error("tbls: {0}")]
    TBlsError(#[from] tbls::TBlsError),
}
