pub mod group;
pub mod keys;
pub mod node;
pub mod store;
pub mod toml;

/// Re-export often used types
pub use energon::drand::traits::DrandScheme as Scheme;
pub use energon::points::KeyPoint;
pub use energon::points::SigPoint;

use energon::backends::error::PointError;

pub trait Hash {
    type Hasher;

    fn hash(&self) -> Result<[u8; 32], PointError>;
}
