mod convert;
pub mod group;
pub mod keys;
pub mod node;
pub mod store;
pub mod toml;

pub use convert::PointSerDeError;
pub use energon::drand::traits::DrandScheme as Scheme;
pub use energon::points::KeyPoint;
pub use energon::points::SigPoint;

pub trait Hash {
    type Hasher;

    fn hash(&self) -> [u8; 32];
}
