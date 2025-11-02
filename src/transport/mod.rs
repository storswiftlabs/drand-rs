//! Transport module abstracts a validation layer between Protobuf and internal types
//! and provides logic for their conversion.

pub mod dkg;
pub mod drand;
pub mod utils;
pub use utils::ConvertProto;
