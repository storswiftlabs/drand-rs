// Copyright 2023-2025 StorSwift Inc.
// SPDX-License-Identifier: Apache-2.0

//! Transport module abstracts a validation layer between Protobuf and internal types
//! and provides logic for their conversion.

pub mod dkg;
pub mod drand;
pub mod utils;
pub use utils::ConvertProto;
