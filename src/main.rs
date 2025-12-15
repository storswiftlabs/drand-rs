// Copyright 2023-2025 StorSwift Inc.
// SPDX-License-Identifier: Apache-2.0

#![warn(clippy::pedantic)]
#![allow(clippy::unreadable_literal, clippy::inline_always)]

// Backends.
#[cfg(all(feature = "default", feature = "asm"))]
compile_error!("Features 'default' and 'asm' are mutually exclusive");
#[cfg(all(feature = "default", feature = "ark_asm"))]
compile_error!("Features 'default' and 'ark-asm' are mutually exclusive");
#[cfg(all(feature = "asm", feature = "ark_asm"))]
compile_error!("Features 'asm' and 'ark-asm' are mutually exclusive");
#[cfg(not(any(feature = "default", feature = "asm", feature = "ark_asm")))]
compile_error!("One of features 'default', 'asm', or 'ark-asm' must be enabled");

mod chain;
mod cli;
mod core;
mod dkg;
mod key;
mod log;
mod net;
mod transport;
#[allow(clippy::all, clippy::pedantic)]
#[rustfmt::skip]
mod protobuf;

#[cfg(feature = "test-integration")]
#[cfg(test)]
mod test_with_golang;

use clap::Parser;
use cli::Cli;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Cli::parse().run().await
}
