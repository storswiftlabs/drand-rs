// Copyright (C) 2023-2024 StorSwift Inc.
// This file is part of the Drand-RS library.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
