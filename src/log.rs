//! Abstraction for logging API to simplify changing dependencies (ongoing test).

mod api {
    use std::fmt::Display;
    use tracing::dispatcher;
    pub use tracing::Span as Logger;
    use tracing_subscriber::{
        fmt::time, prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt, EnvFilter,
    };

    pub fn set_verbose(verbose: bool) {
        if !dispatcher::has_been_set() {
            let filter = EnvFilter::builder().parse_lossy(if verbose {
                "drand=debug,energon=debug"
            } else {
                "drand=info,energon=debug"
            });

            let layer = tracing_subscriber::fmt::layer()
                .with_timer(time::time())
                .with_target(false)
                .with_file(true)
                .with_line_number(true)
                .with_ansi(false);

            tracing_subscriber::registry()
                .with(layer)
                .with(filter)
                .try_init()
                .unwrap();
        }
    }

    pub fn set_node(private_listen: &str) -> Logger {
        tracing::info_span!("", node = format!("{private_listen}"))
    }

    pub fn set_id(private_listen: &str, beacon_id: &str) -> Logger {
        tracing::info_span!("", id = format!("{private_listen}.{beacon_id}"))
    }

    pub fn set_chain(private_listen: &str, beacon_id: &str, dkg_index: u32) -> Logger {
        tracing::info_span!(
            "",
            chain = format!("{private_listen}.{beacon_id}.{dkg_index}")
        )
    }

    // (dkg_index: impl Display) to cover leaving node tag.
    pub fn set_dkg(private_listen: &str, beacon_id: &str, dkg_index: impl Display) -> Logger {
        tracing::info_span!(
            "",
            dkg = format!("{private_listen}.{beacon_id}.{dkg_index}")
        )
    }

    #[macro_export]
    macro_rules! debug {
        ($log:expr, $($arg:tt)*) => {
            tracing::debug!(parent: $log, $($arg)*)
        };
    }

    #[macro_export]
    macro_rules! info {
        ($log:expr, $($arg:tt)*) => {
            tracing::info!(parent: $log, $($arg)*)
        };
    }

    #[macro_export]
    macro_rules! warn {
        ($log:expr, $($arg:tt)*) => {
            tracing::warn!(parent: $log, $($arg)*)
        };
    }

    #[macro_export]
    macro_rules! error {
        ($log:expr, $($arg:tt)*) => {
            tracing::error!(parent: $log, $($arg)*)
        };
    }
}

pub use api::*;
