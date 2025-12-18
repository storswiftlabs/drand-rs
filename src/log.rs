// Copyright 2023-2025 StorSwift Inc.
// SPDX-License-Identifier: Apache-2.0

//! Abstraction for logging API to simplify changing dependencies.
mod api {
    //! Note: Reuse existing logger instances from respective layers, see [`setup_logger`].
    pub use slog::Logger;
    use slog::{Drain, Level, LevelFilter};
    use slog_term::{CountingWriter, FullFormat, PlainSyncDecorator};
    use std::{fmt::Display, io::Write, sync::OnceLock};

    static LOG_LEVEL: OnceLock<Level> = OnceLock::new();

    fn get_level() -> Level {
        *LOG_LEVEL.get().unwrap_or(&Level::Debug)
    }

    pub fn set_verbose(verbose: bool) {
        if LOG_LEVEL.get().is_none() {
            let _ = LOG_LEVEL.set(if verbose { Level::Debug } else { Level::Info });
        }
    }

    /// This function intentionally leaks the header string for performance.
    ///
    ///  LAYER   CALL           USAGE                    HEADER FORMAT
    ///  Daemon   1     daemon, rpc, pool         `private_listen`
    ///  Beacon   1     beacon process (manager)  `private_listen`.`beacon_id`
    ///  DKG:     2     dkg, chain, chain store   `private_listen`.`beacon_id`.`dkg_index`
    fn setup_logger(custom_header: String, lvl: Level) -> Logger {
        let header: &str = Box::new(custom_header).leak();
        let handle = PlainSyncDecorator::new(std::io::stdout());
        let drain = FullFormat::new(handle)
            .use_file_location()
            .use_utc_timestamp()
            .use_custom_header_print(move |fn_timestamp, mut rd, record, _| {
                fn_timestamp(&mut rd)?;
                write!(
                    rd,
                    " {} {header:<29}{}:{:<7}",
                    record.level().as_short_str(),
                    record.location().file,
                    record.location().line
                )?;

                let mut count_rd: CountingWriter<'_> = CountingWriter::new(&mut rd);
                write!(count_rd, "{}", record.msg())?;
                Ok(count_rd.count() != 0)
            })
            .build()
            .fuse();

        Logger::root(LevelFilter::new(drain, lvl).fuse(), slog::o!())
    }

    pub fn set_node(private_listen: &str) -> Logger {
        let header = private_listen.to_string();
        setup_logger(header, get_level())
    }

    pub fn set_id(private_listen: &str, beacon_id: &str) -> Logger {
        let header = format!("{private_listen}.{beacon_id}");
        setup_logger(header, Level::Debug)
    }

    pub fn set_chain(private_listen: &str, beacon_id: &str, dkg_index: u32) -> Logger {
        let header = format!("{private_listen}.{beacon_id}.{dkg_index}");
        setup_logger(header, get_level())
    }

    // `dkg_index` is relaxed from u32 to impl Display to cover DKG index(string) for leaving node.
    pub fn set_dkg(private_listen: &str, beacon_id: &str, dkg_index: impl Display) -> Logger {
        let header = format!("{private_listen}.{beacon_id}.{dkg_index}");
        setup_logger(header, Level::Debug)
    }

    #[macro_export]
    macro_rules! debug {
        ($log:expr, $($arg:tt)*) => {
            slog::debug!($log, $($arg)*)
        };
    }

    #[macro_export]
    macro_rules! info {
        ($log:expr, $($arg:tt)*) => {
            slog::info!($log, $($arg)*)
        };
    }

    #[macro_export]
    macro_rules! warn {
        ($log:expr, $($arg:tt)*) => {
            slog::warn!($log, $($arg)*)
        };
    }

    #[macro_export]
    macro_rules! error {
        ($log:expr, $($arg:tt)*) => {
            slog::error!($log, $($arg)*)
        };
    }
}

pub use api::*;
