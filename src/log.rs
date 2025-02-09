use std::sync::Arc;
use tracing::Span;
use tracing_subscriber::fmt::time;
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

pub fn init_log(verbose: bool) -> anyhow::Result<()> {
    let filter = EnvFilter::builder().parse_lossy(match verbose {
        true => "drand=debug",
        false => "drand=info",
    });

    let layer = tracing_subscriber::fmt::layer()
        .with_timer(time::time())
        .with_target(false)
        .with_file(true)
        .with_line_number(true)
        .with_ansi(true);

    tracing_subscriber::registry()
        .with(layer)
        .with(filter)
        .try_init()?;

    Ok(())
}

pub struct Logger {
    host: Arc<str>,
    pub span: Span,
}

impl Logger {
    /// Top-level drand node log.
    pub fn register_node(private_listen: &str) -> Self {
        let span = tracing::info_span!("", node = private_listen);
        Self {
            host: private_listen.into(),
            span,
        }
    }

    /// Register new logger for connection pool.
    pub fn register_pool(&self) -> Self {
        let span = tracing::info_span!("", pool = self.host.as_ref());
        Self {
            host: Arc::clone(&self.host),
            span,
        }
    }

    /// Register new logger for beacon id or dkg index
    pub fn new_child(&self, mut args: String) -> Self {
        args.insert_str(0, self.host.as_ref());
        let span = tracing::info_span!("", id = args);
        Self {
            host: Arc::clone(&self.host),
            span,
        }
    }
}
