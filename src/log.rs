use std::sync::Arc;
use tracing::Span;
use tracing_subscriber::fmt::time;
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

pub fn init_log(verbose: bool) -> anyhow::Result<()> {
    let filter = EnvFilter::builder().parse_lossy(match verbose {
        true => "drand=trace",
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
        .try_init()
        .map_err(|e| anyhow::anyhow!(e))
}

pub struct Logger {
    host: Arc<str>,
    pub span: Span,
}

impl Logger {
    pub fn register_node(private_listen: &str) -> Self {
        let span = tracing::info_span!("", node = private_listen);
        Self {
            host: private_listen.into(),
            span,
        }
    }

    pub fn register_pool(&self) -> Self {
        let span = tracing::info_span!("", pool = self.host.as_ref());
        Self {
            host: Arc::clone(&self.host),
            span,
        }
    }
    // TODO: use this to specify also dkg index
    pub fn new_child(&self, mut args: String) -> Self {
        args.insert_str(0, self.host.as_ref());
        let span = tracing::info_span!("", id = args);
        Self {
            host: Arc::clone(&self.host),
            span,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::info;

    #[test]
    fn simple_log() {
        init_log(true).unwrap();
        let host = "127.0.0.1:321";
        let id = "default";
        let dkg_index = 8;

        let node_log = Logger::register_node(host);
        info!(parent: &node_log.span, "node");

        let id_log = node_log.new_child(format!(".{id}"));
        info!(parent: &id_log.span, "node.id");

        let log_id_dkg = id_log.new_child(format!(".{id}.{dkg_index}"));
        info!(parent: &log_id_dkg.span, "node.id.dkg_index");
    }
}
