use tracing::dispatcher;
use tracing_subscriber::fmt::time;
use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

pub fn setup_tracing(verbose: bool) -> anyhow::Result<()> {
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
            .with_ansi(true);

        tracing_subscriber::registry()
            .with(layer)
            .with(filter)
            .try_init()?;
    }

    Ok(())
}
