//! Helpers for e2e tests

use crate::cli::Cmd;
use crate::cli::Config;
use crate::cli::KeyGenConfig;
use crate::cli::CLI;

use crate::net::control;
use crate::net::protocol;
use crate::net::utils::TestListener;

use crate::core::daemon::Daemon;
use crate::key::Scheme;

use energon::drand::schemes::DefaultScheme;
use std::net::SocketAddr;
use std::str::FromStr;
use tempfile::TempDir;
use tokio::net::TcpListener;

impl CLI {
    pub(super) fn keygen(config: KeyGenConfig) -> Self {
        Self {
            verbose: true,
            commands: Cmd::GenerateKeypair(config),
        }
    }

    pub(super) fn load_id(control: &str, id: &str) -> Self {
        Self {
            verbose: true,
            commands: Cmd::Load {
                control: control.to_owned(),
                id: id.to_owned(),
            },
        }
    }
}

/// Initial config for tests,
pub struct TestConfig {
    pub fs_guard: TempDir,
    pub folder: String,
    pub control: TcpListener,
    pub private_listen: TcpListener,
    pub id: Option<String>,
    pub scheme: String,
}

impl TestConfig {
    pub async fn as_default() -> Self {
        let fs_guard = TempDir::new().unwrap();
        let folder = fs_guard.path().join("test").as_path().display().to_string();

        Self {
            fs_guard,
            folder,
            control: TcpListener::bind("127.0.0.1:0").await.unwrap(),
            private_listen: TcpListener::bind("127.0.0.1:0").await.unwrap(),
            id: "default".to_string().into(),
            scheme: DefaultScheme::ID.to_string(),
        }
    }

    pub async fn _new(control_port: &str, private_listen: &str) -> Self {
        let fs_guard = TempDir::new().unwrap();
        let folder = fs_guard.path().join("test").as_path().display().to_string();

        Self {
            fs_guard,
            folder,
            control: TcpListener::bind(format!("127.0.0.1:{control_port}"))
                .await
                .unwrap(),
            private_listen: TcpListener::bind(SocketAddr::from_str(private_listen).unwrap())
                .await
                .unwrap(),
            id: "default".to_string().into(),
            scheme: DefaultScheme::ID.to_string(),
        }
    }

    /// Returns start config compatible with app cli type
    pub fn start_config(&self) -> Config {
        Config {
            folder: self.folder.to_owned(),
            control: self.control.local_addr().unwrap().port().to_string(),
            private_listen: self.private_listen.local_addr().unwrap().to_string(),
            id: self.id.to_owned(),
        }
    }

    /// Returns keygen config compatible with app cli type
    pub(super) fn keygen_config(&self) -> KeyGenConfig {
        KeyGenConfig {
            folder: self.folder.to_owned(),
            control: self.control.local_addr().unwrap().port().to_string(),
            address: self.private_listen.local_addr().unwrap().to_string(),
            id: self.id.to_owned().unwrap(),
            scheme: self.scheme.to_owned(),
        }
    }

    pub(super) async fn start_drand_node(self) -> (TempDir, Config) {
        let start_config = self.start_config();
        let daemon = Daemon::new(&start_config).await.unwrap();

        tokio::spawn(async move {
            tokio::try_join!(
                daemon.tracker.spawn(control::start_server::<TestListener>(
                    daemon.clone(),
                    self.control
                )),
                daemon.tracker.spawn({
                    protocol::start_server::<TestListener>(daemon.clone(), self.private_listen)
                })
            )
            .unwrap()
        });

        (self.fs_guard, start_config)
    }
}
