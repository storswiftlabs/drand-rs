use crate::cli::*;
use crate::core::daemon::Daemon;
use crate::dkg::status::Status;
use crate::key::Scheme;

use crate::net::control;
use crate::net::dkg_control::DkgControlClient;
use crate::net::protocol;
use crate::net::utils::TestListener;

use energon::drand::schemes::DefaultScheme;
use tempfile::TempDir;
use tokio::net::TcpListener;

/// RPC to check DKG state machine statuses
///
/// # Arguments
///
///  * `port` - Node control port
///  * `id` - ID of beacon process
///  * `check_current` - Expected current status
///  * `check_finished` - Expected finished status, should be always `None` for fresh nodes.
pub async fn assert_statuses(
    port: &str,
    id: &str,
    check_current: Status,
    check_finished: Option<Status>,
) {
    let mut client = DkgControlClient::new(port)
        .await
        .expect("connection: DkgControl service should be reachable");
    let response = client
        .dkg_status(id)
        .await
        .expect("DkgControl::Status should not fail");

    // get statuses from response
    let current = response
        .current
        .expect("the 'current' DkgEntry should always have a value")
        .state;
    let finished = response.complete;

    // assert current
    assert_eq!(check_current, Status::try_from(current).unwrap());
    // assert finished
    if let Some(expected) = check_finished {
        let received = Status::try_from(finished.unwrap().state).unwrap();
        assert_eq!(
            expected, received,
            "'completed' DKG statuses mismatch, expected: {expected}, received: {received}",
        )
    } else {
        assert_eq!(None, finished)
    }
}

/// Helper to run CLI for Drand-go.
pub async fn cmd_golang(script: &str) {
    let output = match async_std::process::Command::new("/bin/bash")
        .args(["-c", script])
        .spawn()
    {
        Ok(mut cmd) => cmd.status().await.unwrap(),
        Err(err) => panic!("failed to spawn cmd {}, error: {}", script, err),
    };

    if !output.success() {
        panic!(
            "failed to execute script: {script}, code: {:?}",
            output.code()
        )
    }
}

// TODO: replace to rpc from rs.
pub async fn cmd_golang_args(script: &str, joiners: Vec<&str>) {
    let joiners_string = joiners.join(" ");

    let mut cmd = async_std::process::Command::new("/bin/bash");
    cmd.arg("-c").arg(format!("{script} {joiners_string}"));

    let output = match cmd.spawn() {
        Ok(mut cmd) => cmd.status().await.unwrap(),
        Err(err) => panic!("failed to spawn cmd {}, error: {}", script, err),
    };

    if !output.success() {
        panic!(
            "failed to execute script: {script}, code: {:?}",
            output.code()
        );
    }
}

impl CLI {
    pub fn keygen(config: KeyGenConfig) -> Self {
        Self {
            verbose: true,
            commands: Cmd::GenerateKeypair(config),
        }
    }

    pub fn load_id(control: &str, id: &str) -> Self {
        Self {
            verbose: true,
            commands: Cmd::Load {
                control: control.to_owned(),
                id: id.to_owned(),
            },
        }
    }

    pub fn dkg_join(control: &str, id: &str) -> Self {
        Self {
            verbose: true,
            commands: Cmd::Dkg(crate::cli::Dkg::Join {
                control: control.to_owned(),
                id: id.to_owned(),
            }),
        }
    }

    pub fn stop(control: &str, id: Option<&str>) -> Self {
        Self {
            verbose: true,
            commands: Cmd::Stop {
                control: control.to_owned(),
                id: id.map(|x| x.to_string()),
            },
        }
    }
}

/// Config for single node
pub struct TestConfig {
    pub fs_guard: TempDir,
    pub folder: String,
    pub control: TcpListener,
    pub private_listen: TcpListener,
    pub id: Option<String>,
    pub scheme: String,
}

impl TestConfig {
    /// Returns config with random ports and default values.
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

    pub async fn new(control_port: usize, private_listen: usize, scheme: &str, id: &str) -> Self {
        let fs_guard = TempDir::new().unwrap();
        let folder = fs_guard.path().join("test").as_path().display().to_string();
        let control = TcpListener::bind(format!("127.0.0.1:{control_port}"))
            .await
            .unwrap();
        let private_listen = TcpListener::bind(format!("127.0.0.1:{private_listen}"))
            .await
            .unwrap();

        Self {
            fs_guard,
            folder,
            control,
            private_listen,
            id: id.to_string().into(),
            scheme: scheme.to_string(),
        }
    }

    /// Returns start config compatible with CLI.
    pub fn start_config(&self) -> Config {
        Config {
            folder: self.folder.to_owned(),
            control: self.control.local_addr().unwrap().port().to_string(),
            private_listen: self.private_listen.local_addr().unwrap().to_string(),
            id: self.id.to_owned(),
        }
    }

    /// Returns keygen config compatible with CLI.
    pub fn keygen(&self) -> KeyGenConfig {
        KeyGenConfig {
            folder: self.folder.to_owned(),
            control: self.control.local_addr().unwrap().port().to_string(),
            address: self.private_listen.local_addr().unwrap().to_string(),
            id: self.id.to_owned().unwrap(),
            scheme: self.scheme.to_owned(),
        }
    }

    pub async fn start_node(self) -> (TempDir, Config) {
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

/// Contains nodes-rs filesystem and CLI config.
pub struct Nodes {
    configs: Vec<Config>,
    _fs: Vec<TempDir>,
    beacon_id: String,
}

impl Nodes {
    /// Generates n keypairs with provided scheme and beacon id. Starts n daemons.
    pub async fn generate(n: usize, scheme: &str, id: &str) -> Self {
        let mut configs = Vec::with_capacity(n);
        let mut _fs = Vec::with_capacity(n);

        let control = 8800;
        let address = 22000;

        for i in 0..n {
            let config = TestConfig::new(control + i, address + i, scheme, id).await;
            assert!(CLI::keygen(config.keygen()).run().await.is_ok());
            let (fs_guard, config) = config.start_node().await;
            configs.push(config);
            _fs.push(fs_guard);
        }

        // sleep 1sec to account slow CI/CD pipelines
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        Self {
            configs,
            _fs,
            beacon_id: id.to_string(),
        }
    }

    /// Checks if current and completed statuses are equal to expected
    pub async fn assert_statuses(&self, check_current: Status, check_finished: Option<Status>) {
        for node in self.configs.iter() {
            assert_statuses(
                &node.control,
                &self.beacon_id,
                check_current,
                check_finished,
            )
            .await;
        }
    }

    pub async fn dkg_join(&self) {
        for node in self.configs.iter() {
            assert!(CLI::dkg_join(&node.control, &self.beacon_id,)
                .run()
                .await
                .is_ok());
        }
    }

    pub async fn stop_daemon(&self) {
        for node in self.configs.iter() {
            assert!(CLI::stop(&node.control, None).run().await.is_ok())
        }
    }

    /// Returns a list of internet facing ipv4 nodes addresses.  
    ///
    /// TLS termination excluded from testground, private-listen addresses are same with internet facing ones.
    pub fn get_addresses(&self) -> Vec<&str> {
        self.configs
            .iter()
            .map(|c| c.private_listen.as_str())
            .collect()
    }
}
