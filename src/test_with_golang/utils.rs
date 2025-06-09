//! Utilities for testing Drand-rs with Drand-go (v2.1.2-insecure bebad8fc).

use crate::cli::*;
use crate::dkg::status::Status;
use crate::key::Scheme;
use crate::net::dkg_control::DkgControlClient;
use crate::protobuf::dkg::DkgEntry;

use energon::drand::schemes::DefaultScheme;
use energon::kyber::dkg::minimum_t;
use rand::rngs::ThreadRng;
use rand::seq::SliceRandom;
use rand::Rng;
use std::env;
use std::fmt::Write as _;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::sync::LazyLock;
use std::time::Duration;
use tokio::time::sleep;
use tracing::*;

/// Absolute path for Drand-go v2.1.2-insecure bebad8fc
static DRAND_BIN_PATH: LazyLock<String> = LazyLock::new(|| {
    format!(
        "{}/{INNER_PATH}drand_go",
        env::var("CARGO_MANIFEST_DIR").unwrap()
    )
});
/// Inner path for current directory
pub const INNER_PATH: &str = "src/test_with_golang/";
/// Path for group state summary, used in DKG scenario generator
pub const FRAMES_PATH: &str = "src/test_with_golang/frames.txt";

impl CLI {
    #[inline(always)]
    fn new(commands: Cmd) -> Self {
        Self {
            verbose: true,
            commands,
        }
    }

    pub fn keygen(config: KeyGenConfig) -> Self {
        Self::new(Cmd::GenerateKeypair(config))
    }

    pub fn start(config: Config) -> Self {
        Self::new(Cmd::Start(config))
    }

    pub fn load_id(control: &str, id: &str) -> Self {
        Self::new(Cmd::Load {
            control: control.to_string(),
            id: id.to_string(),
        })
    }

    pub fn dkg_join(control: &str, id: &str, groupfile_path: Option<&str>) -> Self {
        Self::new(Cmd::Dkg(crate::cli::Dkg::Join {
            control: control.to_string(),
            id: id.to_string(),
            group: groupfile_path.map(|p| p.to_string()),
        }))
    }

    pub fn dkg_accept(control: &str, id: &str) -> Self {
        Self::new(Cmd::Dkg(crate::cli::Dkg::Accept {
            control: control.to_string(),
            id: id.to_string(),
        }))
    }

    pub fn stop(control: &str, id: Option<&str>) -> Self {
        Self::new(Cmd::Stop {
            control: control.to_string(),
            id: id.map(|x| x.to_string()),
        })
    }
}

/// Drand implementation
#[derive(Debug)]
pub enum Lang {
    GO,
    RS,
}

#[derive(Debug)]
pub struct NodeConfig {
    /// Longterm node identifier
    cmd_i: usize,
    /// Base folder name: `node<cmd_i>_<lang>`
    folder_name: String,
    /// Base folder absolute path
    folder_path: String,
    /// Groupfile absolute path
    groupfile_path: String,
    /// Beacon ID
    pub id: String,
    scheme: String,
    /// Node private-listen address (in tests is same to URI, no TLS termination).
    private_listen: String,
    /// Node control port
    pub control: String,
    /// Golang or Rust implementation
    implementation: Lang,
    /// Statistical information about given node as participant
    summary: CompletedRoles,
}

#[derive(Debug, Default)]
struct CompletedRoles {
    joiner: u16,
    remainer: u16,
    leaver: u16,
}

impl std::fmt::Display for CompletedRoles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "J: {:<3}| R: {:<3}| L: {:<3}",
            self.joiner, self.remainer, self.leaver
        )
    }
}

pub struct Scenario {
    epoch: u16,
    pub thr: usize,
    pub joiners: Vec<usize>,
    pub remainers: Vec<usize>,
    pub leavers: Vec<usize>,
    frames: Option<File>,
    rng: ThreadRng,
}

impl std::fmt::Display for Scenario {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "\nEpoch: {}\nThr: {}\nJoiners: {:?}\nRemainers: {:?}\nLeavers: {:?}",
            self.epoch, self.thr, self.joiners, self.remainers, self.leavers
        )
    }
}

#[derive(Debug)]
enum Role {
    Joiner,
    Remainer,
    Leaver,
    Out,
}

impl Scenario {
    fn init(nodes: usize) -> Self {
        Self {
            epoch: 0,
            thr: 0,
            joiners: Vec::with_capacity(nodes),
            remainers: Vec::with_capacity(nodes),
            leavers: Vec::with_capacity(nodes),
            frames: None,
            rng: rand::rng(),
        }
    }

    /// Write summary to the file.
    pub fn enable_frames(&mut self) {
        self.frames = Some(File::create(FRAMES_PATH).unwrap())
    }

    /// Helper function for mapping between nodes indentifiers and their roles (as participants) for current epoch.
    fn get_role(&self, i: &usize) -> Role {
        match (
            self.joiners.contains(i),
            self.remainers.contains(i),
            self.leavers.contains(i),
        ) {
            (true, false, false) => Role::Joiner,
            (false, true, false) => Role::Remainer,
            (false, false, true) => Role::Leaver,
            (false, false, false) => Role::Out,
            _ => panic!("scenario: unknown role for {i}"),
        }
    }

    async fn write_frame(&mut self, nodes: &[NodeConfig], is_finished: bool) {
        if self.frames.is_none() {
            return;
        }
        // Step: scenario for each epoch has 2 steps:
        // `Starting` is the setup phase, before DKG execution.
        // `Finished` step means DKG is completed correctly and members groupfiles are verified

        // Metadata
        let step = if is_finished {
            "STARTING >> FINISHED"
        } else {
            "STARTING >"
        };
        let group_size = self.joiners.len() + self.remainers.len();
        let viz_group = "#".repeat(group_size);
        let viz_thr = "#".repeat(self.thr);
        let mut out = format!(
            "Scenario: {step}\nEpoch: {}\nThreshold: {:<2} {viz_thr}\nGroup len: {:<2} {viz_group}\n\n",
            self.epoch, self.thr, group_size
        );

        // Table headers
        writeln!(
            &mut out,
            "  Folder |    Address    |   Role   |  Completed roles",
        )
        .unwrap();

        // Table content
        for n in nodes {
            let folder = &n.folder_name;
            let role = format!("{:?}", self.get_role(&n.cmd_i));
            let address = &n.private_listen;
            let summary = &n.summary;
            writeln!(&mut out, "{folder:<10}{address:<17}{role:<11}{summary}").unwrap();
        }

        self.frames
            .as_ref()
            .unwrap()
            .write_all(out.as_bytes())
            .unwrap();
    }

    fn clear_roles(&mut self) {
        self.joiners.clear();
        self.remainers.clear();
        self.leavers.clear();
    }
}

pub struct NodesGroup {
    pub nodes: Vec<NodeConfig>,
    pub sn: Scenario,
    /// Groupfile is required for joiners in existing group, see [`NodeConfig::dkg_join`]
    pub group_file_path: String,
}

impl NodesGroup {
    pub async fn check_results(&mut self) {
        // Groupfiles of remainers and joiners should be same
        self.assert_groupfiles_with_leader();

        // All nodes should be in non-terminal state
        for n in self.nodes.iter_mut() {
            let status = get_current_status(&n.control, &n.id).await;
            if status.is_terminal() {
                panic!(
                    "terminal statuses are not expected, folder: {}, address: {}, status: {}",
                    n.folder_name, n.private_listen, status
                );
            };

            // Add current role to own node statistic
            match self.sn.get_role(&n.cmd_i) {
                Role::Joiner => {
                    n.summary.joiner += 1;
                }
                Role::Remainer => {
                    n.summary.remainer += 1;
                }
                Role::Leaver => {
                    n.summary.leaver += 1;
                    n.set_to_fresh().await;
                    sleep(Duration::from_secs(5)).await;
                }
                Role::Out => { /* not tracked */ }
            };
        }

        self.sn.write_frame(&self.nodes, true).await;
    }

    pub async fn generate_nodes(n: usize) -> Self {
        if n < 2 {
            panic!("at least 2 nodes required")
        }
        let half = n / 2;
        let nodes: Vec<NodeConfig> = (0..n)
            .map(|i| {
                if i < half {
                    NodeConfig::new(i, "AAA", DefaultScheme::ID, Lang::GO)
                } else {
                    NodeConfig::new(i, "AAA", DefaultScheme::ID, Lang::RS)
                }
            })
            .collect();

        for n in &nodes {
            n.generate_keypair().await
        }
        let sn = Scenario::init(n);
        let group_file_path = format!(
            "{}/multibeacon/AAA/groups/drand_group.toml",
            nodes[0].folder_path
        );

        Self {
            nodes,
            sn,
            group_file_path,
        }
    }

    pub async fn start_daemons(&self) {
        for n in &self.nodes {
            n.start().await
        }
    }

    pub async fn leader_dkg_execute(&mut self) {
        if self.sn.frames.is_some() {
            self.sn.write_frame(&self.nodes, false).await;
        }
        let args = format!("dkg execute --id AAA --control {}", self.nodes[0].control);
        run_cmd_golang(&args).await
    }

    pub async fn members_proceed_proposal(&self) {
        for n in self.nodes.iter().skip(1) {
            if self.sn.joiners.contains(&n.cmd_i) {
                if self.sn.epoch > 1 {
                    // Joiner in existing group
                    n.dkg_join(Some(&self.group_file_path)).await
                } else {
                    // Joiner in new group
                    n.dkg_join(None).await
                }
            } else if self.sn.remainers.contains(&n.cmd_i) {
                n.dkg_accept().await
            } else {
                // * Leaver get no say if the rest of the network wants them out
                // * Node is not included in current scenario
                continue;
            }
        }
    }

    /// Generates roles for participants based on the current group state.
    ///
    /// After first epoch, the leader (node[0]) is always `Remainer`, other nodes may leave, join, remain, or stay out of the group.
    pub fn generate_roles(&mut self) {
        self.sn.epoch += 1;

        // Initial DKG for all nodes with minimal threshold
        if self.sn.epoch == 1 {
            let nodes_len = self.nodes.len();
            self.sn.joiners.extend(0..nodes_len);
            self.sn.thr = minimum_t(nodes_len);
            return;
        }

        // Joiners are selected randomly from nodes that are not joiners or remainers at previous epoch.
        let new_joiners: Vec<usize> = self
            .nodes
            .iter()
            // Exclude prev joiners
            .filter(|n| {
                !self.sn.joiners.contains(&n.cmd_i) &&
            // Exclude prev remainers
                !self.sn.remainers.contains(&n.cmd_i) &&
            // Pick up node with ~50% probability
                self.sn.rng.random::<bool>()
            })
            .map(|n| n.cmd_i)
            .collect();

        let new_joiners_count = new_joiners.len();

        // Reconstruct group from previous epoch and shuffle nodes positions.
        let prev_group_size = self.sn.joiners.len() + self.sn.remainers.len();
        let mut prev_group = Vec::with_capacity(prev_group_size);
        prev_group.extend_from_slice(&self.sn.joiners);
        prev_group.extend_from_slice(&self.sn.remainers);
        prev_group.shuffle(&mut self.sn.rng);

        // Leader node has protected `Remainer` role and must be at the beginning of shuffled vector.
        let leader_position = prev_group.iter().position(|x| *x == 0).unwrap();
        prev_group.swap(0, leader_position);

        let prev_thr = self.sn.thr;

        // No leavers case
        if prev_group_size == 2 || prev_group_size == prev_thr {
            self.sn.clear_roles();
            self.sn.joiners.extend_from_slice(&new_joiners);
            self.sn.remainers.extend_from_slice(&prev_group);
            self.sn.thr = minimum_t(prev_group_size + new_joiners_count);
            return;
        }

        let new_remainers_count = self.sn.rng.random_range(prev_thr..=prev_group_size);
        let new_group_size = new_joiners_count + new_remainers_count;

        let new_thr = self
            .sn
            .rng
            .random_range(minimum_t(new_group_size)..=new_group_size);
        let (new_remainers, new_leavers) = prev_group.split_at(new_remainers_count);

        self.sn.clear_roles();
        self.sn.thr = new_thr;
        self.sn.joiners.extend_from_slice(&new_joiners);
        self.sn.remainers.extend_from_slice(new_remainers);
        self.sn.leavers.extend_from_slice(new_leavers);
    }

    pub async fn leader_generate_proposal(&mut self) {
        if self.sn.joiners.is_empty() && self.sn.remainers.is_empty() && self.sn.leavers.is_empty()
        {
            panic!("received empty scenario")
        }
        info!(
            "Leader: generating proposal for new scenario: {}\n",
            self.sn
        );

        let joiners = map_node_addresses(&self.nodes, &self.sn.joiners);
        let remainers = map_node_addresses(&self.nodes, &self.sn.remainers);
        let leavers = map_node_addresses(&self.nodes, &self.sn.leavers);

        let mut args = "dkg generate-proposal --id AAA ".to_string();
        let roles = [
            (joiners.as_slice(), "--joiner"),
            (remainers.as_slice(), "--remainer"),
            (leavers.as_slice(), "--leaver"),
        ];
        for (list, flag) in roles {
            for v in list {
                write!(args, " {flag} {v}").unwrap();
            }
        }
        write!(
            args,
            " --control {} --out {}/proposal.toml",
            self.nodes[0].control, self.nodes[0].folder_path
        )
        .unwrap();

        run_cmd_golang(&args).await;
        self.leader_initiate_proposal().await
    }

    pub async fn leader_initiate_proposal(&mut self) {
        let remainers_len = self.sn.remainers.len();
        let group_size = self.sn.joiners.len() + remainers_len;
        debug!("leader_initiate_proposal: group_size {group_size},thr: {}, joiners: {:?}, remainers: {:?}, leavers: {:?}",self.sn.thr, self.sn.joiners, self.sn.remainers, self.sn.leavers);
        let threshold = self.sn.thr;

        let args = if self.sn.remainers.is_empty() {
            format!("dkg init --id AAA --period 5s --timeout 50s --catchup-period 1s --genesis-delay 24h --threshold {threshold} --scheme pedersen-bls-chained --proposal {}/proposal.toml --control {}", self.nodes[0].folder_path, self.nodes[0].control)
        } else {
            format!("dkg reshare --id AAA --timeout 50s --catchup-period 1s --threshold {threshold} --proposal {}/proposal.toml --control {}", self.nodes[0].folder_path, self.nodes[0].control)
        };
        run_cmd_golang(&args).await
    }

    /// Directly specifies roles and threshold for next epoch.
    ///
    /// **Use with caution:**
    /// - This is manual alternative of random scenario in [`Self::generate_roles`]
    /// - Arguments must be consistent with Drand protocol and previous group state.
    pub fn setup_scenario(
        &mut self,
        joiners: &[usize],
        remainers: &[usize],
        leavers: &[usize],
        thr: usize,
    ) {
        self.sn.epoch += 1;
        self.sn.thr = thr;

        self.sn.clear_roles();
        self.sn.joiners.extend_from_slice(joiners);
        self.sn.remainers.extend_from_slice(remainers);
        self.sn.leavers.extend_from_slice(leavers);
    }

    /// Performs a byte-level comparison of the members groupfiles against the groupfile from the leader
    pub fn assert_groupfiles_with_leader(&self) {
        let leader_groupfile = std::fs::read(&self.group_file_path).unwrap();
        // Joiners
        for j in &self.sn.joiners {
            assert_eq!(
                leader_groupfile,
                std::fs::read(&self.nodes[*j].groupfile_path).unwrap()
            )
        }
        // Remainers
        for r in &self.sn.remainers {
            assert_eq!(
                leader_groupfile,
                std::fs::read(&self.nodes[*r].groupfile_path).unwrap()
            )
        }
        info!(
            "test[assert_groupfiles]: joiners {:?} and remainers {:?} are in right state",
            self.sn.joiners, &self.sn.remainers
        )
    }
}

fn map_node_addresses<'a>(nodes: &'a [NodeConfig], identifiers: &[usize]) -> Vec<&'a str> {
    nodes
        .iter()
        .filter(|n| identifiers.contains(&n.cmd_i))
        .map(|n| n.private_listen.as_str())
        .collect()
}

/// Helper to generate n nodes and run a fresh DKG
///
/// First half of nodes use Golang implementation, leader is always node[0], see [`NodesGroup::generate_nodes`].
/// Threshold is minimal by default; this can be changed if a custom threshold is specified.
/// Note: Custom threshold should follow the Drand protocol.
pub async fn run_fresh_dkg(n: usize, custom_thr: Option<usize>) -> NodesGroup {
    let mut nodes = NodesGroup::generate_nodes(n).await;
    nodes.start_daemons().await;
    sleep(Duration::from_secs(5)).await;
    // in fresh DKG all nodes are joiners.
    nodes.sn.joiners.extend(0..n);

    nodes.sn.thr = custom_thr.unwrap_or_else(|| minimum_t(n));
    nodes.leader_generate_proposal().await;
    sleep(Duration::from_secs(2)).await;
    nodes.members_proceed_proposal().await;
    nodes.leader_dkg_execute().await;
    sleep(Duration::from_secs(10)).await;
    nodes.assert_groupfiles_with_leader();
    nodes.check_results().await;
    nodes
}

impl NodeConfig {
    fn new(cmd_i: usize, id: &str, scheme: &str, lang: Lang) -> Self {
        let private_listen = format!("127.0.0.1:{}", 44000 + cmd_i);
        let control = format!("{}", 55000 + cmd_i);
        let folder_name = format!("node{cmd_i}_{lang:?}");
        let folder_path = format!(
            "{}/{INNER_PATH}{folder_name}",
            env::var("CARGO_MANIFEST_DIR").unwrap()
        );
        let groupfile_path = format!("{folder_path}/multibeacon/{id}/groups/drand_group.toml");

        Self {
            cmd_i,
            folder_name,
            folder_path,
            groupfile_path,
            id: id.into(),
            scheme: scheme.into(),
            private_listen,
            control,
            implementation: lang,
            summary: CompletedRoles::default(),
        }
    }

    /// Removes all distributed materilas and restarts beacon ID at Fresh state.
    pub async fn set_to_fresh(&self) {
        // remove beacon ID folder from node base folder
        std::fs::remove_dir_all(format!("{}/multibeacon/{}", self.folder_path, self.id)).unwrap();
        match self.implementation {
            Lang::GO => {
                let mut cmd = async_std::process::Command::new("/bin/bash");
                cmd.arg("-c")
                    .arg(format!(
                        "{} stop --control {}",
                        DRAND_BIN_PATH.as_str(),
                        self.control
                    ))
                    .spawn()
                    .unwrap();
                // Note: in golang implementation dkg.db is shared across beacon IDs,
                //       if more than one ID required in new test cases then invoke CLI
                //       `dkg nuke --id <beacon_id> --control <port>` instead of file removal.
                std::fs::remove_file(format!("{}/dkg.db", self.folder_path)).unwrap();
            }
            Lang::RS => self.stop().await,
        }
        self.generate_keypair().await;
        self.start().await;
        sleep(Duration::from_secs(2)).await
    }

    async fn generate_keypair(&self) {
        match self.implementation {
            Lang::RS => {
                let config = KeyGenConfig {
                    folder: self.folder_path.clone(),
                    control: self.control.clone(),
                    id: self.id.clone(),
                    scheme: self.scheme.clone(),
                    address: self.private_listen.clone(),
                };
                CLI::keygen(config).run().await.unwrap();
            }
            Lang::GO => {
                let args = format!(
                    "generate-keypair --folder {} --control {} --id {} --scheme {} {} > /dev/null 2>&1",
                    self.folder_path, self.control, self.id, self.scheme, self.private_listen
                );
                run_cmd_golang(&args).await
            }
        }
    }

    async fn start(&self) {
        match self.implementation {
            Lang::GO => {
                let args = format!(
                    "{} start --folder {} --private-listen {} --verbose --control {} >> {}/node{}.log 2>&1",
                    DRAND_BIN_PATH.as_str(),
                    self.folder_path,
                    self.private_listen,
                    self.control,
                    self.folder_path,
                    self.cmd_i
                );
                let mut cmd = async_std::process::Command::new("/bin/bash");
                cmd.arg("-c").arg(args).spawn().unwrap();
            }
            Lang::RS => {
                let config = Config {
                    folder: self.folder_path.clone(),
                    control: self.control.clone(),
                    private_listen: self.private_listen.clone(),
                    id: Some(self.id.clone()),
                };
                tokio::task::spawn(async move { CLI::start(config).run().await.unwrap() });
            }
        }
    }

    pub async fn dkg_join(&self, groupfile: Option<&str>) {
        match self.implementation {
            Lang::RS => CLI::dkg_join(&self.control.to_string(), &self.id, groupfile)
                .run()
                .await
                .unwrap(),
            Lang::GO => {
                let args = if let Some(path) = groupfile {
                    format!(
                        "dkg join --id {} --group {path} --control {}",
                        self.id, self.control
                    )
                } else {
                    format!("dkg join --id {} --control {}", self.id, self.control)
                };

                run_cmd_golang(&args).await
            }
        }
    }

    pub async fn dkg_accept(&self) {
        match self.implementation {
            Lang::RS => CLI::dkg_accept(&self.control.to_string(), &self.id)
                .run()
                .await
                .unwrap(),
            Lang::GO => {
                let args = format!(
                    "dkg accept --id {} --control {} > {}/node{}_accept.log 2>&1",
                    self.id, self.control, self.folder_path, self.cmd_i
                );
                run_cmd_golang(&args).await
            }
        }
    }

    pub async fn stop(&self) {
        match self.implementation {
            Lang::RS => CLI::stop(&self.control.to_string(), None)
                .run()
                .await
                .unwrap(),
            Lang::GO => {
                let mut cmd = async_std::process::Command::new("/bin/bash");
                cmd.arg("-c")
                    .arg(format!(
                        "{} stop --control {} > /dev/null 2>&1",
                        DRAND_BIN_PATH.as_str(),
                        self.control
                    ))
                    .spawn()
                    .unwrap();
            }
        }
    }
}

async fn run_cmd_golang(args: &str) {
    let mut cmd = async_std::process::Command::new("/bin/bash");
    cmd.arg("-c")
        .arg(format!("{} {args}", DRAND_BIN_PATH.as_str()));

    let output = match cmd.spawn() {
        Ok(mut cmd) => cmd.status().await.unwrap(),
        Err(err) => panic!("failed to spawn cmd: {args}, error: {err}"),
    };

    if !output.success() {
        panic!("failed to execute: {args}, code: {:?}", output.code());
    }
}

pub fn remove_nodes_fs() {
    let path = PathBuf::from(format!(
        "{}/{INNER_PATH}",
        env::var("CARGO_MANIFEST_DIR").unwrap()
    ));

    let entries = std::fs::read_dir(path).unwrap();

    for entry in entries {
        let entry = entry.unwrap();
        let path = entry.path();

        if path.is_dir()
            && path
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("node")
        {
            std::fs::remove_dir_all(&path).unwrap();
        }
    }
}

async fn get_current_status(control_port: &str, id: &str) -> Status {
    let mut client = DkgControlClient::new(control_port).await.unwrap();
    let response = client.dkg_status(id).await.unwrap();

    let current = response.current.unwrap().state;
    Status::try_from(current).unwrap()
}

pub async fn get_finished_state(control_port: &str, id: &str) -> DkgEntry {
    let mut client = DkgControlClient::new(control_port).await.unwrap();
    let response = client.dkg_status(id).await.unwrap();
    response.complete.unwrap()
}
