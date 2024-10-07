use super::group::Group;
use super::keys::DistKeyShare;
use super::keys::Pair;
use super::toml::FromToml;
use super::toml::IntoToml;
use super::toml::ScalarSerialized;

use energon::drand::Scheme;

use anyhow::bail;
use anyhow::Result;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

pub const MULTIBEACON_FOLDER: &str = "multibeacon";
const DEFAULT_FOLDER: &str = ".drand";
const KEY_FOLDER: &str = "key";
const GROUP_FOLDER: &str = "groups";
const PRIVATE_ID_FILE: &str = "drand_id.private";
const PUBLIC_ID_FILE: &str = "drand_id.public";
const PRIVATE_DIST_KEY_FILE: &str = "dist_key.private";
const GROUP_FILE: &str = "drand_group.toml";

#[derive(Debug, Clone)]
pub struct FileStore {
    inner: Arc<InnerFS>,
}

#[derive(Debug)]
pub struct InnerFS {
    _base_folder: PathBuf,
    key_folder: PathBuf,
    private_key_file: PathBuf,
    public_key_file: PathBuf,
    group_file: PathBuf,
    private_dist_key_file: PathBuf,
}

impl FileStore {
    pub fn create_new(base_folder: PathBuf) -> Result<Self> {
        let key_folder = base_folder.join(KEY_FOLDER);
        std::fs::create_dir_all(&key_folder)?;

        let private_key_file = key_folder.join(PRIVATE_ID_FILE);
        let public_key_file = key_folder.join(PUBLIC_ID_FILE);
        let group_folder = base_folder.join(GROUP_FOLDER);
        std::fs::create_dir_all(&group_folder)?;

        let group_file = group_folder.join(GROUP_FILE);
        let private_dist_key_file = group_folder.join(PRIVATE_DIST_KEY_FILE);

        Ok(FileStore {
            inner: Arc::new(InnerFS {
                key_folder,
                private_key_file,
                public_key_file,
                _base_folder: base_folder,
                group_file,
                private_dist_key_file,
            }),
        })
    }

    pub fn set(base_folder: &str, beacon_id: &str) -> Self {
        let base_folder = PathBuf::from(base_folder)
            .join(MULTIBEACON_FOLDER)
            .join(beacon_id);
        let key_folder = base_folder.join(KEY_FOLDER);
        let private_key_file = key_folder.join(PRIVATE_ID_FILE);
        let public_key_file = key_folder.join(PUBLIC_ID_FILE);

        let group_folder = base_folder.join(GROUP_FOLDER);
        let group_file = group_folder.join(GROUP_FILE);
        let private_dist_key_file = group_folder.join(PRIVATE_DIST_KEY_FILE);
        Self {
            inner: Arc::new(InnerFS {
                key_folder,
                private_key_file,
                public_key_file,
                _base_folder: base_folder,
                group_file,
                private_dist_key_file,
            }),
        }
    }

    pub fn save_pair<S: Scheme>(&self, pair: &Pair<S>) -> Result<()> {
        std::fs::write(&self.private_key_file, pair.private().to_toml()?)?;
        std::fs::write(&self.public_key_file, pair.public().to_toml()?)?;
        println!(
            "Generated keys at: {}\n{}\n",
            self.key_folder.display(),
            pair.public()
        );
        Ok(())
    }

    pub fn save_distributed<S: Scheme>(
        &self,
        group: &Group<S>,
        share: &DistKeyShare<S>,
    ) -> Result<&PathBuf> {
        std::fs::write(&self.group_file, group.to_toml()?)?;
        std::fs::write(&self.private_dist_key_file, share.to_toml()?)?;

        Ok(&self.private_dist_key_file)
    }

    pub fn load_pair_raw(&self) -> Result<super::common::Pair> {
        let private_str = read_to_string(&self.private_key_file)?;
        let public_str = read_to_string(&self.public_key_file)?;

        let private = ScalarSerialized::from_toml(&private_str)?;
        let public = super::common::Identity::from_toml(&public_str)?;
        Ok(super::common::Pair::init(private, public))
    }

    pub fn load_share<S: Scheme>(&self) -> Result<DistKeyShare<S>> {
        let share_str = read_to_string(&self.private_dist_key_file)?;
        let share = DistKeyShare::from_toml(&share_str)?;
        Ok(share)
    }

    pub fn show_dist_key_path(&self) -> &str {
        self.private_dist_key_file.to_str().unwrap()
    }

    pub fn verify_path(path: &str, id: &str) -> Result<PathBuf> {
        let mut path = Path::new(path).to_owned();
        // maybe too strict
        if path.is_relative() {
            bail!("expected absolute folder path, received: {path:?}",)
        }
        path = path.join(MULTIBEACON_FOLDER).join(id);
        if path.exists() {
            bail!("Keypair already present in {path:?}\nRemove them before generating new one",)
        }

        Ok(path)
    }

    pub fn drand_home() -> String {
        match home::home_dir() {
            Some(path) => path.join(DEFAULT_FOLDER).display().to_string(),
            None => {
                panic!("Couldn't get home directory")
            }
        }
    }
}

fn read_to_string(p: &Path) -> Result<String> {
    std::fs::read_to_string(p)
        .map_err(|e| anyhow::anyhow!("Error reading: {}, :{e:?}", p.display()))
}

impl std::ops::Deref for FileStore {
    type Target = InnerFS;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
