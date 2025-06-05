use super::group::Group;
use super::keys::Pair;
use super::toml::PairToml;
use super::toml::Toml;
use super::Scheme;

use energon::kyber::dkg::DistKeyShare;
use std::fs::File;
use std::fs::Permissions;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::path::PathBuf;
use tracing::info;

// Filesystem constants
const DEFAULT_DIR: &str = ".drand";
const MULTIBEACON_DIR: &str = "multibeacon";
const KEY_DIR: &str = "key";
const GROUP_DIR: &str = "groups";
const DB_DIR: &str = "db";
const PRIVATE_ID_FILE: &str = "drand_id.private";
const PUBLIC_ID_FILE: &str = "drand_id.public";
const PRIVATE_SHARE_FILE: &str = "dist_key.private";
const GROUP_FILE: &str = "drand_group.toml";

/// Directories permission
const DIR_PERM: u32 = 0o740;
/// Private id permission
const PRIVATE_PERM: u32 = 0o600;
/// Public id permission
const PUBLIC_PERM: u32 = 0o664;

#[derive(thiserror::Error, Debug)]
#[error("file_store: {0}")]
pub enum FileStoreError {
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error("file already exists in {0}")]
    FileAlreadyExists(PathBuf),
    #[error("file is not found at {0}")]
    FileNotFound(PathBuf),
    // Normally should not be possible
    #[error("toml error")]
    TomlError,
    #[error("schemes in public and private parts must be equal and non-empty")]
    InvalidPairSchemes,
    #[error("beacon id is not found in filestore")]
    BeaconNotFound,
    #[error("beacon id is failed to init, unknown scheme")]
    FailedInitID,
    #[error("chain_store error: {0}")]
    ChainStore(String),
    #[error("dkg_store error: {0}")]
    DkgStore(#[from] crate::dkg::store::DkgStoreError),
}

/// FileStore holds absolute path of **beacon_id** and abstracts the
/// loading and saving private/public cryptographic materials.
pub struct FileStore {
    pub beacon_path: PathBuf,
}

impl FileStore {
    /// Creates filesystem for given **beacon_id** and validates storage structure.
    pub fn new_checked(base_path: &str, beacon_id: &str) -> Result<Self, FileStoreError> {
        let base_path = absolute_path(base_path)?;

        let multibeacon_path = base_path.join(MULTIBEACON_DIR);
        if !base_path.try_exists()? {
            new_secure_dir(&base_path)?;
            new_secure_dir(&multibeacon_path)?;
        } else if !multibeacon_path.try_exists()? {
            return Err(FileStoreError::FileNotFound(multibeacon_path));
        }
        let beacon_path = multibeacon_path.join(beacon_id);

        if beacon_path.try_exists()? {
            return Err(FileStoreError::FileAlreadyExists(beacon_path));
        } else {
            new_secure_dir(&beacon_path)?;
        }

        // Create beacon sub folders
        new_secure_dir(&beacon_path.join(KEY_DIR))?;
        new_secure_dir(&beacon_path.join(GROUP_DIR))?;
        new_secure_dir(&beacon_path.join(DB_DIR))?;

        Ok(Self { beacon_path })
    }

    /// A check for minimal valid filestore structure.
    pub fn validate(&self) -> Result<(), FileStoreError> {
        if !self.beacon_path.try_exists()? {
            return Err(FileStoreError::FileNotFound(self.beacon_path.to_owned()));
        }

        let private = self.private_id_file();
        if !private.try_exists()? {
            return Err(FileStoreError::FileNotFound(private));
        }

        let public_id = self.public_id_file();
        if !public_id.try_exists()? {
            return Err(FileStoreError::FileNotFound(public_id));
        }

        Ok(())
    }

    /// Returns an absolute path to multibeacon folder and non-empty list of pre-validated filestores
    pub fn read_multibeacon_folder(folder: &str) -> Result<(PathBuf, Vec<Self>), FileStoreError> {
        // Check if 'multibeacon' exists
        let base = absolute_path(folder)?;
        let multibeacon = base.join(MULTIBEACON_DIR);
        if !multibeacon.try_exists()? {
            return Err(FileStoreError::FileNotFound(multibeacon));
        }
        // Attempt to read and validate stores
        let mut stores = vec![];
        let entries = std::fs::read_dir(&multibeacon)?;

        for entry in entries.flatten() {
            if let Some(beacon_id) = entry.file_name().to_str() {
                let store = Self {
                    beacon_path: multibeacon.join(beacon_id),
                };
                store.validate()?;
                stores.push(store);
            }
        }
        if stores.is_empty() {
            return Err(FileStoreError::BeaconNotFound);
        }
        info!(
            "Detected stores, folder: {multibeacon:?}, amount: {}",
            stores.len()
        );

        Ok((multibeacon, stores))
    }

    pub fn save_key_pair<S: Scheme>(&self, pair: &Pair<S>) -> Result<(), FileStoreError> {
        let pair_toml = pair.toml_encode().ok_or(FileStoreError::TomlError)?;

        // save private
        let mut f = File::create(self.private_id_file())?;
        f.set_permissions(Permissions::from_mode(PRIVATE_PERM))?;
        f.write_all(pair_toml.private().as_bytes())?;

        // save public
        let mut f = File::create(self.public_id_file())?;
        f.set_permissions(Permissions::from_mode(PUBLIC_PERM))?;
        f.write_all(pair_toml.public().as_bytes())?;

        println!(
            "Generated keys at: {}\n{}\n",
            self.beacon_path.join(KEY_DIR).display(),
            pair_toml.public()
        );

        Ok(())
    }

    pub fn save_group<S: Scheme>(&self, group: &Group<S>) -> Result<(), FileStoreError> {
        let group_toml = group.toml_encode().ok_or(FileStoreError::TomlError)?;
        let mut f = File::create(self.group_file())?;
        f.set_permissions(Permissions::from_mode(PUBLIC_PERM))?;
        f.write_all(group_toml.to_string().as_bytes())?;

        Ok(())
    }

    /// Returns non-generic group representation
    pub fn load_group(&self) -> Result<crate::transport::drand::GroupPacket, FileStoreError> {
        let group_toml = std::fs::read_to_string(self.group_file())?;
        let group = crate::transport::drand::GroupPacket::from_toml(&group_toml)
            .ok_or(FileStoreError::TomlError)?;

        Ok(group)
    }

    pub fn save_share<S: Scheme>(&self, share: &DistKeyShare<S>) -> Result<(), FileStoreError> {
        let share_toml = share.toml_encode().ok_or(FileStoreError::TomlError)?;
        let mut f = File::create(self.private_share_file())?;
        f.set_permissions(Permissions::from_mode(PRIVATE_PERM))?;
        f.write_all(share_toml.to_string().as_bytes())?;

        Ok(())
    }

    /// Returns [`PairToml`] to handle a case where generic type is not initialized yet.
    pub fn load_key_pair_toml(&self) -> Result<PairToml, FileStoreError> {
        let private_str = std::fs::read_to_string(self.private_id_file())?;
        let public_str = std::fs::read_to_string(self.public_id_file())?;
        let pair_toml = PairToml::parse(private_str.as_str(), public_str.as_str())
            .ok_or(FileStoreError::TomlError)?;

        Ok(pair_toml)
    }

    pub fn load_share<S: Scheme>(&self) -> Result<DistKeyShare<S>, FileStoreError> {
        let share_str = std::fs::read_to_string(self.private_share_file())?;
        Toml::toml_decode(&share_str.parse().map_err(|_| FileStoreError::TomlError)?)
            .ok_or(FileStoreError::TomlError)
    }

    pub fn drand_home() -> String {
        match home::home_dir() {
            Some(path) => path.join(DEFAULT_DIR).display().to_string(),
            None => {
                panic!("Couldn't get home directory")
            }
        }
    }

    pub fn is_fresh_run(&self) -> Result<bool, FileStoreError> {
        match (
            self.group_file().exists(),
            self.private_share_file().exists(),
        ) {
            (true, true) => Ok(false),
            (false, false) => Ok(true),
            (true, false) => Err(FileStoreError::FileNotFound(self.private_share_file())),
            (false, true) => Err(FileStoreError::FileNotFound(self.group_file())),
        }
    }

    pub fn get_beacon_id(&self) -> Option<&str> {
        Path::new(&self.beacon_path).file_name()?.to_str()
    }

    fn private_id_file(&self) -> PathBuf {
        self.beacon_path.join(KEY_DIR).join(PRIVATE_ID_FILE)
    }

    fn public_id_file(&self) -> PathBuf {
        self.beacon_path.join(KEY_DIR).join(PUBLIC_ID_FILE)
    }

    pub fn group_file(&self) -> PathBuf {
        self.beacon_path.join(GROUP_DIR).join(GROUP_FILE)
    }

    pub fn private_share_file(&self) -> PathBuf {
        self.beacon_path.join(GROUP_DIR).join(PRIVATE_SHARE_FILE)
    }

    pub fn db_path(&self) -> PathBuf {
        self.beacon_path.join(DB_DIR)
    }
}

#[inline]
fn absolute_path(base_path: &str) -> Result<PathBuf, FileStoreError> {
    let absolute = match Path::new(base_path).is_absolute() {
        true => PathBuf::from(base_path),
        false => std::fs::canonicalize(base_path)?,
    };

    Ok(absolute)
}

fn new_secure_dir(folder: &PathBuf) -> Result<(), FileStoreError> {
    std::fs::create_dir(folder)?;
    std::fs::set_permissions(folder, Permissions::from_mode(DIR_PERM))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::utils::Address;
    use energon::drand::schemes::DefaultScheme;

    // #Required permissions
    //
    // /tmp/../testnet                      740
    //  └── multibeacon                     740
    //        └── default                   740
    //            ├── groups                740
    //            │   ├── dist_key.private  600
    //            │   └── drand_group.toml  664
    //            └── key                   740
    //                ├── drand_id.private  600
    //                └── drand_id.public   664

    #[test]
    fn check_permissions_and_data() {
        // Build absolute path for base folder 'testnet'
        let temp_dir = tempfile::TempDir::new().unwrap();
        let base_path = temp_dir
            .path()
            .join("testnet")
            .as_path()
            .to_str()
            .unwrap()
            .to_string();

        // Create new store, save share and pair
        let store = FileStore::new_checked(base_path.as_str(), "some_id").unwrap();
        let address = Address::default();
        let pair: Pair<DefaultScheme> = Pair::generate(address).unwrap();
        let share = DistKeyShare::<DefaultScheme>::default();
        store.save_key_pair(&pair).unwrap();
        store.save_share(&share).unwrap();

        // Assert permissions
        fn assert_perm(path: PathBuf, mode: u32) {
            assert!(std::fs::metadata(path).unwrap().permissions().mode() & 0o777 == mode)
        }
        assert_perm(store.private_id_file(), PRIVATE_PERM);
        assert_perm(store.public_id_file(), PUBLIC_PERM);
        assert_perm(store.private_share_file(), PRIVATE_PERM);
        assert_perm(base_path.as_str().into(), DIR_PERM);

        // Load back the share and pair
        let loaded_share: DistKeyShare<DefaultScheme> = store.load_share().unwrap();
        let pair_toml = store.load_key_pair_toml().unwrap();
        let loaded_pair: Pair<DefaultScheme> = Toml::toml_decode(&pair_toml).unwrap();

        assert!(pair == loaded_pair);
        assert!(share == loaded_share);
    }
}
