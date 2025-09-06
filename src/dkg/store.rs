use super::state::State;
use super::status::Status;
use crate::key::toml::Toml;
use crate::key::Scheme;

use std::fs::File;
use std::fs::Permissions;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::path::PathBuf;

use tracing::error;

/// Directory located at `base_folder/multibeacon/beacon_id/`.
const DKG_STORE_DIR: &str = "dkg";
/// TOML encoded representation of the current [`State`].
const CURRENT_FILE: &str = "current.toml";
/// TOML encoded representation of the finished [`State`].
const FINISHED_FILE: &str = "finished.toml";

/// Permissions
const DIR_PERM: u32 = 0o755;
const FILE_PERM: u32 = 0o660;

/// Store for current and finished DKGs, contains absolute path to [`DKG_STORE_DIR`]
pub struct DkgStore {
    path: PathBuf,
}

impl DkgStore {
    pub fn init<S: Scheme>(
        path_to_id: &Path,
        fresh_run: bool,
        id: &str,
    ) -> Result<Self, DkgStoreError> {
        let store = Self {
            path: path_to_id.join(DKG_STORE_DIR),
        };

        match (fresh_run, store.path.exists()) {
            // Fresh run case
            (true, false) => {
                std::fs::create_dir(&store.path).map_err(DkgStoreError::CreateDir)?;
                std::fs::set_permissions(&store.path, Permissions::from_mode(DIR_PERM))
                    .map_err(DkgStoreError::Permission)?;
            }
            // Brocken configuration
            (false, false) => {
                error!("{} at {}", DkgStoreError::NotFound, store.path.display());
                return Err(DkgStoreError::FailedToLoad);
            }
            _ => (),
        }

        // Check timeout in case node is within dkg
        match store.get_current::<S>() {
            Ok(mut state) => {
                if matches!(
                    *state.status(),
                    Status::Proposed | Status::Proposing | Status::Accepted | Status::Joined
                ) && state.time_expired()
                {
                    state.status = Status::TimedOut;
                    store.save_current(&state)?;
                } else if state.status() == &Status::Executing {
                    // Node can not be loaded into executing state regardless of timeout(drand-go v2.1.0).
                    state.status = Status::Failed;
                    store.save_current(&state)?;
                }
            }
            Err(DkgStoreError::NotFound) => store.save_current(&State::<S>::fresh(id))?,
            Err(err) => return Err(err),
        }

        Ok(store)
    }

    /// Retrieves the last successful state.
    ///
    /// If the current state is terminal, it will attempt to retrieve the finished state.
    /// If the finished state is not found, it returns a fresh state for the provided beacon ID.
    pub(super) fn get_last_succesful<S: Scheme>(
        &self,
        beacon_id: &str,
    ) -> Result<State<S>, DkgStoreError> {
        let current = self.get::<S>(CURRENT_FILE)?;

        if current.status.is_terminal() {
            match self.get_finished() {
                Ok(finished) => Ok(finished),
                Err(DkgStoreError::NotFound) => Ok(State::fresh(beacon_id)),
                Err(err) => Err(err),
            }
        } else {
            Ok(current)
        }
    }

    pub(super) fn get_current<S: Scheme>(&self) -> Result<State<S>, DkgStoreError> {
        self.get(CURRENT_FILE)
    }

    pub(super) fn get_finished<S: Scheme>(&self) -> Result<State<S>, DkgStoreError> {
        self.get(FINISHED_FILE)
    }

    pub(super) fn save_current<S: Scheme>(&self, state: &State<S>) -> Result<(), DkgStoreError> {
        let toml = state
            .toml_encode()
            .ok_or(DkgStoreError::TomlError)?
            .to_string();

        self.save(CURRENT_FILE, &toml)?;

        Ok(())
    }

    #[allow(dead_code)]
    /// Finished state is a new state for current and finished.
    pub(super) fn save_finished<S: Scheme>(&self, state: &State<S>) -> Result<(), DkgStoreError> {
        let toml = state
            .toml_encode()
            .ok_or(DkgStoreError::TomlError)?
            .to_string();

        self.save(FINISHED_FILE, &toml)?;
        self.save(CURRENT_FILE, &toml)?;

        Ok(())
    }

    fn get<S: Scheme>(&self, kind: &str) -> Result<State<S>, DkgStoreError> {
        let path = self.path.join(kind);
        if !path.exists() {
            return Err(DkgStoreError::NotFound);
        }
        let file_str = std::fs::read_to_string(path).map_err(DkgStoreError::Read)?;
        let state = State::toml_decode(
            &file_str
                .parse()
                .map_err(|_| DkgStoreError::ParseStringError)?,
        )
        .ok_or(DkgStoreError::TomlError)?;

        Ok(state)
    }

    fn save(&self, kind: &str, toml: &str) -> Result<(), DkgStoreError> {
        if !self.path.exists() {
            return Err(DkgStoreError::NotFound);
        }
        let path_to_file = self.path.join(kind);
        let is_new_file = !path_to_file.exists();

        let mut f = File::create(path_to_file).map_err(DkgStoreError::CreateFile)?;
        if is_new_file {
            f.set_permissions(Permissions::from_mode(FILE_PERM))
                .map_err(DkgStoreError::Permission)?;
        }
        f.write_all(toml.as_bytes()).map_err(DkgStoreError::Write)?;

        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum DkgStoreError {
    #[error("failed to load a dkg history")]
    FailedToLoad,
    #[error("failed to create directory: {0}")]
    CreateDir(std::io::Error),
    #[error("failed to write into a file: {0}")]
    Write(std::io::Error),
    #[error("failed to create file: {0}")]
    CreateFile(std::io::Error),
    #[error("invalid permission: {0}")]
    Permission(std::io::Error),
    #[error("dkg folder is not found")]
    NotFound,
    #[error("failed to read file: {0}")]
    Read(std::io::Error),
    #[error("parse string error")]
    ParseStringError,
    #[error("toml error")]
    TomlError,
}

impl PartialEq for DkgStoreError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            // The only required check
            (Self::NotFound, Self::NotFound) => true,
            _ => false,
        }
    }
}
