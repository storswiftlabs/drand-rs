//! DRAFT!

use crate::key::store::FileStore;
use crate::key::store::FileStoreError;
use crate::protobuf::drand::ChainInfoPacket;
use crate::protobuf::drand::Metadata;
use crate::transport::drand::GroupPacket;
use std::io::ErrorKind;

pub struct ChainHandler {
    group: Box<GroupPacket>,
}

impl ChainHandler {
    /// Attempt to initialize ChainHandler.
    #[allow(clippy::field_reassign_with_default)]
    pub fn try_init(fs: &FileStore, beacon_id: &str) -> Result<Self, FileStoreError> {
        let handler = match fs.load_group() {
            // to unblock early tests
            Ok(group) => Self {
                group: group.into(),
            },
            Err(err) => match err {
                FileStoreError::IO(error) => {
                    // NotFound error is expected for groupfile loading and means that node is fresh.
                    if error.kind() == ErrorKind::NotFound {
                        // TODO: align this once DKG is ready
                        let mut group = GroupPacket::default();
                        group.metadata = Metadata::mimic_version(beacon_id, &[]);
                        Self {
                            group: group.into(),
                        }
                    } else {
                        return Err(FileStoreError::IO(error));
                    }
                }
                _ => return Err(err),
            },
        };

        Ok(handler)
    }

    pub fn chain_info(&self) -> ChainInfoPacket {
        self.group.get_chain_info()
    }
}
