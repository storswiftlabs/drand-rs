// Copyright (C) 2023-2024 StorSwift Inc.
// This file is part of the Drand-RS library.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::beacon::BeaconCmd;
use super::beacon::BeaconID;
use super::dkg::DkgCmd;
use crate::key::store::FileStore;
use crate::key::store::MULTIBEACON_FOLDER;
use crate::net::pool::PoolCmd;
use crate::net::transport::SetupInfo;

use anyhow::bail;
use anyhow::Context;
use anyhow::Result;

use arc_swap::ArcSwap;
use arc_swap::ArcSwapAny;
use arc_swap::Guard;

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;

pub struct MultiBeacon(ArcSwapAny<Arc<Vec<BeaconHandler>>>);
pub type Snapshot = Guard<Arc<Vec<BeaconHandler>>>;

impl MultiBeacon {
    pub fn new(
        beacon_id: Option<&String>,
        base_folder: &str,
        private_listen: &str,
        pool: mpsc::Sender<PoolCmd>,
    ) -> Result<Self> {
        // Load single id
        if let Some(id) = beacon_id {
            let h = BeaconHandler::new(id, base_folder, private_listen, pool.clone())?;
            return Ok(Self(ArcSwap::from(Arc::new(vec![h]))));
        };

        // Attempt to load ids from multibeacon folder
        let mut store = vec![];
        let path = PathBuf::from(base_folder).join(MULTIBEACON_FOLDER);

        match fs::read_dir(&path) {
            Ok(entries) => {
                for entry in entries {
                    if let Some(filename) = entry?.file_name().to_str() {
                        let h = BeaconHandler::new(
                            filename,
                            base_folder,
                            private_listen,
                            pool.clone(),
                        )?;
                        store.push(h)
                    }
                }
            }
            Err(e) => bail!("Failed to read {}: {e:?}", path.display()),
        }

        Ok(Self(ArcSwap::from(Arc::new(store))))
    }

    pub fn shapshot(&self) -> Snapshot {
        self.0.load()
    }

    /// Replaces the value inside this instance
    pub fn replace_store(&self, val: Arc<Vec<BeaconHandler>>) {
        self.0.store(val)
    }

    pub async fn cmd(&self, cmd: BeaconCmd, id: &str) -> anyhow::Result<()> {
        if let Some(beacon) = self.0.load().iter().find(|h| h.beacon_id.is_eq(id)) {
            beacon
                .tx
                .send(cmd)
                .await
                // should not be possible
                .with_context(|| format!("beacon [{id}] receiver dropped"))?;
            return Ok(());
        }
        bail!("beacon [{id}] is not running")
    }

    pub async fn init_dkg(
        &self,
        callback: oneshot::Sender<Result<String>>,
        setup_info: SetupInfo,
        id: BeaconID,
    ) -> Result<()> {
        if let Some(beacon) = self.0.load().iter().find(|h| h.beacon_id == id) {
            beacon
                .tx
                .send(BeaconCmd::Dkg(DkgCmd::Init(
                    callback,
                    setup_info,
                    beacon.tx.clone(),
                )))
                .await?
        } else {
            bail!("MultiBeacon::cmd: beacon [{id}] not found")
        }

        Ok(())
    }

    pub fn load_id(
        &self,
        beacon_id: &str,
        base_folder: &str,
        private_listen: &str,
        pool: mpsc::Sender<PoolCmd>,
    ) -> Result<()> {
        if self
            .0
            .load()
            .iter()
            .any(|h| h.beacon_id.as_str() == beacon_id)
        {
            bail!("MultiBeacon::load_id: beacon is already loaded: {beacon_id}")
        }
        let h = BeaconHandler::new(beacon_id, base_folder, private_listen, pool)?;
        let mut new_store = self
            .0
            .load()
            .iter()
            .cloned()
            .collect::<Vec<BeaconHandler>>();

        new_store.push(h);
        self.0.store(Arc::new(new_store));

        Ok(())
    }
}

#[derive(Clone)]
pub struct BeaconHandler {
    beacon_id: BeaconID,
    tx: Sender<BeaconCmd>,
}

impl BeaconHandler {
    pub fn new(
        beacon_id: &str,
        base_folder: &str,
        private_listen: &str,
        pool: mpsc::Sender<PoolCmd>,
    ) -> Result<Self> {
        let (tx, rx) = mpsc::channel(100);
        let fs = FileStore::set(base_folder, beacon_id);
        let pair = fs.load_pair_raw()?;
        let beacon_id = BeaconID::new(beacon_id);
        crate::core::schemes::run_beacon(pair, beacon_id.clone(), fs, rx, private_listen, pool)?;

        Ok(Self { beacon_id, tx })
    }
    pub fn id(&self) -> &BeaconID {
        &self.beacon_id
    }
    pub fn sender(&self) -> &Sender<BeaconCmd> {
        &self.tx
    }
}
