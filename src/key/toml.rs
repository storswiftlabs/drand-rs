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

use super::group::Group;
use super::keys::DistKeyShare;
use super::keys::Identity;
use super::node::Node;

use crate::core::KeyPoint;
use crate::core::Scheme;

use energon::cyber::poly::PriShare;
use energon::traits::Affine;
use energon::traits::ScalarField;

use anyhow::bail;
use anyhow::Result;

use toml::value::Array;
use toml::Table;
use toml::Value;

/// Create Map<String, Value> from (key:&str, value:Value), requires Value to be implemented by ConvertToml::to_value(&self)
macro_rules! map {
    ($($key:literal : $value:expr),* $(,)?) => {
        {
            let kv = [$(($key.to_string(), $value.into())),*];
            IntoIterator::into_iter(kv).collect::<Table>()
        }
    };
}

/// Get value by $key:&str, define resulting type of value by $method
macro_rules! get {
    ($map:expr, $key:literal, $method:ident) => {
        $map.get($key)
            .and_then(|v| v.$method())
            .ok_or_else(|| anyhow::anyhow!(concat!("Expected ", $key)))
    };
}

pub trait IntoToml: Sized {
    fn to_value(&self) -> Value;

    fn to_toml(&self) -> Result<String> {
        Ok(toml::to_string(&self.to_value())?)
    }
}

impl<S: Scheme> IntoToml for Identity<S> {
    fn to_value(&self) -> Value {
        let map = map![
             "Address":    self.address(),
             "Key":        self.key().to_string(),
             "TLS":        self.tls(),
             "Signature":  self.signature().to_string(),
             "SchemeName": S::ID,
        ];
        Value::Table(map)
    }
}

impl<S: ScalarField> IntoToml for S {
    fn to_value(&self) -> Value {
        Value::Table(map!("Key": self.to_string()))
    }
}

impl<S: Scheme> IntoToml for Node<S> {
    fn to_value(&self) -> Value {
        let mut map = self
            .identity()
            .to_value()
            .as_table()
            .expect("Value should be Table")
            .to_owned();
        map.extend(map! {"Index": self.index()});
        Value::Table(map)
    }
}

impl<S: Scheme> IntoToml for Group<S> {
    fn to_value(&self) -> Value {
        let mut map = map! {
            "Threshold":      self.threshold,
            "Period":         self.period.to_string()+"s",          // period in seconds
            "CatchupPeriod":  self.catchup_period.to_string()+"s",  // catchup_period in seconds
            "GenesisTime":    self.genesis_time as i64,
            "TransitionTime": self.transition_time as i64,
            "GenesisSeed":    hex::encode(&self.genesis_seed),
            "SchemeID":       S::ID,
            "ID":             self.beacon_id.to_string(),
        };

        let nodes: Vec<Value> = self
            .nodes
            .iter()
            .map(|node| node.to_value())
            .collect::<Array>();
        map.insert("Nodes".to_string(), Value::Array(nodes));

        let dist_key = self
            .dist_key
            .iter()
            .map(|key| Value::String(key.to_string()))
            .collect::<Array>();

        let mut public_key = Table::new();
        public_key.insert("Coefficients".to_string(), Value::Array(dist_key));
        map.insert("PublicKey".to_string(), Value::Table(public_key));
        Value::Table(map)
    }
}

pub trait FromToml: Sized {
    fn from_value(value: &Value) -> Result<Self>;

    fn from_toml(toml: &str) -> Result<Self> {
        Self::from_value(&toml::from_str(toml)?)
    }
}

impl FromToml for super::common::Identity {
    fn from_value(value: &Value) -> Result<Self> {
        let address = get!(value, "Address", as_str)?.into();
        let tls = get!(value, "TLS", as_bool)?;
        let key = hex::decode(get!(value, "Key", as_str)?)?;
        let signature = hex::decode(get!(value, "Signature", as_str)?)?;

        let scheme_name = get!(value, "SchemeName", as_str)?.to_owned();
        let identity = super::common::Identity {
            address,
            key,
            tls,
            signature,
            scheme_name: Some(scheme_name),
        };
        Ok(identity)
    }
}

impl<S: Scheme> FromToml for Identity<S> {
    fn from_value(value: &Value) -> Result<Self> {
        let scheme_str = get!(value, "SchemeName", as_str)?;
        if S::ID != scheme_str {
            bail!(
                "fs: identity load error, expected scheme: {} received: {}",
                S::ID,
                scheme_str
            )
        }
        let address = get!(value, "Address", as_str)?.to_string();
        let tls = get!(value, "TLS", as_bool)?;
        let key_bytes = hex::decode(get!(value, "Key", as_str)?)?;
        let sig_bytes = hex::decode(get!(value, "Signature", as_str)?)?;
        let identity = Self::new(
            address,
            tls,
            Affine::deserialize(&key_bytes)?,
            Affine::deserialize(&sig_bytes)?,
        );

        Ok(identity)
    }
}

impl FromToml for super::common::Node {
    fn from_value(value: &Value) -> Result<Self> {
        let index = get!(value, "Index", as_integer)? as u32;
        let identity = super::common::Identity::from_value(value)?;
        Ok(super::common::Node { identity, index })
    }
}

impl FromToml for super::common::Group {
    fn from_value(value: &Value) -> Result<Self> {
        let threshold = get!(value, "Threshold", as_integer)? as u32;
        let period = seconds_to_u32(get!(value, "Period", as_str)?)?;
        let catchup_period = seconds_to_u32(get!(value, "CatchupPeriod", as_str)?)?;
        let genesis_time = get!(value, "GenesisTime", as_integer)? as u64;
        let transition_time = get!(value, "TransitionTime", as_integer)? as u64;
        let genesis_seed = hex::decode(get!(value, "GenesisSeed", as_str)?)?;
        let scheme_id = get!(value, "SchemeID", as_str)?.to_string();
        let beacon_id = get!(value, "ID", as_str)?.to_string();

        let nodes_array = get!(value, "Nodes", as_array)?;
        let nodes: Result<Vec<_>> = nodes_array
            .iter()
            .map(super::common::Node::from_value)
            .collect();

        let dist_key_array = get!(
            get!(value, "PublicKey", as_table)?,
            "Coefficients",
            as_array
        )?;

        if dist_key_array.is_empty() {
            bail!("Distributed public key is empty")
        }

        let mut dist_key: Vec<Vec<u8>> = Vec::with_capacity(dist_key_array.len());
        for value in dist_key_array {
            match value.as_str() {
                Some(v) => dist_key.push(hex::decode(v)?),
                None => todo!(),
            }
        }

        Ok(super::common::Group {
            nodes: nodes?,
            threshold,
            period,
            genesis_time,
            transition_time,
            genesis_seed,
            dist_key,
            catchup_period,
            beacon_id,
            scheme_id,
        })
    }
}

pub type ScalarSerialized = Vec<u8>;
impl FromToml for ScalarSerialized {
    fn from_value(value: &Value) -> Result<Self> {
        Ok(hex::decode(get!(value, "Key", as_str)?)?)
    }
}

/// Remove last element, which is "s" and parse to u32
fn seconds_to_u32(seconds: &str) -> Result<u32> {
    let s = match seconds.strip_suffix(|_: char| true) {
        Some(s) => s.parse::<u32>()?,
        None => bail!("toml: invalid format for seconds: {seconds}"),
    };
    Ok(s)
}

impl<S: Scheme> IntoToml for DistKeyShare<S> {
    fn to_value(&self) -> Value {
        let mut config = map! {
            "Index": self.private().i,
            "Share": self.private().v.to_string(),
        };

        let commits = self
            .public()
            .commits
            .iter()
            .map(|commit| Value::String(commit.to_string()))
            .collect::<Array>();
        config.insert("Commits".to_string(), Value::Array(commits));

        config.insert("SchemeName".to_string(), Value::String(S::ID.to_string()));
        Value::Table(config)
    }
}
impl<S: Scheme> FromToml for DistKeyShare<S> {
    fn from_value(value: &Value) -> Result<Self> {
        let scheme_str = get!(value, "SchemeName", as_str)?;
        if S::ID != scheme_str {
            bail!(
                "fs: share load error, expected scheme: {} received: {}",
                S::ID,
                scheme_str
            )
        }
        let i = get!(value, "Index", as_integer)? as u32;
        let share_bytes = hex::decode(get!(value, "Share", as_str)?)?;
        let share = S::Scalar::from_bytes_be(&share_bytes)?;
        let commits_value = get!(value, "Commits", as_array)?;
        let mut commits: Vec<KeyPoint<S>> = vec![];
        for c in commits_value.iter() {
            if let Some(c) = c.as_str() {
                commits.push(Affine::deserialize(&hex::decode(c)?)?)
            } else {
                bail!("fs: share load: commit is empty")
            }
        }
        let dist_key_share = DistKeyShare::new(commits, PriShare { i, v: share });
        Ok(dist_key_share)
    }
}
