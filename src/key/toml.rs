use super::group::Group;
use super::keys::DistPublic;
use super::keys::Identity;
use super::keys::Pair;
use super::keys::Share;
use super::node::Node;
use super::Scheme;
use crate::net::utils::Address;
use crate::net::utils::Seconds;

use energon::kyber::poly::PriShare;
use energon::traits::Affine;
use energon::traits::ScalarField;

use std::str::FromStr;
use toml_edit::Array;
use toml_edit::ArrayOfTables;
use toml_edit::DocumentMut;
use toml_edit::Item;
use toml_edit::Table;
use toml_edit::Value;

pub trait Toml: Sized {
    type Inner;

    /// Encodes into TOML representation.
    fn toml_encode(&self) -> Option<Self::Inner>;
    /// Decodes from TOML representation
    fn toml_decode(value: &Self::Inner) -> Option<Self>;
}

/// PairToml is a toml representation of a [`Pair`]
pub struct PairToml {
    private: DocumentMut,
    public: DocumentMut,
}

impl PairToml {
    pub fn new(private: DocumentMut, public: DocumentMut) -> Self {
        Self { private, public }
    }

    pub fn parse(private: &str, public: &str) -> Option<Self> {
        let private = private.parse().ok()?;
        let public = public.parse().ok()?;
        Some(Self::new(private, public))
    }

    /// Returns a str representation of associated [Scheme::ID] if public and private parts contain the `SchemeName`.
    pub fn get_scheme_id(&self) -> Option<&str> {
        let private_scheme = self.private.get("SchemeName")?.as_str()?;
        let public_scheme = self.public.get("SchemeName")?.as_str()?;

        if private_scheme != public_scheme || public_scheme.is_empty() {
            return None;
        }

        Some(public_scheme)
    }

    pub fn private(&self) -> String {
        self.private.to_string()
    }

    pub fn public(&self) -> String {
        self.public.to_string()
    }
}

impl<S: Scheme> Toml for Pair<S> {
    type Inner = PairToml;

    fn toml_encode(&self) -> Option<Self::Inner> {
        let public = self.public_identity().toml_encode()?;
        let private_key_bytes = self.private_key().to_bytes_be().ok()?;
        let mut private = Table::new();
        let _ = private.insert("Key", hex::encode(private_key_bytes).into());
        let _ = private.insert("SchemeName", S::ID.into());

        Some(Self::Inner::new(private.into(), public.into()))
    }

    fn toml_decode(pair: &Self::Inner) -> Option<Self> {
        let public = Identity::toml_decode(&pair.public)?;
        let private_bytes = hex::decode(pair.private.get("Key")?.as_str()?).ok()?;
        let private = ScalarField::from_bytes_be(&private_bytes).ok()?;

        Some(Self::new(private, public))
    }
}

impl<S: Scheme> Toml for Identity<S> {
    type Inner = Table;

    fn toml_encode(&self) -> Option<Self::Inner> {
        let key_bytes = self.key().serialize().ok()?;
        let signature_bytes = self.signature().serialize().ok()?;

        let mut table = Self::Inner::new();
        let _ = table.insert("Address", self.address().into());
        let _ = table.insert("Key", hex::encode(key_bytes).into());
        let _ = table.insert("Signature", hex::encode(signature_bytes).into());
        let _ = table.insert("SchemeName", S::ID.into());

        Some(table)
    }

    fn toml_decode(table: &Self::Inner) -> Option<Self> {
        if S::ID != table.get("SchemeName")?.as_str()? {
            return None;
        }
        let address = Address::precheck(table.get("Address")?.as_str()?).ok()?;
        let key_bytes = hex::decode(table.get("Key")?.as_str()?).ok()?;
        let signature_bytes = hex::decode(table.get("Signature")?.as_str()?).ok()?;

        let key = Affine::deserialize(&key_bytes).ok()?;
        let signature = Affine::deserialize(&signature_bytes).ok()?;

        Some(Self::new(address, key, signature))
    }
}

impl<S: Scheme> Toml for Node<S> {
    type Inner = Table;

    fn toml_encode(&self) -> Option<Self::Inner> {
        let mut table = self.public().toml_encode()?;
        let _ = table.insert("Index", (self.index() as i64).into());

        Some(prefix_keys(table))
    }

    fn toml_decode(table: &Self::Inner) -> Option<Self> {
        let public = Identity::toml_decode(table)?;
        let index = table.get("Index")?.as_integer()? as u32;

        Some(Self::new(public, index))
    }
}

impl<S: Scheme> Toml for DistPublic<S> {
    type Inner = Table;

    fn toml_encode(&self) -> Option<Self::Inner> {
        let mut commits = Array::new();
        for commit in self.commits().iter() {
            commits.push(Value::from(hex::encode(commit.serialize().ok()?)))
        }
        let mut table = Self::Inner::new();
        let _ = table.insert("Coefficients", commits.into());

        Some(prefix_keys(table))
    }

    fn toml_decode(table: &Self::Inner) -> Option<Self> {
        let array = table.get("Coefficients")?.as_array()?;
        let mut commits = Vec::with_capacity(array.len());
        for commit in array.iter() {
            commits.push(Affine::deserialize(&hex::decode(commit.as_str()?).ok()?).ok()?)
        }

        Some(Self::new(commits))
    }
}

impl<S: Scheme> Toml for Group<S> {
    type Inner = DocumentMut;

    fn toml_encode(&self) -> Option<Self::Inner> {
        let mut nodes = ArrayOfTables::new();
        for node in self.nodes().iter() {
            nodes.push(node.toml_encode()?)
        }

        let mut doc = Self::Inner::new();
        doc.insert("Threshold", (self.threshold as i64).into());
        doc.insert("Period", self.period.to_string().into());
        doc.insert("CatchupPeriod", self.catchup_period.to_string().into());
        doc.insert("GenesisTime", (self.genesis_time as i64).into());
        doc.insert("TransitionTime", (self.transition_time as i64).into());
        doc.insert("GenesisSeed", hex::encode(&self.genesis_seed).into());
        doc.insert("SchemeID", S::ID.into());
        doc.insert("ID", self.beacon_id.as_str().into());
        doc.insert("Nodes", Item::ArrayOfTables(nodes));
        doc.insert("PublicKey", Item::Table(self.dist_key.toml_encode()?));

        Some(doc)
    }

    fn toml_decode(table: &Self::Inner) -> Option<Self> {
        if S::ID != table.get("SchemeID")?.as_str()? {
            return None;
        }
        let threshold = table.get("Threshold")?.as_integer()? as u32;
        let period = table.get("Period")?.as_str().map(Seconds::from_str)?.ok()?;
        let genesis_time = table.get("GenesisTime")?.as_integer()? as u64;
        let transition_time = table.get("TransitionTime")?.as_integer()? as u64;
        let genesis_seed = table.get("GenesisSeed")?.as_str().map(hex::decode)?.ok()?;
        let beacon_id = table.get("ID")?.as_str()?.into();

        let catchup_period = table
            .get("CatchupPeriod")?
            .as_str()
            .map(Seconds::from_str)?
            .ok()?;

        let nodes = table
            .get("Nodes")?
            .as_array_of_tables()?
            .iter()
            .map(Node::toml_decode)
            .collect::<Option<Vec<_>>>()?;

        let dist_key = table
            .get("PublicKey")?
            .as_table()
            .map(DistPublic::toml_decode)??;

        Some(Self::new(
            threshold,
            period,
            catchup_period,
            genesis_time,
            transition_time,
            genesis_seed,
            beacon_id,
            nodes,
            dist_key,
        ))
    }
}

impl<S: Scheme> Toml for Share<S> {
    type Inner = DocumentMut;

    fn toml_encode(&self) -> Option<Self::Inner> {
        let share_bytes = self.private().value().to_bytes_be().ok()?;

        let mut commits = Array::new();
        for commit in self.public().commits().iter() {
            commits.push(Value::from(hex::encode(commit.serialize().ok()?)))
        }

        let mut table = Self::Inner::new();
        let _ = table.insert("Index", (self.private().index() as i64).into());
        let _ = table.insert("Share", hex::encode(share_bytes).into());
        let _ = table.insert("Commits", commits.into());
        let _ = table.insert("SchemeName", S::ID.into());

        Some(table)
    }

    fn toml_decode(table: &Self::Inner) -> Option<Self> {
        if S::ID != table.get("SchemeName")?.as_str()? {
            return None;
        }
        let index = table.get("Index")?.as_integer()? as u32;
        let private_key_bytes = hex::decode(table.get("Share")?.as_str()?).ok()?;
        let private_key = ScalarField::from_bytes_be(&private_key_bytes).ok()?;
        let pri_share = PriShare::new(index, private_key);

        let commits_array = table.get("Commits")?.as_array()?;
        let mut commits = Vec::with_capacity(commits_array.len());
        for commit in commits_array.iter() {
            commits.push(Affine::deserialize(&hex::decode(commit.as_str()?).ok()?).ok()?)
        }

        Some(Self::new(DistPublic::new(commits), pri_share))
    }
}

/// Helper function to set table **keys** prefix
fn prefix_keys(mut table: Table) -> Table {
    let keys: Vec<String> = table.iter().map(|(key, _)| key.into()).collect();

    for key in keys.iter() {
        if let Some(mut key_mut) = table.key_mut(key.as_str()) {
            // double-space prefix for consistency with golang implementation
            key_mut.leaf_decor_mut().set_prefix("  ")
        }
    }

    table
}

#[cfg(test)]
mod tests {
    use super::*;
    use energon::drand::schemes::DefaultScheme;

    #[test]
    fn toml_roundtrip() {
        default_vectors::<DefaultScheme>();
    }

    fn default_vectors<S: Scheme>() {
        // Group<S> from/into drand_group.toml
        let expected = toml_samples::group();
        let group: Group<S> = Toml::toml_decode(&expected.parse().unwrap()).unwrap();
        let received = group.toml_encode().unwrap().to_string();
        assert!(expected == received);

        // Share<S> from/into dist_key.private
        let expected = toml_samples::dist_key();
        let share: Share<S> = Toml::toml_decode(&expected.parse().unwrap()).unwrap();
        let received = share.toml_encode().unwrap().to_string();
        assert!(expected == received);

        // Pair<S> from/into drand_id.private drand_id.public
        let expected_private = toml_samples::private_key();
        let expected_public = toml_samples::identity();
        let pair_toml = PairToml::parse(expected_private, expected_public).unwrap();
        let pair: Pair<S> = Toml::toml_decode(&pair_toml).unwrap();
        let received = pair.toml_encode().unwrap();
        assert!(expected_private == received.private());
        assert!(expected_public == received.public());
    }

    /// Data is generated by <https://github.com/drand/drand/tree/master/demo#local-demo-of-drand>
    #[rustfmt::skip]
    mod toml_samples {

        /// filename: drand_group.toml
        pub fn group() -> &'static str {
r#"Threshold = 4
Period = "3s"
CatchupPeriod = "1s"
GenesisTime = 1736058215
TransitionTime = 1736058215
GenesisSeed = "023779ea9eac851bf27c35cdff64b8e55774ad8b20752c0258fce264fa70b57c"
SchemeID = "pedersen-bls-chained"
ID = "default"

[[Nodes]]
  Address = "127.0.0.1:36023"
  Key = "8f759c785b9395ca8b3e069da592f50e934c618aa5d0abbd256706f356c8511361243c90f34a22a28374cb2e23e3691b"
  Signature = "8b48047c4e9e87af9c6955ae4dcddbc6b2356b24c2e2a6f7ab0b6a4058dfa18c82d3e8e95abe027ac8ab156fb29636bd173462aaabd83d5977c56313e9c42d3f99470f289f6ff92eed8a45915defa16c27fbb138ee656955f20a591bc4066701"
  SchemeName = "pedersen-bls-chained"
  Index = 0

[[Nodes]]
  Address = "127.0.0.1:44901"
  Key = "9692fb81be90f5754dd6514ddcabccce4d16d20d52ce4d130d94c63268ca744d2e4784cdcdb319824a16ff36d4b26d30"
  Signature = "9222bb2f3ee82d55de320396efb136d43f9876590acc627c679afe69aec189907bf0cd721ba8b415fa373a0eae3d7545020116b134e9238a81f19bca03c04112a198f783e47cd69f4cdd9206f0e09526aea45ab6b1d3483c9bb2a724ec132f49"
  SchemeName = "pedersen-bls-chained"
  Index = 1

[[Nodes]]
  Address = "127.0.0.1:40733"
  Key = "96c69742f60912fcce8e544d0b5bea4bcca556aac4a8b2b39d4e7cfa8b5eebbce54712a657a6a411e8814ac5844234d3"
  Signature = "997fa9dd0d6cf0985d2d23d0bc1d36eba1ff7f41585f6a2395d0574e9aaa79396ccd2ef119f435cb5c59cc5cba4677cd18a47e527e27dfc6202ea752acdb8a68c75c1ed9791e21fc872cd603614c9071059c6db39cd4f08680c3a9a67a3db696"
  SchemeName = "pedersen-bls-chained"
  Index = 2

[[Nodes]]
  Address = "127.0.0.1:34689"
  Key = "aa975e6d8140506f721edfe0df1c602cf7b2385871d35d9164e37ac68bc071ff5e9cf64bacc17d8a3bb8caa85e06e12b"
  Signature = "b9e4f1c9076bcb05acf74e3fdb952e1c48420c3cb14038bbb4eb95876a85118a56096f8a7f2407ebdeb00adb557263070debd3a09c022267436c33f91420736418929bc72fd4037f08e7ba1516e7a6b2a846f4fb34819e54ee7617035782b9d9"
  SchemeName = "pedersen-bls-chained"
  Index = 3

[[Nodes]]
  Address = "127.0.0.1:38161"
  Key = "ab37151b401ba77a9a5bf002d8381b7a9b45f789dc4c3dc40faf0421b7d409c1496e487626ae45482517fffa3de12741"
  Signature = "a28ad77f42e540230a475412f860cdc2ff35a98a8ccd93c01f3e1652fd8dcc4c50def4c81718b007e7876f387e45308017b7bbeb69ef658a851b9328178aed2be06a38197d795d709d5a89f247bc698a051866c293714b718576eaccfd8b069d"
  SchemeName = "pedersen-bls-chained"
  Index = 4

[[Nodes]]
  Address = "127.0.0.1:40743"
  Key = "ad4fab7573351bd57066c487f391c1e52cebbe58d8c69cbc4b5586add6e6315f5b1d935ce412f7406b013c946f00b015"
  Signature = "964c4dd662a469e405e79825d73a1362168f1479aa3e077d23a1602fbecc8a8ee71abefb41d3f7456f65e469557ef62b0e598bd9ad6e12662a97724e2432bf7d5dde064cfbcfd4fbc4285810cdcf5d2f0fc18f482fd6a548cb3519c68c9d6aeb"
  SchemeName = "pedersen-bls-chained"
  Index = 5

[PublicKey]
  Coefficients = ["b86ba00f3eb8bda9cd60723ad45402a01c36329dae25e6cc84800572e3ec1dd6b9efebf0ea7fd46ae7abb7b760076635", "96fce82b8a327b1b986ce23dc18b8b39423512e7bac610672f4f6dff1682944248e21571498ef9c615348bbf9a1ec9a3", "ad1caaff9c1c90d10a33cf89a5497dbcb70de64aed07efe997c912a2d70c024dd4afafc89c374191c27147da5b7ea916", "8d37571eed924b8e4df483914b714bbac9356c50045ab9c7f22f2c42edde46ef163ee69c8d65b235e51c3b4906f2977e"]
"#
        }

        /// filename: drand_id.private
        pub fn private_key() -> &'static str {
r#"Key = "4dee50f69880dce2b793ed2bad9966bc1d333c0ab28f2eff2794753e65202747"
SchemeName = "pedersen-bls-chained"
"#
        }

        /// filename: drand_id.public
        pub fn identity() -> &'static str {
r#"Address = "127.0.0.1:38161"
Key = "ab37151b401ba77a9a5bf002d8381b7a9b45f789dc4c3dc40faf0421b7d409c1496e487626ae45482517fffa3de12741"
Signature = "a28ad77f42e540230a475412f860cdc2ff35a98a8ccd93c01f3e1652fd8dcc4c50def4c81718b007e7876f387e45308017b7bbeb69ef658a851b9328178aed2be06a38197d795d709d5a89f247bc698a051866c293714b718576eaccfd8b069d"
SchemeName = "pedersen-bls-chained"
"#
        }

        /// filename: dist_key.private
        pub fn dist_key() -> &'static str {
r#"Index = 4
Share = "675431249f101cf5001b4dd0dd8f25107a2c547b77b0693309731906b12fca98"
Commits = ["b86ba00f3eb8bda9cd60723ad45402a01c36329dae25e6cc84800572e3ec1dd6b9efebf0ea7fd46ae7abb7b760076635", "96fce82b8a327b1b986ce23dc18b8b39423512e7bac610672f4f6dff1682944248e21571498ef9c615348bbf9a1ec9a3", "ad1caaff9c1c90d10a33cf89a5497dbcb70de64aed07efe997c912a2d70c024dd4afafc89c374191c27147da5b7ea916", "8d37571eed924b8e4df483914b714bbac9356c50045ab9c7f22f2c42edde46ef163ee69c8d65b235e51c3b4906f2977e"]
SchemeName = "pedersen-bls-chained"
"#
        }
    }
}
