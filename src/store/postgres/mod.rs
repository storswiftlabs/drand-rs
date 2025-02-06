use super::{Beacon, StorageError, Store};
use sqlx::{PgPool, Row};
use tonic::async_trait;

#[cfg(test)]
mod db_container;

pub struct PgStore {
    // db is the database connection
    db: PgPool,
    beacon_id: i32,
    requires_previous: bool,
}

impl PgStore {
    pub async fn new(
        pool: PgPool,
        requires_previous: bool,
        beacon_name: &str,
    ) -> Result<Self, sqlx::Error> {
        sqlx::migrate!("src/store/postgres/migrations")
            .run(&pool)
            .await?;

        let mut store = PgStore {
            db: pool,
            beacon_id: 0,
            requires_previous,
        };
        let beacon_id = store.add_beacon_name(beacon_name).await?;
        store.beacon_id = beacon_id;

        Ok(store)
    }

    pub async fn add_beacon_name(&self, beacon_name: &str) -> Result<i32, sqlx::Error> {
        sqlx::query("INSERT INTO beacons (name) VALUES ($1) ON CONFLICT DO NOTHING")
            .bind(beacon_name)
            .execute(&self.db)
            .await?;

        let beacon_id = sqlx::query("SELECT id FROM beacons WHERE name = $1")
            .bind(beacon_name)
            .fetch_one(&self.db)
            .await?
            .get::<i32, _>("id");
        Ok(beacon_id)
    }
}

#[async_trait]
impl Store for PgStore {
    async fn len(&self) -> Result<usize, StorageError> {
        let count = sqlx::query("SELECT COUNT(*) FROM beacon_details WHERE beacon_id = $1")
            .bind(self.beacon_id)
            .fetch_one(&self.db)
            .await
            .map_err(|e| e.into())?
            .get::<i64, _>("count");
        Ok(count as usize)
    }

    async fn put(&self, b: Beacon) -> Result<(), StorageError> {
        let query = r#"
	INSERT INTO beacon_details
		(beacon_id, round, signature)
	VALUES
		($1, $2, $3)
	ON CONFLICT DO NOTHING
"#;

        let result = sqlx::query(query)
            .bind(self.beacon_id)
            .bind(b.round as i64) // maps u64 to bigint
            .bind(b.signature)
            .execute(&self.db)
            .await;

        result.map_err(|e| StorageError::IoError(e.to_string()))?;
        Ok(())
    }

    async fn last(&self) -> Result<Beacon, StorageError> {
        let result = sqlx::query(
            "SELECT * FROM beacon_details WHERE beacon_id = $1 ORDER BY round DESC LIMIT 1",
        )
        .bind(self.beacon_id)
        .fetch_one(&self.db)
        .await;

        let row = match result {
            Ok(row) => row,
            Err(e) => return Err(e.into()),
        };
        let mut beacon = Beacon {
            previous_sig: vec![],
            round: row.try_get::<i64, _>("round").map_err(|e| e.into())? as u64,
            signature: row.try_get("signature").map_err(|e| e.into())?,
        };

        if beacon.round > 0 && self.requires_previous {
            let prev = self.get(beacon.round - 1).await?;
            beacon.previous_sig = prev.signature;
        }

        Ok(beacon)
    }

    async fn get(&self, round: u64) -> Result<Beacon, StorageError> {
        let row = sqlx::query("SELECT * FROM beacon_details WHERE beacon_id = $1 AND round = $2")
            .bind(self.beacon_id)
            .bind(round as i64)
            .fetch_one(&self.db)
            .await
            .map_err(|e| e.into())?;
        let mut beacon = Beacon {
            previous_sig: vec![],
            round: row.try_get::<i64, _>("round").map_err(|e| e.into())? as u64,
            signature: row.try_get("signature").map_err(|e| e.into())?,
        };

        if round > 0 && self.requires_previous {
            let prev = self.get(round - 1).await?;
            beacon.previous_sig = prev.signature;
        }

        Ok(beacon)
    }

    async fn close(self) -> Result<(), StorageError> {
        Ok(())
    }

    async fn del(&self, round: u64) -> Result<(), StorageError> {
        sqlx::query("DELETE FROM beacon_details WHERE beacon_id = $1 AND round = $2")
            .bind(self.beacon_id)
            .bind(round as i64)
            .execute(&self.db)
            .await
            .map_err(|e| e.into())?;
        Ok(())
    }
}

fn sqlx_to_storage_error(e: sqlx::Error) -> StorageError {
    match e {
        sqlx::Error::RowNotFound => StorageError::NotFound,
        sqlx::Error::ColumnNotFound(e) => StorageError::KeyError(e.to_string()),
        _ => StorageError::IoError(e.to_string()),
    }
}

impl Into<StorageError> for sqlx::Error {
    fn into(self) -> StorageError {
        sqlx_to_storage_error(self)
    }
}

#[cfg(test)]
mod tests {
    use sqlx::postgres::PgPoolOptions;

    use crate::store::{Beacon, Store};

    use super::{
        db_container::{end_pg, start_pg},
        PgStore,
    };

    #[derive(Debug)]
    struct PgTestData {
        pg_id: String,
        pg_ip: String,
    }

    impl Drop for PgTestData {
        fn drop(&mut self) {
            let pg_id = self.pg_id.clone();
            let task = tokio::spawn(async move {
                println!("dropping pg container");
                end_pg(&pg_id).await.unwrap();
                println!("dropped pg container");
            });
            let handle = tokio::runtime::Handle::current().clone();
            tokio::task::block_in_place(move || {
                handle.block_on(task).unwrap();
            });
        }
    }

    #[test]
    fn test_pg() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (pg_id, pg_ip) = start_pg().await.unwrap();
            let testdata = PgTestData { pg_id, pg_ip };

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect(&format!(
                    "postgres://postgres:password@{}/test",
                    testdata.pg_ip
                ))
                .await
                .unwrap();

            let store = PgStore::new(pool, true, "beacon_name")
                .await
                .expect("Failed to create store");

            store
                .put(Beacon {
                    previous_sig: vec![1, 2, 3],
                    round: 0,
                    signature: vec![4, 5, 6],
                })
                .await
                .expect("Failed to put beacon");

            store
                .put(Beacon {
                    previous_sig: vec![4, 5, 6],
                    round: 1,
                    signature: vec![7, 8, 9],
                })
                .await
                .expect("Failed to put beacon");

            let b = store.get(1).await.expect("Failed to get beacon");

            assert_eq!(
                b.previous_sig,
                vec![4, 5, 6],
                "Previous signature does not match"
            );
            assert_eq!(b.round, 1, "Round does not match");
            assert_eq!(b.signature, vec![7, 8, 9], "Signature does not match");

            let len = store.len().await.expect("Failed to get length");

            assert_eq!(len, 2, "Length should be 2");

            let b = store.last().await.expect("Failed to get last");
            assert_eq!(
                b.previous_sig,
                vec![4, 5, 6],
                "Previous signature does not match"
            );
            assert_eq!(b.round, 1, "Round does not match");
            assert_eq!(b.signature, vec![7, 8, 9], "Signature does not match");

            store.del(1).await.expect("Failed to delete");

            let len = store.len().await.expect("Failed to get length");

            assert_eq!(len, 1, "Length should be 1");
        });
    }
}
