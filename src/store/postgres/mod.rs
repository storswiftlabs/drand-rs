use super::{Beacon, StorageError, Store};
use cursor::PgCursor;
use sqlx::{PgPool, Row};
use tonic::async_trait;

#[cfg(test)]
mod db_container;

pub mod cursor;

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

    async fn get_beacon(&self, round: u64) -> Result<Beacon, StorageError> {
        let row = sqlx::query("SELECT * FROM beacon_details WHERE beacon_id = $1 AND round = $2")
            .bind(self.beacon_id)
            .bind(round as i64)
            .fetch_one(&self.db)
            .await
            .map_err(|e| e.into())?;
        let beacon = Beacon {
            previous_sig: vec![],
            round: row.try_get::<i64, _>("round").map_err(|e| e.into())? as u64,
            signature: row.try_get("signature").map_err(|e| e.into())?,
        };

        Ok(beacon)
    }

    async fn get_at_position(&self, pos: usize) -> Result<Beacon, StorageError> {
        let row = sqlx::query("SELECT * FROM beacon_details WHERE beacon_id = $1 ORDER BY round ASC LIMIT 1 OFFSET $2")
            .bind(self.beacon_id)
            .bind(pos as i64)
            .fetch_one(&self.db)
            .await
            .map_err(|e| e.into())?;
        let mut beacon = Beacon {
            previous_sig: vec![],
            round: row.try_get::<i64, _>("round").map_err(|e| e.into())? as u64,
            signature: row.try_get("signature").map_err(|e| e.into())?,
        };

        if beacon.round > 0 && self.requires_previous {
            let prev = self.get_beacon(beacon.round - 1).await?;
            beacon.previous_sig = prev.signature;
        }

        Ok(beacon)
    }

    async fn get_pos(&self, round: u64) -> Result<usize, StorageError> {
        let count =
            sqlx::query("SELECT COUNT(*) FROM beacon_details WHERE beacon_id = $1 AND round < $2")
                .bind(self.beacon_id)
                .bind(round as i64)
                .fetch_one(&self.db)
                .await
                .map_err(|e| e.into())?
                .get::<i64, _>("count");
        Ok(count as usize)
    }
}

#[async_trait]
impl Store for PgStore {
    type Cursor<'a> = PgCursor<'a>;
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
            let prev = self.get_beacon(beacon.round - 1).await?;
            beacon.previous_sig = prev.signature;
        }

        Ok(beacon)
    }

    async fn first(&self) -> Result<Beacon, StorageError> {
        let result = sqlx::query(
            "SELECT * FROM beacon_details WHERE beacon_id = $1 ORDER BY round ASC LIMIT 1",
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
            let prev = self.get_beacon(beacon.round - 1).await?;
            beacon.previous_sig = prev.signature;
        }

        Ok(beacon)
    }

    async fn get(&self, round: u64) -> Result<Beacon, StorageError> {
        let mut beacon = self.get_beacon(round).await?;

        if round > 0 && self.requires_previous {
            let prev = self.get_beacon(round - 1).await?;
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

    fn cursor(&self) -> PgCursor<'_> {
        PgCursor::new(self)
    }
}

#[allow(clippy::from_over_into)]
impl Into<StorageError> for sqlx::Error {
    fn into(self) -> StorageError {
        match self {
            sqlx::Error::RowNotFound => StorageError::NotFound,
            sqlx::Error::ColumnNotFound(e) => StorageError::KeyError(e.to_string()),
            _ => StorageError::IoError(self.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use sqlx::postgres::PgPoolOptions;

    use crate::store::testing::{test_cursor, test_store};

    use super::*;

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
                db_container::end_pg(&pg_id).await.unwrap();
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
            let (pg_id, pg_ip) = db_container::start_pg().await.unwrap();
            let testdata = PgTestData { pg_id, pg_ip };

            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect(&format!(
                    "postgres://postgres:password@{}/test",
                    testdata.pg_ip
                ))
                .await
                .unwrap();

            let mut store = PgStore::new(pool, true, "beacon_name")
                .await
                .expect("Failed to create store");

            test_store(&store).await;

            store.requires_previous = false;

            test_cursor(&store).await;
        });
    }
}
