use std::path::Path;

use rusqlite::{Connection, Result, params};

use crate::payload::SensorPayload;

pub struct SensorReadingStore {
    connection: Connection,
}

impl SensorReadingStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let store = Self {
            connection: Connection::open(path)?,
        };

        store.create_schema()?;
        Ok(store)
    }

    pub fn insert_reading(&self, reading: &SensorPayload) -> Result<()> {
        self.connection.execute(
            r#"
            INSERT INTO sensor_readings (
                temp_dht,
                humidity,
                temp_one_wire,
                mq2_analog,
                mq2_gas
            )
            VALUES (?1, ?2, ?3, ?4, ?5)
            "#,
            params![
                reading.temp_dht as f64,
                reading.umidade as f64,
                reading.temp_one_wire as f64,
                reading.mq2_analog as f64,
                reading.mq2_gas
            ],
        )?;

        Ok(())
    }

    fn create_schema(&self) -> Result<()> {
        self.connection.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS sensor_readings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recorded_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                temp_dht REAL NOT NULL,
                humidity REAL NOT NULL,
                temp_one_wire REAL NOT NULL,
                mq2_analog REAL NOT NULL,
                mq2_gas INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_sensor_readings_recorded_at
                ON sensor_readings (recorded_at);
            "#,
        )?;

        self.add_column_if_missing("mq2_analog", "REAL NOT NULL DEFAULT 0")?;
        self.add_column_if_missing("mq2_gas", "INTEGER NOT NULL DEFAULT 0")?;

        Ok(())
    }

    fn add_column_if_missing(&self, column_name: &str, column_definition: &str) -> Result<()> {
        let exists = self
            .connection
            .prepare("PRAGMA table_info(sensor_readings)")?
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<Result<Vec<_>>>()?
            .iter()
            .any(|name| name == column_name);

        if !exists {
            self.connection.execute(
                &format!(
                    "ALTER TABLE sensor_readings ADD COLUMN {column_name} {column_definition}"
                ),
                [],
            )?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[test]
    fn creates_schema_and_persists_readings() {
        let db_path = std::env::temp_dir().join(format!(
            "servidor-mqtt-test-{}.db",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time should be after Unix epoch")
                .as_nanos()
        ));

        let store = SensorReadingStore::open(&db_path).expect("database should open");
        let reading = SensorPayload {
            temp_dht: 24.5,
            umidade: 63.0,
            temp_one_wire: 23.8,
            mq2_analog: 1400.0,
            mq2_gas: true,
        };

        store
            .insert_reading(&reading)
            .expect("reading should be inserted");

        let count: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM sensor_readings", [], |row| row.get(0))
            .expect("reading count should be available");

        assert_eq!(count, 1);

        let (mq2_analog, mq2_gas, recorded_at): (f64, bool, String) = store
            .connection
            .query_row(
                "SELECT mq2_analog, mq2_gas, recorded_at FROM sensor_readings LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("stored MQ2 data should be available");

        assert_eq!(mq2_analog, 1400.0);
        assert!(mq2_gas);
        assert!(!recorded_at.is_empty());

        drop(store);
        fs::remove_file(db_path).ok();
    }
}
