mod mqtt;
mod payload;
mod storage;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = storage::SensorReadingStore::open("sensor_readings.db")?;
    mqtt::run_sensor_listener(store).await;
    Ok(())
}
