use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct SensorPayload {
    #[serde(rename = "tempDHT")]
    pub temp_dht: f32,
    pub umidade: f32,
    #[serde(rename = "tempOneWire")]
    pub temp_one_wire: f32,
    #[serde(rename = "mq2Analog")]
    pub mq2_analog: f32,
    #[serde(rename = "mq2Gas")]
    pub mq2_gas: bool,
}
