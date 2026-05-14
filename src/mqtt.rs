use std::time::Duration;

use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS, TlsConfiguration, Transport};

use crate::payload::SensorPayload;
use crate::storage::SensorReadingStore;

const CLIENT_ID: &str = "rust-server-sensor-monitor";
const BROKER_HOST: &str = "2cd4e9f8396443f9bf9c16820fac480f.s1.eu.hivemq.cloud";
const BROKER_PORT: u16 = 8883;
const USERNAME: &str = "rustServer";
const PASSWORD: &str = "Petterson67";
const SENSOR_TOPIC: &str = "sensors/leitura";

pub async fn run_sensor_listener(store: SensorReadingStore) {
    let (client, mut eventloop) = AsyncClient::new(mqtt_options(), 10);

    println!("Iniciando servidor MQTT...");
    println!("Aguardando conexao em {BROKER_HOST}...");

    loop {
        match eventloop.poll().await {
            Ok(event) => handle_event(&client, &store, event).await,
            Err(error) => {
                eprintln!("Erro de conexao: {error:?}");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

fn mqtt_options() -> MqttOptions {
    let mut mqtt_options = MqttOptions::new(CLIENT_ID, BROKER_HOST, BROKER_PORT);
    mqtt_options.set_credentials(USERNAME, PASSWORD);
    mqtt_options.set_transport(Transport::tls_with_config(TlsConfiguration::default()));
    mqtt_options.set_keep_alive(Duration::from_secs(10));
    mqtt_options.set_clean_session(true);
    mqtt_options
}

async fn handle_event(client: &AsyncClient, store: &SensorReadingStore, event: Event) {
    match event {
        Event::Incoming(Packet::ConnAck(_)) => {
            println!("Conectado ao broker HiveMQ. Inscrevendo no topico...");
            if let Err(error) = client.subscribe(SENSOR_TOPIC, QoS::AtMostOnce).await {
                eprintln!("Erro ao subscrever: {error:?}");
            } else {
                println!("Inscrito em '{SENSOR_TOPIC}' com sucesso.");
            }
        }
        Event::Incoming(Packet::SubAck(_)) => {
            println!("SubAck recebido. Aguardando dados dos sensores...");
        }
        Event::Incoming(Packet::Publish(message)) => {
            let raw = String::from_utf8_lossy(&message.payload);
            match serde_json::from_str::<SensorPayload>(&raw) {
                Ok(data) => {
                    print_reading(&message.topic, &data);

                    if let Err(error) = store.insert_reading(&data) {
                        eprintln!("Erro ao salvar leitura no banco SQLite: {error}");
                    } else {
                        println!("Leitura salva no banco SQLite.");
                    }
                }
                Err(error) => {
                    eprintln!("Erro ao processar JSON: {error}");
                    eprintln!("Payload bruto: {raw}");
                }
            }
        }
        _ => {}
    }
}

fn print_reading(topic: &str, data: &SensorPayload) {
    println!("-----------------------------------------");
    println!("Mensagem recebida em: {topic}");
    println!("Temp DHT:      {:.1} C", data.temp_dht);
    println!("Umidade:       {:.1}%", data.umidade);
    println!("Temp OneWire:  {:.1} C", data.temp_one_wire);
    println!("MQ2 Analog:    {:.0}", data.mq2_analog);
    println!(
        "MQ2 Gas:       {}",
        if data.mq2_gas {
            "detectado"
        } else {
            "nao detectado"
        }
    );
    println!("-----------------------------------------");
}
