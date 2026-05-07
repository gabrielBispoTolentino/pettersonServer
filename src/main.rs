use rumqttc::{AsyncClient, MqttOptions, QoS, Event, Packet, Transport, TlsConfiguration};
use serde::Deserialize;
use tokio;

#[derive(Debug, Deserialize)]
struct SensorPayload {
    tempDHT: f32,
    umidade: f32,
    tempOneWire: f32,
}

#[tokio::main]
async fn main() {
    let mut mqttoptions = MqttOptions::new(
        "rust-server-sensor-monitor",
        "2cd4e9f8396443f9bf9c16820fac480f.s1.eu.hivemq.cloud",
        8883,
    );
    mqttoptions.set_credentials("rustServer", "Petterson67");
    mqttoptions.set_transport(Transport::tls_with_config(
        TlsConfiguration::default()
    ));
    mqttoptions.set_keep_alive(std::time::Duration::from_secs(10));
    mqttoptions.set_clean_session(true);

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    println!("Iniciando servidor MQTT...");
    println!("Aguardando conexão em 2cd4e9f8396443f9bf9c16820fac480f.s1.eu.hivemq.cloud...");

    loop {
        match eventloop.poll().await {
            Ok(event) => {
                match &event {
                    Event::Incoming(Packet::ConnAck(_)) => {
                        println!("Conectado ao broker HiveMQ! Inscrevendo no tópico...");
                        if let Err(e) = client
                            .subscribe("sensors/leitura", QoS::AtMostOnce)
                            .await
                        {
                            eprintln!("Erro ao subscrever: {:?}", e);
                        } else {
                            println!("Inscrito em 'sensors/leitura' com sucesso.");
                        }
                    }
                    Event::Incoming(Packet::SubAck(_)) => {
                        println!("SubAck recebido — aguardando dados dos sensores...");
                    }
                    Event::Incoming(Packet::Publish(msg)) => {
                        let raw = String::from_utf8_lossy(&msg.payload);
                        match serde_json::from_str::<SensorPayload>(&raw) {
                            Ok(data) => {
                                println!("─────────────────────────────────────────");
                                println!("Mensagem recebida em: {}", msg.topic);
                                println!("Temp DHT:      {:.1}°C", data.tempDHT);
                                println!("Umidade:       {:.1}%",  data.umidade);
                                println!("Temp OneWire:  {:.1}°C", data.tempOneWire);
                                println!("─────────────────────────────────────────");
                            }
                            Err(e) => {
                                eprintln!("Erro ao processar JSON: {}", e);
                                eprintln!("Payload bruto: {}", raw);
                            }
                        }
                    }
                    _ => {}
                }
            }
            Err(e) => {
                eprintln!("Erro de conexão: {:?}", e);
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        }
    }
}