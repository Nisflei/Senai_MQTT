# simular_sensor.py — Simula um dispositivo IoT publicando dados via MQTT

import os
import json
import time
import random
import logging
import signal
import sys
from dotenv import load_dotenv
from awscrt import mqtt
from awsiot import mqtt_connection_builder

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("sim")

CONFIG = {
    "endpoint": os.getenv("IOT_ENDPOINT"),
    "cert_path": os.getenv("IOT_CERT_PATH"),
    "key_path": os.getenv("IOT_KEY_PATH"),
    "ca_path": os.getenv("IOT_CA_PATH"),
    "client_id": f"simulador-sensor-{int(time.time())}",
    "topico": os.getenv("IOT_TOPIC", "sensores/temperatura"),
    "intervalo_s": 5,  # publica a cada 5 segundos
}


def main():
    log.info("[SIM] Iniciando simulador de sensor IoT...")

    mqtt_connection = mqtt_connection_builder.mtls_from_path(
        endpoint=CONFIG["endpoint"],
        cert_filepath=CONFIG["cert_path"],
        pri_key_filepath=CONFIG["key_path"],
        ca_filepath=CONFIG["ca_path"],
        client_id=CONFIG["client_id"],
        clean_session=True,
        keep_alive_secs=30,
    )

    mqtt_connection.connect().result()
    log.info("[SIM] Conectado ao IoT Core!")
    log.info(f"[SIM] Publicando no tópico: {CONFIG['topico']}")
    log.info(f"[SIM] Intervalo: {CONFIG['intervalo_s']}s\n")

    contador = [0]  # lista para permitir mutação dentro da função de encerramento

    def encerrar(signum, frame):
        log.info(f"\n[SIM] Encerrando simulador... Total enviado: {contador[0]}")
        try:
            mqtt_connection.disconnect().result()
        except Exception:
            pass
        sys.exit(0)

    signal.signal(signal.SIGINT, encerrar)
    signal.signal(signal.SIGTERM, encerrar)

    while True:
        contador[0] += 1

        payload = {
            "sensor_id": "sensor-temp-01",
            "tipo": "temperatura",
            "valor": round(20 + random.random() * 15, 2),
            "unidade": "°C",
            "timestamp": int(time.time() * 1000),
            "localizacao": "sala-servidores",
        }

        try:
            mqtt_connection.publish(
                topic=CONFIG["topico"],
                payload=json.dumps(payload),
                qos=mqtt.QoS.AT_LEAST_ONCE,
            )
            log.info(f"[SIM] #{contador[0]} Publicado: {payload['valor']}{payload['unidade']}")
        except Exception as erro:
            log.error(f"[SIM] Erro ao publicar: {erro}")

        time.sleep(CONFIG["intervalo_s"])


if __name__ == "__main__":
    main()
