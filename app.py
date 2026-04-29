# app.py — Aplicação principal: conecta ao IoT Core (MQTT) e salva dados no RDS (MySQL)

import os
import json
import time
import logging
import signal
import sys
from dotenv import load_dotenv
from awscrt import mqtt, io
from awsiot import mqtt_connection_builder

from db import inicializar_banco, inserir_leitura

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("app")

# ─── Configurações via variáveis de ambiente ──────────────────────────────────

CONFIG = {
    "endpoint": os.getenv("IOT_ENDPOINT"),
    "cert_path": os.getenv("IOT_CERT_PATH"),
    "key_path": os.getenv("IOT_KEY_PATH"),
    "ca_path": os.getenv("IOT_CA_PATH"),
    "client_id": os.getenv("IOT_CLIENT_ID", f"ec2-python-{int(time.time())}"),
    "topico": os.getenv("IOT_TOPIC", "sensores/#"),
}


# ─── Callbacks MQTT ───────────────────────────────────────────────────────────

def on_connection_interrupted(connection, error, **kwargs):
    log.warning(f"[MQTT] Conexão interrompida: {error}")


def on_connection_resumed(connection, return_code, session_present, **kwargs):
    log.info(f"[MQTT] Conexão retomada. return_code={return_code}")


def on_message_received(topic, payload, **kwargs):
    """Chamado sempre que uma mensagem chega em um tópico assinado."""
    mensagem_raw = payload.decode("utf-8")
    log.info(f"[MQTT] Mensagem recebida no tópico '{topic}':")
    log.info(f"[MQTT] Payload: {mensagem_raw}")

    try:
        dados = json.loads(mensagem_raw)

        # Validação básica
        if dados.get("valor") is None and dados.get("value") is None:
            log.warning("[APP] Mensagem sem campo 'valor'. Ignorando.")
            return

        # Normalizar campos (aceita nomes em pt-BR ou en)
        dados_normalizados = {
            "sensor_id": dados.get("sensor_id")
            or dados.get("sensorId")
            or dados.get("id")
            or "desconhecido",
            "tipo": dados.get("tipo") or dados.get("type"),
            "valor": dados.get("valor") if dados.get("valor") is not None else dados.get("value"),
            "unidade": dados.get("unidade") or dados.get("unit"),
            "timestamp": dados.get("timestamp") or int(time.time() * 1000),
        }

        inserir_leitura(dados_normalizados, topic)
        log.info("[APP] Dados salvos no banco com sucesso.")
    except json.JSONDecodeError:
        log.error("[APP] Mensagem não é um JSON válido.")
    except Exception as erro:
        log.error(f"[APP] Erro ao processar mensagem: {erro}")


# ─── Função principal ─────────────────────────────────────────────────────────

def main():
    # 1. Inicializar o banco
    inicializar_banco()

    # 2. Configurar a conexão MQTT com o IoT Core
    log.info("[MQTT] Conectando ao IoT Core...")
    log.info(f"[MQTT] Endpoint: {CONFIG['endpoint']}")
    log.info(f"[MQTT] Client ID: {CONFIG['client_id']}")
    log.info(f"[MQTT] Tópico: {CONFIG['topico']}")

    mqtt_connection = mqtt_connection_builder.mtls_from_path(
        endpoint=CONFIG["endpoint"],
        cert_filepath=CONFIG["cert_path"],
        pri_key_filepath=CONFIG["key_path"],
        ca_filepath=CONFIG["ca_path"],
        client_id=CONFIG["client_id"],
        clean_session=False,
        keep_alive_secs=30,
        on_connection_interrupted=on_connection_interrupted,
        on_connection_resumed=on_connection_resumed,
    )

    # 3. Conectar
    connect_future = mqtt_connection.connect()
    connect_future.result()  # aguarda conclusão
    log.info("[MQTT] Conexão estabelecida com sucesso!")

    # 4. Inscrever no tópico
    subscribe_future, _ = mqtt_connection.subscribe(
        topic=CONFIG["topico"],
        qos=mqtt.QoS.AT_LEAST_ONCE,
        callback=on_message_received,
    )
    subscribe_future.result()
    log.info(f"[MQTT] Inscrito no tópico: {CONFIG['topico']}")
    log.info("[APP] Aguardando mensagens MQTT...")

    # 5. Encerramento gracioso
    def encerrar(signum, frame):
        log.info("\n[APP] Encerrando aplicação...")
        try:
            mqtt_connection.disconnect().result()
        except Exception:
            pass
        sys.exit(0)

    signal.signal(signal.SIGINT, encerrar)
    signal.signal(signal.SIGTERM, encerrar)

    # 6. Manter o processo ativo
    while True:
        time.sleep(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as erro:
        log.error(f"[APP] Erro fatal: {erro}")
        sys.exit(1)
