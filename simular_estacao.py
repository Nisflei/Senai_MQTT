# simular_estacao.py — Simulador de Estação Meteorológica Virtual
#
# Diferente do simulador básico (que gera valores aleatórios), este simula
# uma estação meteorológica REAL com múltiplos sensores correlacionados:
#
#   • Temperatura: varia conforme o horário do dia (curva senoidal)
#   • Umidade:     inversamente proporcional à temperatura + ruído
#   • Pressão:     pequena variação em torno de 1013 hPa
#
# Além disso, a cada ~20 leituras ocorre uma ANOMALIA simulada
# (pico ou queda abrupta), útil para treinar sistemas de alerta.
#
# Publica em TÓPICOS SEPARADOS para cada tipo de sensor:
#   estacao/sala-01/temperatura
#   estacao/sala-01/umidade
#   estacao/sala-01/pressao

import os
import json
import time
import math
import random
import logging
import signal
import sys
from datetime import datetime
from dotenv import load_dotenv
from awscrt import mqtt
from awsiot import mqtt_connection_builder

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("estacao")

CONFIG = {
    "endpoint": os.getenv("IOT_ENDPOINT"),
    "cert_path": os.getenv("IOT_CERT_PATH"),
    "key_path": os.getenv("IOT_KEY_PATH"),
    "ca_path": os.getenv("IOT_CA_PATH"),
    "client_id": f"estacao-meteo-{int(time.time())}",
    "base_topico": os.getenv("IOT_BASE_TOPIC", "estacao/sala-01"),
    "estacao_id": "estacao-meteo-01",
    "intervalo_s": 5,
    # Probabilidade (0 a 1) de ocorrer uma anomalia a cada ciclo
    "probabilidade_anomalia": 0.05,
}


# ─── Geradores realistas ─────────────────────────────────────────────────────

def gerar_temperatura_realista() -> float:
    """Simula curva diária de temperatura (mínima ~5h, máxima ~15h).

    Fórmula: usa seno deslocado para representar o ciclo de 24h.
    Temperatura base: 22°C, amplitude: 6°C, + ruído aleatório.
    """
    agora = datetime.now()
    hora_decimal = agora.hour + agora.minute / 60.0

    # Seno com período de 24h, mínimo às 5h, máximo às 15h
    ciclo = math.sin((hora_decimal - 9) * math.pi / 12)
    temperatura = 22 + 6 * ciclo + random.uniform(-0.5, 0.5)
    return round(temperatura, 2)


def gerar_umidade_realista(temperatura: float) -> float:
    """Umidade é inversamente correlacionada com a temperatura."""
    # Quanto mais quente, menos úmido (aproximação simples)
    base = 90 - (temperatura - 15) * 2
    umidade = max(30, min(95, base + random.uniform(-3, 3)))
    return round(umidade, 2)


def gerar_pressao_realista() -> float:
    """Pressão atmosférica varia pouco, em torno de 1013 hPa."""
    return round(1013 + random.uniform(-5, 5), 2)


def aplicar_anomalia(tipo: str, valor: float) -> tuple[float, bool]:
    """Retorna (valor_modificado, houve_anomalia)."""
    if random.random() > CONFIG["probabilidade_anomalia"]:
        return valor, False

    if tipo == "temperatura":
        return round(valor + random.choice([-15, 20]), 2), True  # pico/queda
    if tipo == "umidade":
        return round(max(0, min(100, valor + random.choice([-40, 30]))), 2), True
    if tipo == "pressao":
        return round(valor + random.choice([-25, 25]), 2), True

    return valor, False


# ─── Publicação ──────────────────────────────────────────────────────────────

def publicar(mqtt_connection, tipo: str, valor: float, unidade: str, anomalia: bool):
    topico = f"{CONFIG['base_topico']}/{tipo}"
    payload = {
        "sensor_id": f"{CONFIG['estacao_id']}-{tipo}",
        "tipo": tipo,
        "valor": valor,
        "unidade": unidade,
        "timestamp": int(time.time() * 1000),
        "estacao": CONFIG["estacao_id"],
        "anomalia": anomalia,
    }

    mqtt_connection.publish(
        topic=topico,
        payload=json.dumps(payload),
        qos=mqtt.QoS.AT_LEAST_ONCE,
    )

    marcador = " ⚠️ ANOMALIA" if anomalia else ""
    log.info(f"[PUB] {topico:40s} → {valor}{unidade}{marcador}")


# ─── Loop principal ──────────────────────────────────────────────────────────

def main():
    log.info("[EST] Iniciando estação meteorológica virtual...")
    log.info(f"[EST] Base do tópico: {CONFIG['base_topico']}")
    log.info(f"[EST] Intervalo: {CONFIG['intervalo_s']}s")
    log.info(f"[EST] Prob. anomalia: {CONFIG['probabilidade_anomalia']*100:.0f}%\n")

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
    log.info("[EST] Conectada ao IoT Core!\n")

    contador = {"total": 0, "anomalias": 0}

    def encerrar(signum, frame):
        log.info(f"\n[EST] Encerrando... Leituras: {contador['total']} | Anomalias: {contador['anomalias']}")
        try:
            mqtt_connection.disconnect().result()
        except Exception:
            pass
        sys.exit(0)

    signal.signal(signal.SIGINT, encerrar)
    signal.signal(signal.SIGTERM, encerrar)

    while True:
        # 1. Gera valores correlacionados
        temp = gerar_temperatura_realista()
        umid = gerar_umidade_realista(temp)
        pres = gerar_pressao_realista()

        # 2. Eventualmente injeta anomalias
        temp_f, a1 = aplicar_anomalia("temperatura", temp)
        umid_f, a2 = aplicar_anomalia("umidade", umid)
        pres_f, a3 = aplicar_anomalia("pressao", pres)

        # 3. Publica nos 3 tópicos
        log.info(f"─── Ciclo #{contador['total'] + 1} ───")
        publicar(mqtt_connection, "temperatura", temp_f, "°C", a1)
        publicar(mqtt_connection, "umidade", umid_f, "%", a2)
        publicar(mqtt_connection, "pressao", pres_f, "hPa", a3)

        contador["total"] += 1
        contador["anomalias"] += sum([a1, a2, a3])

        time.sleep(CONFIG["intervalo_s"])


if __name__ == "__main__":
    main()
