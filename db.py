# db.py — Módulo de conexão e operações com o banco MySQL (RDS)

import os
import json
import logging
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import pooling

load_dotenv()

log = logging.getLogger("db")

_pool = None


def criar_pool():
    """Cria o pool de conexões com o MySQL."""
    global _pool
    _pool = pooling.MySQLConnectionPool(
        pool_name="iot_pool",
        pool_size=5,
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", 3306)),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
        autocommit=True,
    )
    log.info("[DB] Pool de conexões criado.")
    return _pool


def _get_conexao():
    if _pool is None:
        criar_pool()
    return _pool.get_connection()


def inicializar_banco():
    """Testa a conexão e cria a tabela se não existir."""
    if _pool is None:
        criar_pool()

    try:
        conexao = _get_conexao()
        cursor = conexao.cursor()
        log.info("[DB] Conexão com MySQL estabelecida com sucesso.")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS leituras_sensores (
                id INT AUTO_INCREMENT PRIMARY KEY,
                sensor_id VARCHAR(100) NOT NULL,
                tipo VARCHAR(50),
                valor DOUBLE NOT NULL,
                unidade VARCHAR(20),
                timestamp_sensor BIGINT,
                recebido_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                topico VARCHAR(255),
                payload_raw JSON
            )
        """)
        cursor.close()
        conexao.close()
        log.info("[DB] Tabela 'leituras_sensores' verificada/criada.")
    except Exception as erro:
        log.error(f"[DB] Erro ao inicializar o banco: {erro}")
        raise


def inserir_leitura(dados: dict, topico: str) -> int:
    """Insere uma leitura de sensor no banco.

    Args:
        dados: dicionário com os campos do sensor.
        topico: tópico MQTT de origem.

    Returns:
        O ID gerado pelo INSERT.
    """
    if _pool is None:
        criar_pool()

    sql = """
        INSERT INTO leituras_sensores
            (sensor_id, tipo, valor, unidade, timestamp_sensor, topico, payload_raw)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    valores = (
        dados.get("sensor_id", "desconhecido"),
        dados.get("tipo"),
        dados.get("valor"),
        dados.get("unidade"),
        dados.get("timestamp"),
        topico,
        json.dumps(dados),
    )

    conexao = _get_conexao()
    cursor = conexao.cursor()
    try:
        cursor.execute(sql, valores)
        last_id = cursor.lastrowid
        log.info(f"[DB] Leitura inserida com ID: {last_id}")
        return last_id
    except Exception as erro:
        log.error(f"[DB] Erro ao inserir leitura: {erro}")
        raise
    finally:
        cursor.close()
        conexao.close()
