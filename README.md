# Guia Completo (Python): AWS IoT Core (MQTT) + EC2 + RDS (MySQL)

## Visão Geral da Arquitetura

```
Dispositivo IoT ──► AWS IoT Core (Broker MQTT) ──► EC2 (Python) ──► RDS (MySQL)
```

---

## PARTE 1 — Criar o Banco de Dados MySQL na Nuvem (RDS)
## PARTE 2 — Criar a Máquina Virtual (EC2)
## PARTE 3 — Criar o Broker MQTT (AWS IoT Core)
## PARTE 4 — Transferir os Certificados para a EC2
## PARTE 5 — Criar a Aplicação Python na EC2
## PARTE 6 — Executar a Aplicação
## Estrutura Final dos Arquivos

```
~/app-iot/
├── .env                  ← configurações (NÃO versionar)
├── requirements.txt      ← dependências Python
├── db.py                 ← módulo de conexão MySQL
├── app.py                ← aplicação principal (subscriber MQTT)
├── simular_sensor.py     ← simulador de dispositivo IoT
└── venv/                 ← ambiente virtual Python

~/certs/
├── certificate.pem.crt
├── private.pem.key
└── AmazonRootCA1.pem
```
