# Geopolitical Pulse

An enterprise-grade data engineering pipeline designed to monitor and visualize Chinese economic volatility and supply chain disruption in real-time.

## 🚀 The Mission
In a globalized economy, "noise" is everywhere. This project transforms the **GDELT 2.0 Global Firehose** (a 15-minute telemetry stream of global events) into a clean, queryable dashboard. It demonstrates the ability to handle massive, messy datasets using high-performance streaming and modern data modeling.

## 📂 The Architectural Hierarchy
```
geopolitical-pulse/
├── ingestion/                 # 📡 The "Pulse" (Data Acquisition)
│   ├── src/                   # Polars-backed producer logic
│   └── requirements.txt       # Dependencies: httpx, polars, confluent-kafka
│
├── warehouse/                 # 🧠 The "Brain" (Data Storage & Modeling)
│   ├── dbt/                   # dbt Core project for volatility modeling
│   └── sql/                   # Initial DDL and Postgres schema definitions
│
├── frontend/                  # 🎨 The "Art" (Laravel Dashboard)
│   └── [Standard Laravel scaffold]
│
├── infrastructure/            # ⚙️ The "Engine" (Environment Configs)
│   └── docker-compose.yml     # Services: Redpanda, Postgres, pgAdmin
│
├── .gitignore                 # 🔒 High-security (exclude .env, logs, pyc)
└── README.md                  # 📝 Executive summary & technical docs
```

## 🏗️ Technical Architecture
This system is built for performance and scalability, running on a native Linux layer via **WSL2 (Ubuntu)**.

| Layer | Component | Reason |
| :--- | :--- | :--- |
| **Ingestion** | Python + **Polars** | Multi-threaded, Rust-backed processing that outperforms Pandas for large GDELT TSVs. |
| **Streaming** | **Redpanda** | A Kafka-compatible, lightweight streaming platform that runs without Zookeeper bloat. |
| **Warehouse** | **PostgreSQL 15** | Industry-standard relational storage for structured event data. |
| **Modeling** | **dbt (Core)** | Transforming raw "event noise" into business-ready rolling 7-day volatility scores. |
| **Frontend** | **Laravel** | A high-performance PHP framework delivering a sleek, "Art" dashboard for stakeholders. |

## 🤖 The AI "Workforce" Strategy
This project follows a **Bring Your Own Key (BYOK)** model, achieving an enterprise-level development environment with **$0 monthly overhead**:
* **The Architect (Aider/Kimi 2.5):** Deep reasoning for system scaffolding.
* **The Typist (Llama 3/OpenRouter):** Sub-second latency for inline boilerplate.
* **The Bridge (Continue.dev):** Orchestrating the AI models within VS Code.

## 📊 GDELT Filter Logic
We specifically target the Chinese market via the following telemetry filters:
- **ActionGeo_CountryCode:** `CH`
- **Targeted CAMEO Codes:**
    - `03`: Express Intent (Trade deals and cooperation)
    - `04`: Demand (Economic disputes)
    - `10`: Sanctions (Economic protests and official demands)
- **Sentiment Metric:** Monitoring the **GoldsteinScale** to measure event impact.

---
*Created as a demonstration of modern data engineering and AI-assisted development.*
