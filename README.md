# 🪙 Crypto Data Pipeline & Analytics

![Python](https://img.shields.io/badge/python-3.11-blue.svg)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.0-017CEE?logo=apacheairflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-dashboard-FF4B4B?logo=streamlit&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)

> A production-style, end-to-end cryptocurrency data pipeline that extracts market data from CoinGecko and Binance, applies technical indicator transformations, stores results in a PostgreSQL data warehouse, and visualizes everything on a live Streamlit dashboard — all orchestrated by Apache Airflow and containerized with Docker Compose.

---

## 📸 Screenshots

### Airflow DAG — Graph View
![Airflow DAG Graph View](./images/dag.png)

### Streamlit Dashboard
![Streamlit Dashboard](./images/dash1.png)

---

## 📐 Architecture

```mermaid
flowchart LR
    CG[CoinGecko API] --> EP[extract_prices]
    BN[Binance API]   --> EO[extract_ohlcv]
    EP --> TL[transform_and_load]
    EO --> TL
    TL --> UM[update_metadata]
    UM --> FG[extract_fear_greed]
    TL --> DB[(PostgreSQL)]
    UM --> DB
    FG --> DB
    DB --> SD[Streamlit Dashboard]
```

**Data flow summary:**

1. **Extract** — Airflow triggers extractors that call the CoinGecko REST API (spot prices, coin metadata, Fear & Greed index) and the Binance public API (OHLCV candlestick data).
2. **Transform** — Raw DataFrames are processed with Pandas; the `ta` library computes technical indicators (RSI, MACD, Bollinger Bands, moving averages) in-memory.
3. **Load** — Enriched data is upserted into the PostgreSQL data warehouse using SQLAlchemy, with conflict resolution to keep runs idempotent.
4. **Visualize** — Streamlit reads directly from PostgreSQL and renders interactive Plotly charts on demand.

---

## ✨ Features

- **Automated Orchestration** — Apache Airflow DAG enforces task order and dependency, with built-in retry logic and task-level logging.
- **Dual Data Sources** — CoinGecko provides spot prices, coin metadata, and the Fear & Greed sentiment index; Binance supplies OHLCV candlestick data (no API key required for Binance).
- **Technical Indicator Enrichment** — The `ta` library automatically computes RSI, MACD, Bollinger Bands, and moving averages on every pipeline run.
- **Idempotent Upserts** — PostgreSQL upsert logic ensures reruns never produce duplicate rows.
- **Interactive Dashboard** — Streamlit + Plotly delivers live price history, candlestick charts, indicator overlays, and market sentiment in a single-page app.
- **Fully Dockerized** — One `docker compose up -d` launches Airflow (webserver + scheduler), PostgreSQL, and Streamlit with zero manual environment setup.
- **BI Tool Ready** — PostgreSQL is exposed on `localhost:5432` for direct connections from Power BI or Looker Studio.
- **Extensible Coin List** — Add new cryptocurrencies by editing two config dictionaries — no schema changes needed.

---

## 🗂️ Project Structure

```
crypto-pipeline/
├── dags/
│   └── crypto_pipeline.py        # Airflow DAG definition and task graph
├── extractors/
│   ├── coingecko.py              # CoinGecko API client (prices, metadata, Fear & Greed)
│   └── binance.py                # Binance public API client (OHLCV candlesticks)
├── transforms/
│   └── indicators.py             # Pandas + ta technical indicator transformations
├── scripts/
│   └── init_db.sql               # PostgreSQL schema + user bootstrap (auto-runs on first start)
├── dashboard/
│   └── app.py                    # Streamlit visualization application
├── docker-compose.yml            # Full infrastructure stack definition
├── requirements.txt              # Python dependencies
├── .gitignore
└── README.md
```

---

## 🔧 Prerequisites

- **Docker Desktop** (or Docker Engine + Docker Compose plugin) — [Install Docker](https://docs.docker.com/get-docker/)
- **8 GB RAM** allocated to the Docker Engine (Airflow requires significant memory)
- **Free CoinGecko API key** — [Register at CoinGecko](https://www.coingecko.com/en/api)

> Binance OHLCV data is fetched from the public REST endpoint and **does not require a Binance account or API key**.

---

## 🚀 Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/NadjibMoha/crypto-pipeline.git
cd crypto-pipeline
```

### 2. Configure environment variables

Create a `.env` file in the project root:

```bash
nano .env
```

Paste in the following, replacing `your_key_here` with your real CoinGecko API key:

```dotenv
COINGECKO_API_KEY=your_key_here
```

On Linux/macOS, also append the Airflow UID so Airflow can write to mounted volumes:

```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
```

### 3. Create required Airflow directories

```bash
mkdir -p ./dags ./logs ./plugins
```

### 4. Initialize Airflow

Run the one-time initialization that migrates the Airflow metadata database and creates the default admin user:

```bash
docker compose up airflow-init
```

Wait until this command exits cleanly (exit code `0`) before moving on.

### 5. Start the full stack

```bash
docker compose up -d
```

### 6. Verify all services are healthy

```bash
docker compose ps
```

All containers should report `healthy` or `running`. If any are still `starting`, wait 30–60 seconds and re-check.

---

## ▶️ Running the Pipeline

1. Open the **Airflow UI** → [http://localhost:8080](http://localhost:8080)
2. Log in: `admin` / `admin`
3. Find the `crypto_pipeline` DAG in the list
4. **Unpause** the DAG using the toggle on the left
5. Click **▶ Trigger DAG** (play button on the right) to run it immediately
6. Click the DAG name → **Graph** or **Grid** view to watch tasks execute
   - 🟩 Green = success
   - 🟥 Red = failed (click the task → **Log** to investigate)
7. Once all tasks are green, open the dashboard at [http://localhost:8501](http://localhost:8501) and click **Refresh Data** in the sidebar

---

## 🌐 Service Reference

| Service | Container | URL / Connection | Credentials |
|---|---|---|---|
| Airflow UI | `airflow-webserver` | http://localhost:8080 | `admin` / `admin` |
| Streamlit Dashboard | `dashboard` | http://localhost:8501 | — |
| PostgreSQL | `postgres` | `localhost:5432` | `crypto_user` / `crypto_pass` |

### Database connection details

| Parameter | Value |
|---|---|
| Host | `localhost` |
| Port | `5432` |
| Database | `crypto_db` |
| Username | `crypto_user` |
| Password | `crypto_pass` |

---

## ⚙️ Configuration Reference

### Environment Variables

| Variable | Default | Required | Description |
|---|---|---|---|
| `COINGECKO_API_KEY` | — | ✅ | CoinGecko API key for authenticated requests |
| `AIRFLOW_UID` | `1000` | Linux only | Host user UID for correct file permissions on mounted volumes |
| `POSTGRES_USER` | `airflow` | No | Postgres superuser for Airflow's own metadata database |
| `POSTGRES_PASSWORD` | `airflow` | No | Superuser password |
| `CRYPTO_DB_CONN` | `postgresql+psycopg2://crypto_user:crypto_pass@postgres:5432/crypto_db` | No | SQLAlchemy connection string for the crypto warehouse (set in compose) |

### Airflow Settings

| Setting | Value | Notes |
|---|---|---|
| Executor | `LocalExecutor` | Single-node mode (no Celery required) |
| Load examples | `false` | Example DAGs are disabled |
| Extra pip packages | `requests pandas psycopg2-binary sqlalchemy ta python-dotenv` | Installed at container startup via `_PIP_ADDITIONAL_REQUIREMENTS` |

---

## 🪙 Adding New Coins

To track a new cryptocurrency (e.g., Cardano `ADA`):

**Step 1** — Open `extractors/coingecko.py` and add the CoinGecko coin ID to `DEFAULT_COINS`:

```python
DEFAULT_COINS = ["bitcoin", "ethereum", "cardano"]
```

> Use the CoinGecko slug (lowercase), not the ticker. You can browse all IDs at https://api.coingecko.com/api/v3/coins/list

**Step 2** — Open `extractors/binance.py` and update both config structures:

```python
DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "ADAUSDT"]

SYMBOL_TO_COIN_ID = {
    "BTCUSDT": "bitcoin",
    "ETHUSDT": "ethereum",
    "ADAUSDT": "cardano",
}
```

**Step 3** — The next pipeline run will automatically fetch, transform, and upsert records for the new coin. No database schema changes are required.

---

## 📊 Connecting External BI Tools

The PostgreSQL data warehouse is exposed on `localhost:5432` and is compatible with any SQL-capable analytics tool.

### Power BI Desktop

1. Open Power BI → **Get Data** → **PostgreSQL database**
2. Server: `localhost:5432`
3. Database: `crypto_db`
4. Data Connectivity mode: **Import** (snapshot) or **DirectQuery** (live)
5. Username: `crypto_user` · Password: `crypto_pass`

### Looker Studio

> Looker Studio requires a publicly reachable database host. For local development, expose your PostgreSQL port via a tunnel (e.g., [ngrok](https://ngrok.com/) or [Cloudflare Tunnel](https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/)).

1. In Looker Studio: **Add data** → **PostgreSQL**
2. Hostname: your public IP or tunnel hostname
3. Port: `5432`
4. Database: `crypto_db`
5. Username: `crypto_user` · Password: `crypto_pass`

---

## 🐛 Troubleshooting

### `psycopg2.OperationalError: FATAL: password authentication failed for user "airflow"`

The `.env` file was updated after the PostgreSQL volume was already created. The persisted volume holds the old credentials. Fix by wiping the volume and reinitializing:

```bash
docker compose down -v    # removes all volumes — this deletes stored data
docker compose up airflow-init
docker compose up -d
```

---

### `FileNotFoundError: '/opt/airflow/extractors/coingecko.py'`

The local source directories are not being mounted into the Airflow containers. Verify:
- Your project structure matches the layout in [Project Structure](#️-project-structure)
- Docker Desktop has file sharing enabled for the project directory (Settings → Resources → File Sharing)

---

### `airflow-init` hangs or takes very long

The scheduler and webserver may be racing at startup. Wait 2–3 minutes. Check logs:

```bash
docker compose logs airflow-init
```

Also confirm Docker Engine is allocated at least **8 GB RAM** (Docker Desktop → Settings → Resources → Memory).

---

### Streamlit shows no data after the pipeline completes

1. Click **Refresh Data** in the Streamlit sidebar.
2. If still empty, confirm all Airflow tasks finished green.
3. Check individual task logs in Airflow for any database connection errors.
4. Verify PostgreSQL is reachable: `docker compose ps postgres`

---

### CoinGecko returns HTTP 401 or 429

- **401** — `COINGECKO_API_KEY` in `.env` is missing or incorrect.
- **429** — Free tier rate limit exceeded (~30 requests/min). Wait 60 seconds and re-trigger the DAG.

---


## 🙏 Acknowledgements

- [Apache Airflow](https://airflow.apache.org/) — workflow orchestration platform
- [CoinGecko API](https://www.coingecko.com/en/api) — free crypto market data
- [Binance API](https://binance-docs.github.io/apidocs/spot/en/) — public OHLCV candlestick endpoint
- [ta — Technical Analysis Library](https://github.com/bukosabino/ta) — financial indicators for Pandas
- [Streamlit](https://streamlit.io/) — Python-native data app framework
- [Plotly](https://plotly.com/python/) — interactive charting library

---

## 📄 License

This project is licensed under the MIT License. See [LICENSE](./LICENSE) for details.
