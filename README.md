# Crypto Data Pipeline & Analytics 🚀

A production-style end-to-end data pipeline built to extract, transform, and visualize cryptocurrency market data securely and consistently using industry-standard tools.

## 📌 Architecture Stack
- **Orchestration**: Apache Airflow 2.9 (Docker)
- **Database**: PostgreSQL 15 Data Warehouse
- **Transformations**: Pandas + Technical Indicators Library (ta) + SQL
- **Extractors**: CoinGecko (Prices, Metadata, Fear & Greed) & Binance (OHLCV Candlesticks)
- **Dashboard**: Streamlit (with Plotly)
- **Infrastructure**: Docker Compose

---

## 📋 1. Prerequisites
- Docker & Docker Compose installed
- At least **8GB RAM** available for the Docker Engine
- A **Free CoinGecko API Key** (Get one at [CoinGecko API](https://www.coingecko.com/en/api))

---

## 🚀 2. Quickstart

Run these 5 commands to go from zero to a fully running pipeline:

```bash
# 1. Update your CoinGecko API Key in the .env file
nano .env

# 2. Set the correct permissions for the Airflow user
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" >> .env

# 3. Initialize the Airflow Environment & Database
docker compose up airflow-init

# 4. Start the Full Stack (Airflow, Postgres, Streamlit)
docker compose up -d

# 5. Verify the services are healthy
docker compose ps
```

---

## ⚡ 3. How to Trigger the DAG and Verify
1. Open up the Airflow UI at `http://localhost:8080`.
2. Login with credentials: `admin` / `admin`.
3. Locate the `crypto_pipeline` DAG in the list and toggle it **On** (Unpause).
4. Click the **Play button** (▶️) on the right side to trigger it manually.
5. Click on the DAG name and go to the **Graph** or **Grid** view to watch the tasks run in sequence. Green borders mean Success!
6. Open the Streamlit Dashboard at `http://localhost:8501` and hit **Refresh Data** in the sidebar to see your newly processed data.

---

## 🌐 4. Service URLs

| Service             | Container Name      | Local URL / Connection | Credentials |
|---------------------|---------------------|------------------------|-------------|
| **Airflow UI**      | `airflow-webserver` | [http://localhost:8080](http://localhost:8080) | `admin` / `admin` |
| **Dashboard**       | `dashboard`         | [http://localhost:8501](http://localhost:8501) | N/A |
| **PostgreSQL DB**   | `postgres`          | `localhost:5432`       | `crypto_user` / `crypto_pass` |

---

## 📊 5. Connect to BI Tools (Power BI / Looker Studio)

You can bypass Streamlit and connect professional BI tools directly to the PostgreSQL Data Warehouse.

### Connecting Power BI Desktop
1. Open Power BI -> Get Data -> PostgreSQL database
2. Server: `localhost:5432`
3. Database: `crypto_db`
4. Data Connectivity mode: **Import** or **DirectQuery**
5. Credentials: username `crypto_user`, password `crypto_pass`

### Connecting Looker Studio
*(Note: requires deploying the Postgres database with a public IP or using a cloud connector like ngrok for local development)*
1. Add Data -> Select **PostgreSQL**
2. Hostname: `Your_Public_IP`
3. Port: `5432`
4. Database: `crypto_db`
5. Username: `crypto_user`, Password: `crypto_pass`

---

## 📂 6. Folder Structure

```text
crypto-pipeline/
├── dags/                  # Airflow DAGs (crypto_pipeline.py)
├── extractors/            # API integration scripts (coingecko.py, binance.py)
├── transforms/            # Pandas logic for technical indicators
├── scripts/               # SQL initialization scripts (init_db.sql)
├── dashboard/             # Streamlit visualization app
├── docker-compose.yml     # Infrastructure stack orchestration
├── requirements.txt       # Python dependencies
└── .env                   # Secrets and environment variables
```

---

## 🪙 7. How to Add New Coins

To track a new cryptocurrency (e.g., Cardano `ADA` or Dogecoin `DOGE`):
1. Open `extractors/coingecko.py` and modify `DEFAULT_COINS` (use the CoinGecko ID, e.g., `"cardano"`).
2. Open `extractors/binance.py`:
   - Add the Binance symbol to `DEFAULT_SYMBOLS` (e.g., `"ADAUSDT"`).
   - Add the mapping to `SYMBOL_TO_COIN_ID` (e.g., `"ADAUSDT": "cardano"`).
3. The next pipeline run will automatically adjust, fetch, and upsert records for the new coins! 

---

## 🐛 8. Troubleshooting

**Error:** `psycopg2.OperationalError: FATAL: password authentication failed for user "airflow"`
**Fix:** The `.env` file does not match or the Postgres container was created before the env file was updated. Run `docker compose down -v` to delete the volume, and run step 3 again.

**Error:** `FileNotFoundError: [Errno 2] No such file or directory: '/opt/airflow/extractors/coingecko.py'`
**Fix:** Ensure your local folders `extractors`, `transforms`, and `scripts` are structured exactly as shown in the folder section, and are mounted properly in `docker-compose.yml`. 

**Error:** Docker Compose hangs on `airflow-init`
**Fix:** The scheduler and webserver might be trying to connect simultaneously. Wait 1-2 minutes or check the logs using `docker compose logs airflow-init`. Ensure you gave Docker enough RAM (8GB+).
