# multisource-etl-airflow

An end-to-end ETL pipeline built with Apache Airflow that ingests data from two public APIs (COVID-19 statistics and historical weather), lands it in PostgreSQL, joins both sources into a fact table, trains a small linear-regression model on the result, and surfaces everything through Apache Superset dashboards.

Built as a portfolio project during a data-engineering internship at Amdaris.

## What it demonstrates

- **Multi-source ingestion** — two independent ETL DAGs (`covid`, `weather`) sharing a common extract/transform/load pattern via custom Airflow hooks and operators.
- **Warehouse modelling** — region dimension plus a `dw_covid_weather_fact` table joining both sources by date and country.
- **Operational logging** — every API call, file import, and transform step writes to dedicated audit tables (`log_api_import`, `log_import`, `log_transform`).
- **ML on top of the warehouse** — a daily DAG trains a `LinearRegression`, persists predictions and coefficients, and exposes them to Superset.
- **Reusable plugin layer** — custom `DbInsertOperator` (parameterised upsert with `ON CONFLICT`), `CovidApiHook`, `WeatherApiHook`, plus shared helpers for DB access and logging.

## Stack

Apache Airflow 2.10.5 (CeleryExecutor) · PostgreSQL 13 · Redis 7.2 · Apache Superset · scikit-learn · pandas · Docker Compose.

## Architecture

```
                    ┌───────────┐
                    │  regions  │  seeds dbo.regions + log tables
                    └─────┬─────┘
                          │
                ┌─────────┴──────────┐
                ▼                    ▼
          ┌──────────┐         ┌──────────┐
          │  covid   │         │ weather  │  ETL: API → JSON → Postgres
          └────┬─────┘         └────┬─────┘
               │                    │
               └─────────┬──────────┘
                         ▼
              dbo.dw_covid_weather_fact
                         │
                         ▼
                    ┌─────────┐
                    │   ml    │  LinearRegression → predictions + coefficients
                    └─────────┘
                         │
                         ▼
                     Superset
```

Each ETL DAG follows the same shape: `create_table → extract (API → raw JSON) → transform (JSON → records, file moved to success/error) → DbInsertOperator (upsert into dbo.*)`.

## Prerequisites

- [Docker](https://docs.docker.com/get-started/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/)
- A free [WeatherAPI](https://www.weatherapi.com/) key (only needed for the `weather` DAG)

## Getting started

### 1. Initialise the environment

```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker compose up airflow-init
```

This runs database migrations and creates the default `airflow` / `airflow` admin account.

### 2. Start the stack

```bash
docker compose up
```

Once everything is healthy:

| Service        | URL                     | Credentials         |
| -------------- | ----------------------- | ------------------- |
| Airflow UI     | http://localhost:8080   | `airflow` / `airflow` |
| Superset UI    | http://localhost:8091   | `airflow` / `airflow` |

### 3. Create the Postgres connection

In the Airflow UI, open `Admin → Connections → +` and add:

| Field           | Value      |
| --------------- | ---------- |
| Connection Id   | `pg_conn`  |
| Connection Type | `postgres` |
| Host            | `postgres` |
| Schema          | `airflow`  |
| Login           | `airflow`  |
| Password        | `airflow`  |
| Port            | `5432`     |

> All DAGs reference `pg_conn` directly — without it the tasks will fail.

### 4. Configure Airflow Variables

Under `Admin → Variables`:

| Key                | Purpose                                              |
| ------------------ | ---------------------------------------------------- |
| `WEATHER_API_KEY`  | Your WeatherAPI key (required by the `weather` DAG)  |
| `RUN_HISTORICAL`   | `"true"` to backfill from earlier `start_date`       |

### 5. Trigger the DAGs

Run them in this order the first time:

1. `regions` — populates the country dimension and log tables.
2. `covid` and `weather` — can run in parallel afterwards.
3. `ml` — needs `dw_covid_weather_fact` populated by the `weather` DAG.

## Running CLI commands

Either through Docker Compose:

```bash
docker compose run airflow-worker airflow info
```

…or via the helper script, which proxies to the `airflow-cli` service:

```bash
./airflow.sh info
./airflow.sh bash
./airflow.sh python
```

## Tests and lint

```bash
./airflow.sh bash -c "cd /opt/airflow && pytest tests/unit"
ruff check .
```

DAG-load tests live in `tests/unit/dags/`; plugin unit tests in `tests/unit/plugins/`.

## Connecting Superset to Postgres

In Superset, go to `Data → Databases → + Database → PostgreSQL` and use:

```
postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
```

After connecting, the most useful tables for dashboards are:

- `dbo.covid` and `dbo.weather` — raw ETL output
- `dbo.dw_covid_weather_fact` — joined warehouse fact
- `dbo.visualization_covid_weather_fact` — model predictions vs actuals
- `dbo.visualization_coefficients` — feature coefficients of the latest trained model

## Repository layout

```
dags/             # Airflow DAGs: regions, covid, weather, ml
plugins/
  hooks/          # CovidApiHook, WeatherApiHook
  operators/      # DbInsertOperator (parameterised upsert)
  helpers/        # db, logs, weather utilities
sql/              # CREATE TABLE and warehouse-load SQL
tests/unit/       # pytest unit tests for DAGs and plugins
config/           # YAML config (legacy — runtime reads Airflow Variables)
dashboards/       # Superset dashboard exports
```
