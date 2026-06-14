# SQL Data Warehouse

A layered data warehouse built on PostgreSQL following the medallion architecture. Six heterogeneous CSV sources are ingested into a Bronze staging layer, cleansed and normalised in Silver, then modelled into a star schema in Gold — producing analytics-ready fact and dimension tables suited for direct BI consumption.

---

## Architecture

```
6× CSV Sources
      │
      ▼  proc_load_bronze.sql    (raw ingestion, no transformation)
Bronze Layer   (staging tables, exact source representation)
      │
      ▼  proc_load_silver.sql    (type casting, deduplication, NULL handling,
      │                           standardisation, surrogate key assignment)
Silver Layer   (cleansed, conformed, normalised)
      │
      ▼  ddl_gold.sql            (views over Silver)
Gold Layer     (star schema — fact + dimension tables, analytics-ready)
      │
      ▼  SQL reports / BI tools
```

All DDL and ETL logic is implemented as SQL stored procedures, keeping the entire pipeline database-native with no external orchestration dependency.

---

## Core Technical Stack

PostgreSQL, Docker, SQL (DDL + stored procedures), Medallion Architecture, Star Schema

---

## Key Methodologies

- **Medallion layering** — Bronze preserves source data verbatim, making re-processing safe. Silver applies all transformation logic in one place (type casting, deduplication, NULL coalescing, standardisation), so downstream layers are never affected by upstream schema drift without an explicit Silver migration.

- **Stored-procedure ETL** — each layer transition is encapsulated in a stored procedure (`proc_load_bronze`, `proc_load_silver`). This keeps the pipeline declarative, transactional, and testable without an orchestration tool.

- **Star schema in Gold** — fact tables hold measurable events with foreign keys to dimension tables, optimising query performance for aggregations and slice-and-dice analytics without needing ORM or application-layer joins.

- **Surrogate keys** — Silver assigns integer surrogate keys independently of source natural keys, decoupling the warehouse from upstream key changes.

---

## Production Metrics & Validation

- Six independent source systems integrated into a single conformed model.
- Query patterns validated against the Gold layer using the SQL analytics scripts in `docs/`.
- Data catalog (`docs/data_catalog.md`) documents every table, column, and transformation decision.

---

## Local Replication

Prerequisites: Docker, psql (or any PostgreSQL client).

```bash
git clone https://github.com/tharrmeehan/SQL-Data-Warehouse-Project.git
cd SQL-Data-Warehouse-Project

# Spin up PostgreSQL
docker run --name dwh -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:15

# Initialise schema
psql -h localhost -U postgres -f scripts/init_database.sql

# Load layers in order
psql -h localhost -U postgres -d datawarehouse -f scripts/bronze/ddl_bronze.sql
psql -h localhost -U postgres -d datawarehouse -f scripts/bronze/proc_load_bronze.sql
psql -h localhost -U postgres -d datawarehouse -f scripts/silver/ddl_silver.sql
psql -h localhost -U postgres -d datawarehouse -f scripts/silver/proc_load_silver.sql
psql -h localhost -U postgres -d datawarehouse -f scripts/gold/ddl_gold.sql
```

---

## Project Structure

```
├── scripts/
│   ├── init_database.sql
│   ├── bronze/
│   │   ├── ddl_bronze.sql
│   │   └── proc_load_bronze.sql
│   ├── silver/
│   │   ├── ddl_silver.sql
│   │   └── proc_load_silver.sql
│   └── gold/
│       └── ddl_gold.sql
├── datasets/              # Source CSV files
├── docs/
│   ├── data_catalog.md
│   ├── data_architecture.png
│   ├── data_flow_diagram.drawio
│   ├── data_model.drawio
│   └── integration_model.drawio
└── tests/
```
