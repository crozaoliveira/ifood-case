# AGENTS.md

## Project Context

This repository implements an iFood technical case for Data Architecture / Data Engineering.
The goal is to ingest NYC TLC Yellow Taxi trip data for January through May 2023, store the
original files in a Data Lake landing zone, prepare a structured consumption layer, and answer
two analytical questions.

Primary case documents:

- `case/Case_iFood_-_Data_Architecture.pdf`: original case statement.
- `case/resumo_execucao_codex_ifood_case.md`: implementation plan and target architecture.

Treat the markdown as the detailed execution plan and the PDF as the source of the case
requirements. When the detailed plan conflicts with code that already exists in this repository,
prefer the code as the practical implementation reference unless the user asks for a migration.

## Expected Architecture

Use a simple Data Lake flow:

1. Landing: original NYC TLC parquet files, unchanged.
2. Bronze: raw structured data with technical metadata.
3. Silver / Consumption: typed, cleaned, partitioned dataset used by analysis.
4. Analysis: SQL or PySpark queries over the Silver layer.

Required consumption columns from the case:

- `VendorID`
- `passenger_count`
- `total_amount`
- `tpep_pickup_datetime`
- `tpep_dropoff_datetime`

Expected storage paths:

- `landing/yellow_taxi/<RUN_ID>/`
- `bronze/yellow_taxi/`
- `silver/fact_yellow_taxi_trips/`

The project supports both local storage and cloud through `STORAGE_MODE`.

## Data Source

NYC TLC files follow this URL pattern:

```txt
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet
```

The ingestion period must be generated dynamically from:

- `DATE_FROM=YYYY/MM`
- `DATE_TO=YYYY/MM`

Default case period: `2023/01` through `2023/05`, inclusive.

## Target Repository Structure

The intended structure is:

```txt
src/
  __init__.py
  config.py
  utils.py
  spark_session.py
  ingest_landing.py
  bronze.py
  silver.py
  run_pipeline.py
analysis/
  run_analysis.py
  01_avg_total_amount_month.sql
  02_avg_passenger_by_hour_may.sql
notebooks/
  01_ingestion.ipynb
  02_bronze.ipynb
  03_silver.ipynb
  04_analysis.ipynb
README.md
requirements.txt
```

The current repository is still early and may not match this structure yet. Prefer evolving it
toward the target structure instead of adding unrelated entry points.

## Coding Guidelines

- Keep business logic in Python modules under `src/`.
- Keep notebooks thin: they should import and reuse `src/` functions instead of duplicating logic.
- Use PySpark for Bronze, Silver, and/or analysis transformations.
- Keep ingestion idempotent: if a target landing file already exists, skip it.
- Preserve landing files exactly as downloaded.
- Add explicit casts and simple data quality rules in Silver.
- Prefer clear, small functions with script entry points guarded by `if __name__ == "__main__":`.
- Avoid committing secrets, generated data, virtual environments, logs, or notebook checkpoints.
- Do not hardcode AWS credentials or bucket secrets in code.

## Environment

Use `.env` for local configuration and `.env.example` as the documented template.

Expected variables:

```env
STORAGE_MODE=s3
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_REGION=us-east-2
S3_BUCKET=carlos-ifood-case-991416557162-us-east-2-an
S3_BASE_PREFIX=
LOCAL_BASE_PATH=data
DATE_FROM=2023/01
DATE_TO=2023/05
RUN_ID=
```

Use `RUN_ID` as the canonical execution identifier for this repository. If planning documents
mention `EXECUTION_ID`, interpret it as the same concept, but keep the code and environment
variables aligned with `RUN_ID`.

## Expected Commands

Install dependencies:

```bash
pip install -r requirements.txt
```

Run steps individually:

```bash
python src/ingest_landing.py
python src/bronze.py
python src/silver.py
python analysis/run_analysis.py
```

Run the full pipeline:

```bash
python src/run_pipeline.py
```

If module imports are sensitive to execution context, prefer fixing imports/package structure
rather than relying on ad hoc path manipulation.

## Analytical Questions

Query 1: average `total_amount` by month for all yellow taxis.

Expected SQL shape:

```sql
SELECT
    pickup_year,
    pickup_month,
    ROUND(AVG(total_amount), 2) AS avg_total_amount
FROM fact_yellow_taxi_trips
GROUP BY pickup_year, pickup_month
ORDER BY pickup_year, pickup_month;
```

Query 2: average `passenger_count` by pickup hour for May 2023.

Expected SQL shape:

```sql
SELECT
    pickup_hour,
    ROUND(AVG(passenger_count), 2) AS avg_passenger_count
FROM fact_yellow_taxi_trips
WHERE pickup_year = 2023
  AND pickup_month = 5
GROUP BY pickup_hour
ORDER BY pickup_hour;
```

## Validation Expectations

Before considering major changes complete:

- Run focused checks for changed modules when feasible.
- Validate date range generation, including cross-year ranges and invalid inputs.
- Confirm ingestion path construction matches the expected landing layout.
- Confirm Silver contains the required columns and partitions by `pickup_year` and `pickup_month`.
- Update README instructions when commands, structure, or assumptions change.

## Current Notes

- `README.md` is minimal and should be expanded before final delivery.
- Existing source files are partial and currently use names like `RUN_ID` and `src/utils/main.py`.
- Preserve established code choices such as `RUN_ID` when continuing the implementation.
- The final implementation should align structure and behavior with the target architecture while
  respecting existing code decisions unless the user explicitly requests a refactor.
