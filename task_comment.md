GitHub Repository Link: <https://github.com/suhasi-gadge/Data-Ingestion-and-Validation-Workflow-Using-Apache-Airflow>

Implemented an Apache Airflow DAG named `public_financial_data_ingestion_validation` that ingests public daily OHLCV stock price data from Stooq, stages it locally as CSV, validates the dataset, and loads valid records into a SQLite target table.

Repository contents include:
- Airflow DAG script: `dags/financial_data_ingestion_dag.py`
- Reusable ingestion, validation, transformation, and SQLite loading utilities: `src/finance_pipeline.py`
- Docker Compose setup for local Airflow execution: `docker-compose.yml`
- Python dependencies: `requirements.txt`
- Unit tests for validation pass/fail behavior: `tests/test_validation.py`
- README with setup, execution, validation-failure test instructions, and Airflow UI screenshot section

Validation checks include required column checks, missing-value checks, date parsing, duplicate-date detection, numeric type checks, positive price checks, non-negative volume checks, OHLC consistency checks, and minimum row-count checks. The DAG fails during the validation task when invalid data is simulated using `--conf '{"simulate_invalid": true}'`.
