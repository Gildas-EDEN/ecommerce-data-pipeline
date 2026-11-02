# Automated Data Pipeline with Spark, dbt, and Airflow

This project is an automated ETL pipeline designed to replace a
company's daily manual data processing. It consolidates three
heterogeneous sources: a customer database, an orders API, and a product
catalog file. The pipeline integrates them into a consistent data model
ready for analysis.

------------------------------------------------------------------------

## What the Pipeline Does

``` mermaid
flowchart LR
    subgraph S[Sources]
        DB[(PostgreSQL<br/>customers table)]
        API[REST API<br/>orders]
        CSV[/CSV file<br/>product catalog/]
    end

    subgraph P[Python Pipeline Orchestrated by Airflow]
        E[1. Incremental<br/>extraction]
        V{2. Data Quality<br/>Gate}
        SC[3a. Customer<br/>SCD2]
        SP[3b. Aggregations<br/>with Spark]
        L[4. Atomic<br/>loading]
    end

    subgraph W[PostgreSQL Data Warehouse]
        T[(dim_customers<br/>fact_daily_orders<br/>fact_product_performance<br/>raw_*)]
        M[(dbt Marts<br/>sales by segment<br/>product performance)]
    end

    DB --> E
    API --> E
    CSV --> E
    E --> V
    V --> SC --> L
    V --> SP --> L
    L --> T -->|dbt run + dbt test| M
```

1.  **Incremental extraction** from the three sources in parallel: only
    records created or modified since the previous run are read
    (watermarks).
2.  **Data quality gate:** checks for missing values, data types,
    duplicates, email addresses, allowed values, valid ranges, and
    amount consistency. If a table fails validation, nothing is loaded.
3.  **Transformations:** customer history tracking using SCD Type 2
    (pandas) and order aggregations (Spark).
4.  **Atomic loading** into PostgreSQL: all data is written within a
    single transaction (COMMIT or ROLLBACK).
5.  **dbt** builds analytical tables (including a point-in-time join
    against customer history) and tests them.
6.  **Reporting:** data quality and execution reports, failure alerts,
    and temporary-file cleanup.

Each step saves its output as a checkpoint. If a failure occurs, the
next run resumes from the failed step without extracting the data again.

------------------------------------------------------------------------

## Verified Results

  -----------------------------------------------------------------------
  Check                               Result
  ----------------------------------- -----------------------------------
  Automated tests (inside the Docker  **48 out of 48 passed**, including
  container)                          the 8 test cases specified in the
                                      course

  Automated tests (on Windows,        43 passed, 2 skipped (the tests
  without Airflow or Spark)           requiring Airflow or Spark)

  First DAG run in Airflow 3.3.2      14 tasks completed successfully in
                                      under 3 minutes: 5 customers, 144
                                      orders, 287 line items, 5 products

  Second run after a simulated day of **Incremental processing:** 5
  activity                            customers extracted (out of 8), 7
                                      orders (out of 148); unchanged
                                      product catalog skipped

  SCD2 history                        Bob: 2 versions (email change);
                                      Carol: inactive then active; 3 new
                                      customers; no overlapping validity
                                      periods

  Data quality gate (invalid email    Pipeline stopped at
  injected)                           `validate_data`; nothing was loaded
                                      and an alert was generated. After
                                      correction, processing resumed
                                      without creating a false version

  API failure followed by `--resume`  Only order extraction was repeated;
                                      the other steps resumed from their
                                      checkpoints

  dbt                                 7 models built, 24 tests: 23
                                      passed, 1 intentional warning
                                      (order with shipping fees)

  Performance                         **100,000 records transformed in 8
  (`scripts/benchmark.py`)            seconds** (project specification
                                      limit: 2 hours)
  -----------------------------------------------------------------------

Examples of actual reports are available in
[docs/exemples_rapports/](docs/exemples_rapports/).

------------------------------------------------------------------------

## Quick Start

### Prerequisites

-   Docker Desktop (running), 8 GB of RAM, and ports 5432, 8000, 8080,
    and 8081 available
-   Python 3.11 or later (only required to run tests or the pipeline
    outside Docker)

### Option A: Run the Tests on Your PC (2 minutes)

The Python virtual environment is created **outside the OneDrive
folder** to avoid synchronizing thousands of small files:

``` powershell
python -m venv C:\Users\gilda\.venvs\pipeline-spark-dbt-airflow
C:\Users\gilda\.venvs\pipeline-spark-dbt-airflow\Scripts\python.exe -m pip install -r requirements.txt
C:\Users\gilda\.venvs\pipeline-spark-dbt-airflow\Scripts\python.exe -m pytest -v
```

(On this PC, the environment already exists, so only the last command is
needed.)

### Option B: Run the Full Environment with Docker (10 minutes the first time)

``` powershell
docker compose up -d --build
```

Then open **http://localhost:8080** (username: `admin`, password:
`admin`), enable the `automated_data_pipeline` DAG, and watch the tasks
run. The complete guide, including exercises, is available in
[docs/03_guide_pas_a_pas.md](docs/03_guide_pas_a_pas.md).

  ------------------------------------------------------------------------------
  Service                 Address                        Access
  ----------------------- ------------------------------ -----------------------
  Airflow                 http://localhost:8080          admin / admin

  Adminer (database       http://localhost:8081          System: PostgreSQL;
  browser)                                               server: `postgres`;
                                                         username:
                                                         `pipeline_user`;
                                                         password:
                                                         `secure_password`;
                                                         database: `analytics`

  Simulated API           http://localhost:8000/health   API key: `dev-api-key`

  PostgreSQL              localhost:5432                 Same credentials as
                                                         Adminer
  ------------------------------------------------------------------------------

------------------------------------------------------------------------

## Project Structure

``` text
Projet_Pipeline_Spark_dbt_Airflow/
├── config.py               configuration: environment variables + YAML rules
├── extractors.py           connectors: customer database, orders API, product CSV
├── transformers.py         cleaning, pandas aggregations, SCD2, quality checks
├── loaders.py              atomic loading into PostgreSQL (UPSERT, SCD2)
├── pipeline.py              pipeline orchestration, checkpoints, command line
├── checkpoint.py            recovery points and watermarks
├── db.py                    PostgreSQL connection pool
├── reporting.py              execution report (JSON + Markdown) and lineage
├── utils.py                  logs, retries, type conversions, alerts
├── dags/airflow_dag.py       Airflow DAG (14 tasks)
├── spark_jobs/               order aggregations computed with Spark
├── dbt_project/              SQL models, incremental model, SCD2 snapshot, dbt tests
├── config/pipeline.yaml      business rules: required columns, SCD2, tolerances
├── sql/init/                 database and table creation (course script, extended)
├── mock_api/server.py        simulated orders API (the course API URL is unavailable)
├── scripts/                  demo data, activity simulation, benchmark, reset
├── tests/                    48 automated tests (including the 8 course test cases)
├── data/                     input data; checkpoints and reports are generated here
├── docker/airflow/            Airflow + Java + Spark + dbt image
├── docker-compose.yml         complete environment launched with one command
├── docs/                      detailed documentation
└── .env.example               configuration template (copy to .env)
```

------------------------------------------------------------------------

## Mapping to the Project Requirements

### The 8 Functional Requirements

  ---------------------------------------------------------------------------
  Course requirement      Location                    Implementation
  ----------------------- --------------------------- -----------------------
  1\. Multi-source        `extractors.py`             `DatabaseExtractor`
  ingestion                                           (PostgreSQL),
                                                      `APIExtractor`
                                                      (paginated REST),
                                                      `FileExtractor` (CSV)

  2\. Data quality        `transformers.py`           9 categories of checks,
  validation              (`DataValidator`),          PASS / WARN / FAIL
                          `config/pipeline.yaml`      decisions, optional
                                                      quarantine

  3\. Incremental         `pipeline.py`,              Per-source watermarks,
  processing              `checkpoint.py`             file fingerprint, SCD2
                          (`WatermarkStore`), dbt     based on fingerprints,
                          model `fct_orders_enriched` UPSERT, incremental dbt
                                                      model

  4\. SCD2 history        `transformers.py`           Closed and new
  tracking                (`SCD2Processor`),          versions, intervals
                          `loaders.py`,               without gaps or
                          `dbt_project/snapshots/`    overlaps, unique
                                                      database index

  5\. Modular design      One module per              Transformations can be
                          responsibility, pure        tested without a
                          functions                   database

  6\. Error handling and  `utils.retry_call`,         Retries with
  recovery                `APIExtractor._get_json`,   exponential backoff,
                          `checkpoint.py`, Airflow    checkpoints, ROLLBACK,
                          callbacks                   alerts

  7\. Automated           `dags/airflow_dag.py`       Runs daily at 2 a.m.,
  scheduling                                          one run at a time,
                                                      2-hour limit

  8\. Data lineage        `run_id` / `loaded_at`      Each row records which
                          columns, "Lineage" section  pipeline run created it
                          in reports, dbt graph       
  ---------------------------------------------------------------------------

### The 8 Test Cases

  -----------------------------------------------------------------------------------------
  Course test case        Test                                      What it verifies
  ----------------------- ----------------------------------------- -----------------------
  1\. Database extraction `tests/test_extractors.py::test_tc1_*`    Schema, row count, data
                                                                    types, batch reading,
                                                                    parameterized query

  2\. API HTTP 500 error  `tests/test_extractors.py::test_tc2_*`    3 attempts, waits of 1
                                                                    s then 2 s, clear
                                                                    exception, no retry on
                                                                    401

  3\. SCD2: new customers `tests/test_scd2.py::test_tc3_*`          3 new versions with
                                                                    their effective dates

  4\. SCD2: email changed `tests/test_scd2.py::test_tc4_*`          Previous version
                                                                    closed, new version
                                                                    created on the change
                                                                    date

  5\. Missing values      `tests/test_validation.py::test_tc5_*`    Failure with a detailed
                                                                    report; pipeline
                                                                    stopped before
                                                                    transformation

  6\. Incremental         `tests/test_incremental.py::test_tc6_*`   10,000 customers
  processing                                                        followed by 10%
                                                                    changes: only 1,000 are
                                                                    processed

  7\. Recovery after      `tests/test_recovery.py::test_tc7_*`      Restart from the
  failure                                                           transformation step,
                                                                    without re-extraction

  8\. DAG validation      `tests/test_dag.py::test_tc8_*`           DAG loads successfully;
                                                                    14 tasks, correct
                                                                    dependencies and
                                                                    settings
  -----------------------------------------------------------------------------------------

------------------------------------------------------------------------

## Detailed Documentation

1.  [Key Concepts](docs/01_concepts_cles.md): incremental processing,
    SCD2, data quality, idempotency, Airflow, Spark, dbt, and Docker,
    explained using the project's data.
2.  [Architecture](docs/02_architecture.md): the role of each task, the
    data model, technical choices, and limitations.
3.  [Step-by-Step Guide](docs/03_guide_pas_a_pas.md): installation,
    execution, monitoring, and failure simulations to observe how the
    pipeline responds.
4.  [Official Course Solution vs. My
    Implementation](docs/04_solution_officielle_vs_ma_version.md): what
    did not work and why.

------------------------------------------------------------------------

## Useful Commands

  -------------------------------------------------------------------------------------------------------------------
  Action                              Command
  ----------------------------------- -------------------------------------------------------------------------------
  Start the environment               `docker compose up -d --build`

  Check container status              `docker compose ps`

  Run tests inside the container      `docker compose exec -w /opt/project airflow python -m pytest -v`
  (including Airflow and Spark)       

  Simulate one day of activity        `docker compose exec airflow python /opt/project/scripts/simulate_changes.py`

  Run the pipeline from the command   `python pipeline.py`, then after a failure, `python pipeline.py --resume`
  line (on your PC)                   

  Run the performance benchmark       `python scripts/benchmark.py`

  Create the `.zip` file required by  `python scripts/build_submission.py`
  the course                          

  Stop the environment (data is       `docker compose stop`
  preserved)                          

  Reset everything                    `python scripts/reset_demo.py --docker`
  -------------------------------------------------------------------------------------------------------------------

## Versions Used

Airflow 3.3.2 (the course uses 2.8.1; the DAG is compatible with both),
PySpark 4.2.0 with Java, dbt-core 1.12 and dbt-postgres 1.11, PostgreSQL
17, Python 3.12 in Docker and 3.14 on the PC, pandas 3.0.
