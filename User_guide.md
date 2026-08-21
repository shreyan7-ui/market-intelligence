# User Guide

## Activate Virtual Environments

### Linux / WSL

```bash
source airflow-venv/bin/activate
```

### Windows

```bash
venv/Scripts/activate
```

---

# Run Project Scripts

Run the ingestion scripts:

```bash
python -m src.ingestion.ingest_his_data
python -m src.ingestion.ingest_live_data
python -m src.ingestion.read.parquet
```

Run the Bronze layer:

```bash
python -m src.bronze.bronze_news
python -m src.bronze.bronze_stocks
```

Run the Silver layer:

```bash
python -m src.silver.silver_news
python -m src.silver.silver_stocks
```

Run the Gold layer:

```bash
python -m src.gold.gold_market_events
python -m src.gold.gold_news_analytics
python -m src.gold.gold_stocks_analytics
```

---

# Airflow

## Start Airflow

Start Airflow in standalone mode:

```bash
airflow standalone
```

Check Airflow version:

```bash
airflow version
```

---

## List All DAGs

List all available DAGs:

```bash
airflow dags list
```

Check whether the Snowflake DAG is loaded:

```bash
airflow dags list | grep snowflake
```

Check whether the Market Intelligence DAG is loaded:

```bash
airflow dags list | grep market_intelligence
```

### Check for DAG Import Errors

```bash
airflow dags list-import-errors
```

**Note:** If there are no import errors, Airflow will return no data.

---

# Show DAG Structure

Display the structure of the Snowflake DAG:

```bash
airflow dags show snowflake_gold_load
```

Display the structure of the Market Intelligence pipeline:

```bash
airflow dags show market_intelligence_pipeline
```

---

# Triggering DAGs

### Check DAG Structure Before Triggering

```bash
airflow dags show market_intelligence_pipeline
```

### Trigger Snowflake Gold Load

```bash
airflow dags trigger snowflake_gold_load
```

---

# Find Latest DAG Runs

List runs for the Snowflake Gold Load DAG:

```bash
airflow dags list-runs -d snowflake_gold_load
```

List runs for the Market Intelligence Pipeline:

```bash
airflow dags list-runs -d market_intelligence_pipeline
```

---

# List DAG Runs

List all recorded runs for the Market Intelligence Pipeline:

```bash
airflow dags list-runs -d market_intelligence_pipeline
```

List all recorded runs for the Snowflake Gold Load DAG:

```bash
airflow dags list-runs -d snowflake_gold_load
```

---

# Check Individual Task States

Check the state of every task for a specific DAG run:

```bash
airflow tasks states-for-dag-run \
snowflake_gold_load \
"<run_id>"
```

Replace `<run_id>` with the actual DAG run ID.

---

# DAG Doesn't Appear

If a DAG does not appear in the Airflow UI or DAG list, check whether Airflow has loaded it:

```bash
airflow dags list | grep <dag_name>
```

Replace `<dag_name>` with the actual DAG ID.

If the DAG still does not appear, check for import errors:

```bash
airflow dags list-import-errors
```

---

# Task Failed

Check the state of all tasks for a specific DAG run:

```bash
airflow tasks states-for-dag-run \
<dag_id> \
"<run_id>"
```

Replace:

- `<dag_id>` with the DAG ID
- `<run_id>` with the DAG run ID

---

# Find / Debug a Failed Task

Test an individual task directly:

```bash
airflow tasks test <dag_id> <task_id>
```

Example:

```bash
airflow tasks test snowflake_gold_load load_market_events
```

---

# CLI-Based Debugging

Check the Airflow log directory:

```bash
ls ~/airflow/logs
```

Use this when investigating task execution failures or checking whether task logs were generated.

---

# Test DAG Tasks Without Triggering a DAG Run

`airflow tasks test` executes an individual task without creating a normal DAG run.

Test the Market Events loading task:

```bash
airflow tasks test snowflake_gold_load load_market_events
```

Test the Stock Analytics loading task:

```bash
airflow tasks test snowflake_gold_load load_stock_analytics
```

Test the News Analytics loading task:

```bash
airflow tasks test snowflake_gold_load load_news_analytics
```

This is useful for debugging a specific task without running the entire DAG.

---

# Pause / Unpause DAG

Check whether the DAG exists:

```bash
airflow dags list | grep snowflake_gold_load
```

Pause the DAG:

```bash
airflow dags pause snowflake_gold_load
```

Unpause the DAG:

```bash
airflow dags unpause snowflake_gold_load
```

---

# AWS S3 Credential Check

Check which AWS identity is currently being used:

```bash
aws sts get-caller-identity
```

This is useful for debugging AWS credential and IAM permission issues.

---

# List S3 Bucket

List the contents of the Market Intelligence S3 bucket:

```bash
aws s3 ls s3://market-intelligence-platform/
```

---

# PySpark Debugging

Check the installed PySpark version:

```bash
python -c "import pyspark; print(pyspark.__version__)"
```

Check the installed Java version:

```bash
java -version
```

Check the Hadoop home directory:

```bash
echo $HADOOP_HOME
```

> **Note:** Run these commands in **WSL** when debugging the WSL-based PySpark environment.

---

# Snowflake Debugging

Check the Airflow Snowflake connection:

```bash
airflow connections get snowflake_market_intelligence
```

Use this to verify that the Snowflake connection exists and inspect its configured connection details.
