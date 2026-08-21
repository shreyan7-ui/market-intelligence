<!-- activate venvs -->
source airflow-venv/bin/activate
venv/Scripts/activate

<!-- run scripts -->
python -m src.ingestion.ingest_his_data
python -m src.ingestion.ingest_live_data
python -m src.ingestion.read.parquet
python -m src.bronze.bronze_news
python -m src.bronze.bronze_stocks
python -m src.silver.silver_news
python -m src.silver.silver_stocks
python -m src.gold.gold_market_events
python -m src.gold.gold_news_analytics
python -m src.gold.gold_stocks_analytics

<!-- airflow start -->
airflow standalone
airflow version

<!-- list all dags -->
airflow dags list
airflow dags list | grep snowflake
airflow dags list | grep market_intelligence
airflow dags list-import-errors
<!-- for list import errors no data found req -->

<!-- show dag structure -->
airflow dags show snowflake_gold_load
airflow dags show market_intelligence_pipeline

<!-- triggering dags -->
airflow dags show market_intelligence_pipeline
airflow dags trigger snowflake_gold_load

<!-- find latest run -->
airflow dags list-runs -d snowflake_gold_load
airflow dags list-runs -d market_intelligence_pipeline

<!-- list runs -->
airflow dags list-runs -d market_intelligence_pipeline
airflow dags list-runs -d snowflake_gold_load

<!-- check individual task states -->
airflow tasks states-for-dag-run \
snowflake_gold_load \
"<run id>"

<!-- dag dosent appear -->
airflow dags list | grep <dag_name>

<!-- task failed -->
airflow tasks states-for-dag-run \
<dag_id> \
"<run_id>"

<!-- find failed task -->
airflow tasks test <dag_id> <task_id>
eg : airflow tasks test snowflake_gold_load load_market_events

<!-- CLI-based debugging -->
ls ~/airflow/logs

<!-- Test DAG Without Actually Triggering a DAG Run -->
airflow tasks test snowflake_gold_load load_market_events
airflow tasks test snowflake_gold_load load_stock_analytics
airflow tasks test snowflake_gold_load load_news_analytics

<!-- Pause / Unpause DAG -->
airflow dags list | grep snowflake_gold_load
airflow dags pause snowflake_gold_load
airflow dags pause snowflake_gold_load

<!-- aws s3 cred check -->
aws sts get-caller-identity

<!-- List your bucket -->
aws s3 ls s3://market-intelligence-platform/

<!-- pyspark debugging -->
python -c "import pyspark; print(pyspark.__version__)"
java -version
echo $HADOOP_HOME
<!-- do in wsl  -->

<!-- snowflake debug -->
airflow connections get snowflake_market_intelligence