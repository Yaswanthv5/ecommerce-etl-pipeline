from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Replace with your actual project, region, and Databricks cluster ID
GCP_PROJECT_ID = "your-gcp-project-id"
GCP_REGION = "your-gcp-region" # e.g., us-central1
DATABRICKS_CLUSTER_ID = "your-databricks-cluster-id" # Get this from your Databricks workspace URL or cluster config
DATABRICKS_HOST = "https://your-databricks-instance.cloud.databricks.com" # e.g., https://dbc-xxxxxxxx.cloud.databricks.com

# Databricks authentication (consider Airflow Connections for sensitive info)
# For simplicity, we'll assume the Databricks cluster has appropriate service account access
# or you've configured Databricks secrets.
# Using 'spark_python_task' will execute the Python script as a Databricks job.

with DAG(
    dag_id='ecom_data_pipeline',
    default_args=default_args,
    description='E-commerce Data Pipeline from GCS to BigQuery with Spark and dbt',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['ecom', 'data_engineering', 'gcp', 'spark', 'dbt'],
) as dag:
    # Task 1: Run Spark job for raw data extraction and cleaning
    # This assumes your Spark script '01_extract_and_clean.py' is uploaded to Databricks workspace
    # or accessible via GCS.
    extract_clean_spark_task = DataprocSubmitJobOperator(
        task_id='extract_clean_raw_data',
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job={
            "job_name": "ecom_extract_clean",
            "spark_submit_job": {
                "main_python_file_uri": "gs://your-project-id-spark-scripts/01_extract_and_clean.py", # Upload your Python script here
                "jar_file_uris": ["gs://your-project-id-spark-jars/gcs-connector-hadoop3-latest.jar",
                                  "gs://your-project-id-spark-jars/spark-bigquery-with-dependencies_2.12-latest.jar"], # Latest compatible versions
                "properties": {
                    "spark.hadoop.google.cloud.auth.service.account.enable": "true",
                    "spark.hadoop.google.cloud.auth.service.account.json.keyfile": "/path/to/your/gcp/service-account-key.json", # Or use cluster-level IAM roles
                    # If running directly in Databricks, these GCS connector settings might be managed differently
                }
            },
            "placement": {
                "cluster_name": "your-dataproc-cluster-name" # If using Dataproc directly
                # If using Databricks, you'd use DatabricksSubmitRunOperator or equivalent
            }
        },
        # For Databricks, use DatabricksSubmitRunOperator from airflow.providers.databricks.operators.databricks
        # You'd point to the notebook path in Databricks or a Python file on DBFS/GCS
        # Example for Databricks:
        # databricks_conn_id='databricks_default', # Configure a Databricks connection in Airflow
        # new_cluster={...}, # Or existing_cluster_id
        # notebook_task={'notebook_path': '/Users/your_user/ecom_data_pipeline/01_extract_and_clean'},
        # python_file_task={'python_file': 'dbfs:/FileStore/scripts/01_extract_and_clean.py'}
    )

    # Task 2: Run Spark job for silver layer transformations
    transform_silver_spark_task = DataprocSubmitJobOperator(
        task_id='transform_silver_layer',
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job={
            "job_name": "ecom_transform_silver",
            "spark_submit_job": {
                "main_python_file_uri": "gs://your-project-id-spark-scripts/02_transform_to_silver.py",
                "jar_file_uris": ["gs://your-project-id-spark-jars/gcs-connector-hadoop3-latest.jar"],
                "properties": {
                    "spark.hadoop.google.cloud.auth.service.account.enable": "true",
                    "spark.hadoop.google.cloud.auth.service.account.json.keyfile": "/path/to/your/gcp/service-account-key.json",
                }
            },
            "placement": {
                "cluster_name": "your-dataproc-cluster-name"
            }
        },
        # For Databricks:
        # databricks_conn_id='databricks_default',
        # notebook_task={'notebook_path': '/Users/your_user/ecom_data_pipeline/02_transform_to_silver'},
        # python_file_task={'python_file': 'dbfs:/FileStore/scripts/02_transform_to_silver.py'}
    )

    # Task 3: Run Spark job to load data to BigQuery
    load_to_bigquery_spark_task = DataprocSubmitJobOperator(
        task_id='load_to_bigquery',
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job={
            "job_name": "ecom_load_bigquery",
            "spark_submit_job": {
                "main_python_file_uri": "gs://your-project-id-spark-scripts/03_load_to_bigquery.py",
                "jar_file_uris": ["gs://your-project-id-spark-jars/gcs-connector-hadoop3-latest.jar",
                                  "gs://your-project-id-spark-jars/spark-bigquery-with-dependencies_2.12-latest.jar"],
                "properties": {
                    "spark.hadoop.google.cloud.auth.service.account.enable": "true",
                    "spark.hadoop.google.cloud.auth.service.account.json.keyfile": "/path/to/your/gcp/service-account-key.json",
                    "spark.jars.excludes": "com.google.guava:guava" # Often needed for BigQuery connector with Dataproc/Spark
                }
            },
            "placement": {
                "cluster_name": "your-dataproc-cluster-name"
            }
        },
        # For Databricks:
        # databricks_conn_id='databricks_default',
        # notebook_task={'notebook_path': '/Users/your_user/ecom_data_pipeline/03_load_to_bigquery'},
        # python_file_task={'python_file': 'dbfs:/FileStore/scripts/03_load_to_bigquery.py'}
    )

    # Task 4: Run dbt models (BashOperator to execute dbt commands)
    # Ensure dbt is installed on the Airflow worker/container and `profiles.yml` is configured
    # to access BigQuery. The service account key for BigQuery should be accessible.
    run_dbt_models_task = BashOperator(
        task_id='run_dbt_models',
        bash_command=f"""
            export GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/gcp/service-account-key.json
            cd /path/to/your/dbt/project # Your dbt project files will need to be accessible on the Airflow worker
            dbt debug --profile ecom_dwh --target dev
            dbt run --profile ecom_dwh --target dev
            dbt test --profile ecom_dwh --target dev
        """,
        # You'd likely put your dbt project in your DAGs folder or a persistent volume
        # or fetch it from a Git repository in your CI/CD.
    )

    # Define task dependencies
    extract_clean_spark_task >> transform_silver_spark_task >> load_to_bigquery_spark_task >> run_dbt_models_task