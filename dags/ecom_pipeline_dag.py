# dags/ecom_pipeline_final_dag.py
# This is a new Airflow DAG file for the revised data pipeline orchestration.

from airflow import DAG
from airflow.models.xcom_arg import XComArg
from airflow.providers.databricks.operators.databricks import  DatabricksRunNowOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.standard.operators.bash import BashOperator
import pendulum

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# --- Configuration Variables (REPLACE THESE WITH YOUR ACTUAL VALUES) ---
# IMPORTANT: Update these placeholder values with your specific project and resource details.
DATABRICKS_CONN_ID = 'databricks_default' # Ensure this Airflow connection exists and is configured (Host, Token)
# DATABRICKS_CLUSTER_ID = "YOUR_DATABRICKS_CLUSTER_ID" # If you use an existing cluster for the Spark job, replace.
                                                     # If using serverless compute for jobs, you might omit this field
                                                     # as serverless jobs often provision their own ephemeral compute.

DATABRICKS_DLT_PIPELINE_ID = "807299115849478" # Get this ID from your DLT pipeline URL in Databricks UI

GCP_PROJECT_ID = "batch-processing-de" # Your Google Cloud Project ID
BQ_DATASET = "ecom_dwh" # Your BigQuery dataset name where final tables will reside

# Base GCS path where your DLT pipeline stores its internal tables (Bronze and Silver Delta tables).
DLT_STORAGE_GCS_PATH = "gs://batch-processing-de_final_data/dlt" # Example: "gs://my-project-dlt-storage"

# Base GCS path where the Analytic Layer transformations will write the final Parquet files.
# These will then be loaded directly into BigQuery.
ANALYTIC_LAYER_GCS_PATH = "gs://batch-processing-de_final_data" # Example: "gs://my-project-analytic-data"
ANALYTIC_LAYER_GCS_PREFIX = "dlt"

# Path to the Analytic Layer Transformation script on Databricks File System (DBFS) or a Unity Catalog Volume.
# You MUST upload 'spark_scripts/03_analytic_layer_transformations.py' to this location in Databricks.
# DBFS_ANALYTIC_TRANSFORM_SCRIPT_PATH = "dbfs:/FileStore/spark_scripts/03_analytic_layer_transformations.py" # Example: "dbfs:/Users/your_user/scripts/03_analytic_layer.py"

# Path to your dbt project on the Airflow worker/container.
# This directory should contain your dbt_project.yml and model files.
DBT_PROJECT_PATH = "/opt/airflow/dbt_project/ecom_dwh" # Example: "/opt/airflow/dags/dbt/ecom_dwh"


# Path to your GCP service account key file on the Airflow worker/container.
# This key is used by dbt to authenticate with BigQuery. For Cloud Composer,
# ensure the Composer environment's service account has the necessary BigQuery roles,
# and this file might not be explicitly needed if Application Default Credentials (ADC) are used.
GCP_SA_KEY_PATH_FOR_DBT = "/opt/airflow/gcp_keys/gcp_sa_key.json" # Example: "/opt/airflow/gcp_keys/dbt_sa.json"
# If running on Cloud Composer, consider using the Composer environment's service account permissions
# instead of an explicit key file. In that case, you might remove the GOOGLE_APPLICATION_CREDENTIALS export.


# --- Define BigQuery Table Schemas ---
# Explicitly defining schemas for BigQuery loads from GCS is highly recommended
# to ensure data types and nullability are correct. Adjust these based on your
# actual Parquet file schemas and BigQuery requirements.

SALES_FACT_SCHEMA = [
    {"name": "order_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "customer_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "order_date", "type": "DATE", "mode": "NULLABLE"},
    {"name": "order_status", "type": "STRING", "mode": "NULLABLE"},
    {"name": "original_total_amount", "type": "BIGNUMERIC", "mode": "NULLABLE"},
    {"name": "calculated_order_total", "type": "BIGNUMERIC", "mode": "NULLABLE"},
    {"name": "total_products_in_order", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "total_quantity_in_order", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "audit_load_timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
]

DIM_PRODUCTS_SCHEMA = [
    {"name": "product_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "product_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "product_category", "type": "STRING", "mode": "NULLABLE"},
    {"name": "product_price", "type": "BIGNUMERIC", "mode": "NULLABLE"},
    {"name": "product_hash", "type": "STRING", "mode": "NULLABLE"},
    {"name": "effective_start_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "effective_end_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "current_flag", "type": "BOOLEAN", "mode": "NULLABLE"},
]

DIM_CUSTOMERS_SCHEMA = [
    {"name": "customer_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "first_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "last_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "email", "type": "STRING", "mode": "NULLABLE"},
    {"name": "registration_date", "type": "DATE", "mode": "NULLABLE"},
    {"name": "country", "type": "STRING", "mode": "NULLABLE"},
    {"name": "customer_hash", "type": "STRING", "mode": "NULLABLE"},
    {"name": "effective_start_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "effective_end_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "current_flag", "type": "BOOLEAN", "mode": "NULLABLE"},
]


with (DAG(
    dag_id='ecom_data_pipeline_final', # Updated DAG ID to reflect new file
    default_args=default_args,
    description='E-commerce Data Pipeline: DLT -> Analytic Layer Parquet (GCS) -> BigQuery Load -> dbt',
    schedule='@daily',
    start_date=pendulum.datetime(2025, 6, 21, tz="UTC"),
    catchup=False,
    tags=["ecom", "data_engineering", "gcp", "spark", "dlt", "bigquery", "dbt"],
) as dag):
    # Task 1: Trigger the Databricks Delta Live Tables (DLT) pipeline
    # This task starts the DLT pipeline that reads raw data, cleans it,
    # and transforms it into bronze and silver Delta tables.
    trigger_dlt_pipeline = DatabricksRunNowOperator(
        task_id='trigger_delta_live_tables_pipeline',
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id = DATABRICKS_DLT_PIPELINE_ID,
        # pipeline_task={
        #    'pipeline_id': DATABRICKS_DLT_PIPELINE_ID,
        #    'full_refresh': False, # Set to True for a full refresh of all DLT tables
        #                           # Set to False for incremental updates (default for triggered pipelines)
        #},
        # DLT pipelines manage their own compute based on their configuration in Databricks.
        # No explicit new_cluster or existing_cluster_id needed here for DLT.
    )

    # Task 2: Run Spark job for Analytic Layer Transformations
    # This task executes the '03_analytic_layer_transformations.py' script on Databricks.
    # It reads from Silver Delta tables, performs CDC/SCD, and writes final Parquet to GCS.
    '''run_analytic_layer_transformations = DatabricksSubmitRunOperator(
        task_id='run_analytic_layer_transformations',
        databricks_conn_id=DATABRICKS_CONN_ID,
        # Specify an existing cluster ID or define a new, transient cluster for this job.
        # Ensure this cluster has access to DLT_STORAGE_GCS_PATH and ANALYTIC_LAYER_GCS_PATH.
        # existing_cluster_id=DATABRICKS_CLUSTER_ID,
        # new_cluster={
        #     'spark_version': '12.2.x-scala2.12', # Or latest LTS Databricks Runtime version
        #     'node_type_id': 'Standard_DS3_v2', # Choose appropriate node type
        #     'num_workers': 2,
        #     'custom_tags': {'job_type': 'analytic_layer'},
        #     'spark_conf': {
        #         # Add any necessary Spark configurations, e.g., for GCS connector properties
        #         'spark.jars.excludes': 'com.google.guava:guava', # Often needed to prevent conflicts
        #     }
        # },
        python_file_task={
            'python_file': DBFS_ANALYTIC_TRANSFORM_SCRIPT_PATH,
            'parameters': [
                DLT_STORAGE_GCS_PATH,
                ANALYTIC_LAYER_GCS_PATH
            ]
        },
        # Ensure the BigQuery connector JAR is available on the cluster if your
        # 03_analytic_layer_transformations.py script had any implicit BigQuery interaction
        # (though in the current design, it only writes to GCS).
        libraries=[
            {"jar": "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-latest.jar"} # Recommended if any BQ interaction on cluster
        ]
    )'''

    list_fact_sales_files = GCSListObjectsOperator(
        task_id='list_fact_sales_files',
        bucket=ANALYTIC_LAYER_GCS_PATH.replace("gs://", ""),
        prefix=f"{ANALYTIC_LAYER_GCS_PREFIX}/fact_daily_sales/",  # Adjusted prefix
        delimiter='/',  # To ensure it lists objects in subdirectories
        match_glob='**/*.parquet',  # This globbing is for GCSListObjectsOperator's internal filtering
        gcp_conn_id='google_cloud_default',
        # xcom_push is True by default for GCSListObjectsOperator
    )

    list_dim_products_files = GCSListObjectsOperator(
        task_id='list_dim_products_files',
        bucket=ANALYTIC_LAYER_GCS_PATH.replace("gs://", ""),
        prefix=f"{ANALYTIC_LAYER_GCS_PREFIX}/dim_products/",  # Adjusted prefix
        delimiter='/',
        match_glob='**/*.parquet',
        gcp_conn_id='google_cloud_default',
    )

    list_dim_customers_files = GCSListObjectsOperator(
        task_id='list_dim_customers_files',
        bucket=ANALYTIC_LAYER_GCS_PATH.replace("gs://", ""),
        prefix=f"{ANALYTIC_LAYER_GCS_PREFIX}/dim_customers/",  # Adjusted prefix
        delimiter='/',
        match_glob='**/*.parquet',
        gcp_conn_id='google_cloud_default',
    )

    # Task 3: Load Fact Daily Sales from GCS (Parquet) to BigQuery
    # This loads new partitions incrementally into BigQuery.

    load_fact_daily_sales_to_bq = GCSToBigQueryOperator(
        task_id='load_fact_daily_sales_to_bigquery',
        bucket=ANALYTIC_LAYER_GCS_PATH.replace("gs://", ""), # Extract bucket name from GCS path
        source_objects=XComArg(list_fact_sales_files), # Use wildcard to pick up all partitioned Parquet files
        destination_project_dataset_table=f'{GCP_PROJECT_ID}.{BQ_DATASET}.fact_daily_sales',
        schema_fields=SALES_FACT_SCHEMA, # Explicit schema for reliability
        source_format='PARQUET',
        create_disposition='CREATE_IF_NEEDED', # Create table if it doesn't exist
        write_disposition='WRITE_APPEND', # Append new data (assuming partitioning handles duplicates)
        gcp_conn_id='google_cloud_default',# Airflow connection to GCP
        trigger_rule= 'one_success',
    )

    # Task 4: Load Dimension Products from GCS (Parquet) to BigQuery (Full load for SCD Type 2)
    # Since SCD Type 2 logic is handled in the Spark transformation, BigQuery receives the full, updated dimension.
    load_dim_products_to_bq = GCSToBigQueryOperator(
        task_id='load_dim_products_to_bigquery',
        bucket=ANALYTIC_LAYER_GCS_PATH.replace("gs://", ""),
        source_objects=XComArg(list_dim_products_files),
        destination_project_dataset_table=f'{GCP_PROJECT_ID}.{BQ_DATASET}.dim_products',
        schema_fields=DIM_PRODUCTS_SCHEMA,
        source_format='PARQUET',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE', # Truncate existing table and load full new version
        gcp_conn_id='google_cloud_default',
        trigger_rule='one_success',
    )

    # Task 5: Load Dimension Customers from GCS (Parquet) to BigQuery (Full load for SCD Type 2)
    # Similar to products, BigQuery gets the full, updated dimension after Spark transformations.
    load_dim_customers_to_bq = GCSToBigQueryOperator(
        task_id='load_dim_customers_to_bigquery',
        bucket=ANALYTIC_LAYER_GCS_PATH.replace("gs://", ""),
        source_objects= XComArg(list_dim_customers_files),
        destination_project_dataset_table=f'{GCP_PROJECT_ID}.{BQ_DATASET}.dim_customers',
        schema_fields=DIM_CUSTOMERS_SCHEMA,
        source_format='PARQUET',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE', # Truncate existing table and load full new version
        gcp_conn_id='google_cloud_default',
        trigger_rule='one_success',
    )

    # Task 6: Run dbt models
    # This task assumes dbt is installed and configured on the Airflow worker/container
    # and has permissions to access BigQuery.
    run_dbt_models_task = BashOperator(
        task_id='run_dbt_models',
        bash_command=f"""
            export GOOGLE_APPLICATION_CREDENTIALS="{GCP_SA_KEY_PATH_FOR_DBT}"
            cd {DBT_PROJECT_PATH}
            cat <<EOF> ./profiles.yml
            ecom_dwh:
                outputs:
                    dev:
                        dataset: ecom_dwh_dbt
                        job_execution_timeout_seconds: 100
                        job_retries: 1
                        keyfile: {GCP_SA_KEY_PATH_FOR_DBT}
                        location: US
                        method: service-account
                        priority: interactive
                        project: batch-processing-de
                        threads: 2
                        type: bigquery
                target: dev
            EOF
            
            dbt debug --profile ecom_dwh --target dev # Use target 'prod' for production deployments
            dbt run --profile ecom_dwh --target dev
            dbt test --profile ecom_dwh --target dev
        """,
    )

    # Define task dependencies
    # DLT pipeline must complete before analytic layer transformations can start.
    # Analytic layer transformations must complete before GCS to BigQuery loads can start.
    trigger_dlt_pipeline >> [
        list_fact_sales_files,
        list_dim_products_files,
        list_dim_customers_files]

    list_fact_sales_files >> load_fact_daily_sales_to_bq
    list_dim_customers_files >> load_dim_customers_to_bq
    list_dim_products_files >> load_dim_products_to_bq

    [load_fact_daily_sales_to_bq,load_dim_products_to_bq,load_dim_customers_to_bq] >> run_dbt_models_task