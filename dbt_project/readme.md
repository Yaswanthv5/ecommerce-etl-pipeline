E-commerce Data Analytics Platform: Architecture & Design
1. Introduction
This document outlines the architecture and design of an end-to-end data analytics platform for e-commerce data. The primary goal is to establish a robust, scalable, and maintainable pipeline that transforms raw transactional data into analytics-ready formats within Google Cloud Platform (GCP). The pipeline incorporates data ingestion, cleansing, transformation, Change Data Capture (CDC) / Slowly Changing Dimension (SCD) logic, and orchestration, ultimately making curated data available for business intelligence and reporting.

2. Overall Architecture
The platform follows a Medallion Architecture (Bronze, Silver, Gold/Analytic layers) and leverages a combination of managed and self-hosted services within GCP and Databricks.

graph TD
    subgraph Data Sources
        A[Synthetic CSVs] --> B(GCS Raw Layer)
    end

    subgraph Data Processing & Transformation (Databricks)
        B --> C[Databricks DLT Pipeline]
        C --Bronze Layer--> D[Delta Table: Bronze_* (GCS)]
        D --> E[Delta Table: Silver_* (GCS)]
        E --> F[Spark Job: Analytic Layer Transformations]
    end

    subgraph Analytical Data Storage (GCP)
        F --Parquet Output--> G[GCS Analytic Layer]
        G --> H[BigQuery Data Warehouse]
    end

    subgraph Orchestration & BI
        I[Apache Airflow] --> C
        I --> F
        I --> H
        H --> J[BI Tools / Reporting]
    end

    style C fill:#f9f,stroke:#333,stroke-width:2px
    style F fill:#f9f,stroke:#333,stroke-width:2px
    style H fill:#9cf,stroke:#333,stroke-width:2px
    style I fill:#fc6,stroke:#333,stroke-width:2px

Key Components:

Google Cloud Storage (GCS): Serves as the landing zone for raw data, storage for Delta Lake tables (Bronze & Silver), and a temporary staging area for the final analytical Parquet files.

Databricks: Provides the unified analytics platform for executing Spark-based transformations, including Delta Live Tables.

Delta Live Tables (DLT): A declarative framework on Databricks for building reliable ETL pipelines with built-in data quality checks and incremental processing. Used for Bronze and Silver layers.

Apache Spark (on Databricks): Powers the core data transformations, including advanced CDC and SCD Type 2 logic.

BigQuery: Google's serverless, highly scalable, and cost-effective enterprise data warehouse. It stores the final, curated analytical data.

Apache Airflow: The workflow orchestration engine, responsible for scheduling and managing the entire data pipeline. It coordinates tasks across Databricks and GCP.

dbt (Data Build Tool): (Currently removed from this flow's transformation steps, but can be reintroduced for further BigQuery-native transformations or data quality on the loaded data).

3. Data Flow Explanation
Raw Data Generation: Synthetic e-commerce data (customers, products, orders, order items) is generated as CSV files by the generate_data.py script.

GCS Raw Layer: These raw CSVs are uploaded to a designated GCS bucket (gs://your-project-id-raw-data/). This acts as the external source for the DLT pipeline.

Databricks Delta Live Tables (Bronze & Silver):

The Airflow DAG triggers the DLT pipeline (dlt_pipelines/ecom_dlt_pipeline.py).

Bronze Layer: DLT uses Auto Loader to incrementally ingest CSVs from the GCS Raw Layer, apply basic schema inference, and perform initial cleansing (e.g., type casting, handling nulls). This data is stored as immutable Delta tables (e.g., bronze_orders, bronze_products) in a GCS-backed DLT storage location (gs://your-project-id-processed-data/dlt_storage/tables/).

Silver Layer: DLT then reads from the Bronze Delta tables, performs joins (e.g., orders with order items, products, customers), aggregations, and enriches the data. The output is Silver Delta tables (e.g., silver_sales, silver_products, silver_customers), also stored in the DLT storage location. These tables are designed to be clean, conformed, and ready for further analytical processing.

Spark Analytic Layer Transformations:

Following DLT, Airflow triggers a dedicated Spark job (spark_scripts/03_analytic_layer_transformations.py) on Databricks.

This script reads the Silver Delta tables.

It applies complex transformations, including:

SCD Type 2 (Slowly Changing Dimension Type 2) for Dimensions: For dim_products and dim_customers, new versions of records are created when attributes change, preserving historical context with effective_start_date, effective_end_date, and current_flag columns. Unchanged records are carried forward.

Fact Aggregation/Selection: For fact_daily_sales, data is selected, potentially aggregated (though primary aggregation happened in silver), and prepared for direct analytical querying.

The output of this Spark job is written as partitioned Parquet files to a new GCS bucket designated for the Analytic Layer (gs://your-project-id-analytic-layer/). Partitioning (e.g., by order_date for facts, effective_start_date for dimensions) optimizes downstream BigQuery loads.

GCS to BigQuery Load:

Airflow's GoogleCloudStorageToBigQueryOperator is used to directly load the partitioned Parquet files from the GCS Analytic Layer into their respective BigQuery tables (fact_daily_sales, dim_products, dim_customers).

fact_daily_sales is loaded in WRITE_APPEND mode, leveraging partitioning.

Dimension tables (dim_products, dim_customers) are loaded in WRITE_TRUNCATE mode after Spark has already handled the SCD Type 2 logic and created the full, updated dimension state.

dbt (Data Build Tool):

After the initial BigQuery load, the Airflow DAG triggers dbt.

dbt can be used to define further transformations within BigQuery, create aggregated data marts, perform data quality tests, or generate documentation. Its role is now focused on the final semantic layer within BigQuery.

BI Tools / Reporting: The final curated tables in BigQuery are directly accessible by business intelligence tools (e.g., Tableau, Looker, Power BI) for reporting and analysis.

4. Component Breakdown and Responsibilities
4.1. Google Cloud Storage (GCS)
Role: Durable and scalable object storage.

Usage:

Raw Data Landing Zone: gs://your-project-id-raw-data/

DLT Storage & Checkpoints: gs://your-project-id-processed-data/dlt_storage/ (for Delta tables, DLT logs, checkpoint information)

Analytic Layer Storage: gs://your-project-id-analytic-layer/ (for final Parquet files ready for BigQuery load)

4.2. Databricks
Role: Unified platform for data engineering, machine learning, and data science. Provides optimized Spark runtime and Delta Lake capabilities.

Usage: Execution environment for DLT pipelines and custom Spark jobs.

4.3. Delta Live Tables (DLT)
Role: Declarative ETL development, automated infrastructure management, data quality enforcement, and incremental processing.

Script: dlt_pipelines/ecom_dlt_pipeline.py

Bronze Layer (bronze_orders, bronze_order_items, bronze_products, bronze_customers):

Input: Raw CSVs from GCS using Auto Loader (cloudFiles).

Transformations: Schema inference, type casting, basic null filtering.

Output: Delta tables.

Silver Layer (silver_sales, silver_products, silver_customers):

Input: Bronze Delta tables.

Transformations: Joins (orders with items, products, customers), aggregations (e.g., calculated order total), data enrichment. Data quality expectations are defined using @dlt.expect.

Output: Delta tables, clean and conformed.

Processing Mode: Triggered mode for daily/periodic batch processing.

4.4. Apache Spark (on Databricks)
Role: Performs complex, stateful transformations and CDC/SCD logic that are difficult or less efficient to express declaratively solely in DLT's streaming context for the analytical layer.

Script: spark_scripts/03_analytic_layer_transformations.py

Input: Silver Delta tables from the DLT storage location.

Transformations:

Fact Table (fact_daily_sales): Reads silver_sales, selects necessary columns, and prepares for append-only load.

Dimension Tables (dim_products, dim_customers):

Implements SCD Type 2 logic. This involves:

Calculating a hash for relevant dimension attributes to detect changes.

Identifying new records, changed records, and unchanged records by comparing with the previous state of the dimension in GCS Parquet.

Expiring old versions of changed records by setting effective_end_date and current_flag = False.

Inserting new records and new versions of changed records with current_flag = True and appropriate effective_start_date.

Output: Partitioned Parquet files written to the GCS Analytic Layer.

4.5. BigQuery
Role: Central analytical data warehouse for serving BI tools.

Usage: Stores the final fact_daily_sales (append-only) and dim_products, dim_customers (SCD Type 2 enabled, loaded as full truncates per run after Spark handles the logic).

Loading: Data is loaded directly from GCS Parquet files using Airflow's GoogleCloudStorageToBigQueryOperator.

4.6. Apache Airflow
Role: Orchestrates the entire end-to-end data pipeline, ensuring tasks run in the correct order and handling retries/monitoring.

DAG: dags/ecom_pipeline_final_dag.py

Tasks:

trigger_delta_live_tables_pipeline (using DatabricksSubmitRunOperator with pipeline_task): Initiates the DLT update.

run_analytic_layer_transformations (using DatabricksSubmitRunOperator with python_file_task): Triggers the Spark job on Databricks.

load_fact_daily_sales_to_bigquery (using GoogleCloudStorageToBigQueryOperator): Loads sales data from GCS Parquet to BigQuery. WRITE_APPEND mode.

load_dim_products_to_bigquery (using GoogleCloudStorageToBigQueryOperator): Loads product dimension from GCS Parquet to BigQuery. WRITE_TRUNCATE mode.

load_dim_customers_to_bigquery (using GoogleCloudStorageToBigQueryOperator): Loads customer dimension from GCS Parquet to BigQuery. WRITE_TRUNCATE mode.

run_dbt_models_task (using BashOperator): Executes dbt commands within the Airflow worker context.

5. Authentication and Security
Databricks to GCP (GCS/BigQuery):

Workload Identity Federation (Recommended): This is the secure, keyless method. Databricks assumes the identity of a GCP Service Account to access GCS buckets and BigQuery datasets. This avoids storing long-lived credentials. (Setup guide in prior responses).

Airflow to Databricks API:

Databricks Personal Access Token (PAT): Airflow authenticates to the Databricks API using a PAT stored securely in an Airflow connection (databricks_default).

Airflow to GCP (GCS/BigQuery):

Service Account Key (for self-hosted Docker): For local Docker setups, a GCP service account JSON key file (mounted into the Airflow container) provides the credentials for GoogleCloudStorageToBigQueryOperator and dbt. For Cloud Composer, the underlying Composer environment's service account takes care of this.

6. Future Enhancements
Streaming Ingestion: Implement real-time data ingestion (e.g., from Kafka/Pub/Sub) into the Bronze layer using DLT's streaming capabilities.

Enhanced Data Quality: Integrate Great Expectations or other data profiling tools directly into DLT or Spark for more rigorous quality checks.

Data Governance: Implement Unity Catalog for fine-grained access control and data lineage tracking across Databricks and BigQuery.

Monitoring & Alerting: Set up comprehensive monitoring dashboards (e.g., using Grafana, Cloud Monitoring) and alerting for pipeline health and data quality anomalies.

Cost Optimization: Fine-tune Spark cluster configurations and DLT compute settings, explore BigQuery cost controls.

Advanced SCD Types: Implement more complex SCD types (e.g., SCD Type 3 with overwrite, or combinations) as needed.