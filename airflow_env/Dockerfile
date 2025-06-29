# your-ecom-data-pipeline/airflow_env/Dockerfile

# 1. Choose a Base Airflow Image
#    It's crucial to start FROM an official Airflow image.
#    Use the same version as specified in your .env or docker-compose.yaml for consistency.
#    Example: apache/airflow:2.9.0-python3.9 (match your Python version and Airflow version)
FROM apache/airflow:3.0.2-python3.12

# 2. Switch to 'root' user temporarily for system-level installations
#    Airflow runs as the 'airflow' user by default. For system packages, you need root.
USER airflow

# 3. Install any necessary system-level dependencies
#    Example: git, curl, or database client libraries (e.g., for PostgreSQL, MySQL, MSSQL)
#    For GCP integration, you often don't need much here unless you're running gcloud CLI commands
#    directly from the Airflow container (less common, usually done by providers).
#    If you need a Java Runtime Environment for some Spark/Hadoop tools, install here.
# RUN apt-get update && \
#     apt-get install -y --no-install-recommends \
#         git \
#         curl \
#         # libpq-dev  # For PostgreSQL client if connecting directly
#         # default-jre # If you need Java Runtime Environment for some specific tools
#     && apt-get clean && rm -rf /var/lib/apt/lists/*

# 4. Copy your custom requirements.txt for additional Python packages
#    This `requirements.txt` should contain *only* the Python packages NOT already handled
#    by the default Airflow image's provider installation mechanism or the base image itself.
#    The `apache-airflow-providers-*` are typically installed via AIRFLOW_PROVIDER_REQUIREMENTS
#    or the base image already, but if you have custom versions or other non-provider libs, add them here.
COPY requirements.txt /requirements.txt

# 5. Install Python dependencies
#    Use pip to install your custom Python libraries.
#    --no-cache-dir: Reduces image size.
#    If you need to install provider packages (e.g., a specific version not available otherwise)
#    you can put them here, but the standard way is via AIRFLOW_PROVIDER_REQUIREMENTS env var in docker-compose.
RUN pip install --no-cache-dir -r /requirements.txt

# 6. (Optional) Copy custom scripts, plugins, or configurations
#    If you have custom Airflow plugins (e.g., custom operators, hooks)
#    or configuration files that need to be part of the image.
# COPY plugins/ /opt/airflow/plugins/
# COPY custom_config.ini /opt/airflow/custom_config.ini

# 7. Switch back to the 'airflow' user
#    It's crucial for security and proper Airflow operation.
USER airflow

# The CMD directive (what runs when the container starts) is handled by the base Airflow image.
# Do NOT include CMD unless you are completely overriding Airflow's entrypoint.
# Default Airflow images handle the entrypoint and command.