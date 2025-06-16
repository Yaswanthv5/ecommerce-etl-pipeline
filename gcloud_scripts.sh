gcloud iam service-accounts create data-pipeline --description="" --display-name="Pipeline-executor"

gsutil mb -p batch-processing-de -c STANDARD -l US gs://raw-data/
gsutil mb -p batch-processing-de -c STANDARD -l US gs://silver-data/
gsutil mb -p batch-processing-de -c STANDARD -l US gs://stagging/
gsutil mb -p batch-processing-de -c STANDARD -l US gs://final-data/

gcloud services enable bigquery.googleapis.com \
storage.googleapis.com \
pubsub.googleapis.com \
databricks.googleapis.com \
dataproc.googleapis.com \
cloudcomposer.googleapis.com

