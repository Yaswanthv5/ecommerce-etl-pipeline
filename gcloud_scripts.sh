#!/bin/bash

PROJECT_ID = "batch-processing-de"

echo "activating the required API's------------"
gcloud services enable bigquery.googleapis.com \
storage.googleapis.com \
pubsub.googleapis.com \
databricks.googleapis.com \
dataproc.googleapis.com \
cloudcomposer.googleapis.com

echo "Creating the Service Account for the Data-Pipeline"
gcloud iam service-accounts create data-pipeline --description="" --display-name="Pipeline-executor" || echo "service account already exists"

gsutil mb -p batch-processing-de -c STANDARD -l US gs://raw-data/
gsutil mb -p batch-processing-de -c STANDARD -l US gs://silver-data/
gsutil mb -p batch-processing-de -c STANDARD -l US gs://stagging/
gsutil mb -p batch-processing-de -c STANDARD -l US gs://final-data/

