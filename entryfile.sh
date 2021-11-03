#!/bin/bash

echo "----- Activating Google Cloud Services -----"

gcloud services enable bigquery.googleapis.com bigquerystorage.googleapis.com bigqueryreservation.googleapis.com storage.googleapis.com storage-component.googleapis.com dataproc.googleapis.com run.googleapis.com cloudbuild.googleapis.com || exit 1

echo "----- Defining global variables -----"

PROJECT=$(gcloud config get-value project)
DATA_BUCKET="neoway-challenge3"
BIGQUERY_BUCKET="bigquery-tempbucket-nc3"
SCRIPTS_BUCKET="dataproc_shin_scripts3"

echo " PROJECT $PROJECT "
echo " DATA_BUCKET $DATA_BUCKET "
echo " BIGQUERY_BUCKET $BIGQUERY_BUCKET "
echo " SCRIPTS_BUCKET $SCRIPTS_BUCKET "

echo "----- Creating main bucket and uploading data -----"

echo "> create bucket"
gsutil mb -p "$PROJECT" -c standard -l US -b on gs://"$DATA_BUCKET"
echo "> upload data"
gsutil cp -r data/ gs://"$DATA_BUCKET"/lnd || exit 1

echo "----- Creating BigQuery Dataset and BigQuery Bucket -----"
echo "> create bucket"
gsutil mb -p "$PROJECT" -c standard -l US -b on gs://"$BIGQUERY_BUCKET"

echo "> create big query dataset"
bq --location=US mk -d \
--default_table_expiration 360000 \
--description "Feature store" \
--project_id "$PROJECT" \
feature_store 

echo "----- Transforming notebooks into python scripts and upload it to a new bucket -----"

echo "> convert jupyter to python"
jupyter nbconvert --to script ./dataproc_scripts/*.ipynb || exit 1
echo "> create bucket"
gsutil mb -p "$PROJECT" -c standard -l US -b on gs://"$SCRIPTS_BUCKET"
echo "> upload scripts"
gsutil cp -r dataproc_scripts/*.py gs://"$SCRIPTS_BUCKET"/ || exit 1


echo "----- Run batch jobs on Dataproc -----"

echo "> run dataproc workflow"
gcloud dataproc workflow-templates instantiate-from-file --file workflow_dataproc.yaml --region us-east1 || exit 1

echo "----- Serving API -----"

echo "> build serving api"
gcloud builds submit --tag gcr.io/"$PROJECT"/nc-serving-api ./serving_api || exit 1
echo "> deploy api to cloud run"
gcloud run deploy nc-serving-api --region us-east1 --image gcr.io/"$PROJECT"/nc-serving-api --port 8000 --allow-unauthenticated --platform=managed --set-env-vars "GOOGLE_PROJECT=$PROJECT,DATA_BUCKET=$DATA_BUCKET" || exit 1