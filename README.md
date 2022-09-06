# add-column-csv-gcs-to-bq
Creates a cloud function which accepts a csv, adds a column to the csv, then uploads this as a table to BigQuery

Main Doc: https://cloud.google.com/functions/docs/tutorials/storage
https://cloud.google.com/functions/docs/tutorials/storage#functions-prepare-environment-python

1. Create two regional buckets, where the name is a globally unique bucket name, and REGION is the region in which you plan to deploy your functions:

The first is the source bucket you will uplaod the CSV to. 
gsutil mb -l REGION gs://sourcebucket

The second is the processing bucket which will upload the CSV to BigQuery. 
gsutil mb -l REGION gs://destinationbucket

To use Cloud Storage functions, grant the pubsub.publisher role to the Cloud Storage service account:

PROJECT_ID=$(gcloud config get-value project)
PROJECT_NUMBER=$(gcloud projects list --filter="project_id:$PROJECT_ID" --format='value(project_number)')

SERVICE_ACCOUNT=$(gsutil kms serviceaccount -p $PROJECT_NUMBER)

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member serviceAccount:$SERVICE_ACCOUNT \
  --role roles/pubsub.publisher

git clone https://https://github.com/eric-lyons/add-column-csv-gcs-to-bq.git

cd python-docs-samples/functions/v2/storage/

gcloud functions deploy add-column-to-csv-function \
--gen2 \
--runtime=python310 \
--region=REGION \
--source=. \
--entry-point=controller \
--trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
--trigger-event-filters="bucket=SOURCEBUCKETADD"

