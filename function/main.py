# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START functions_cloudevent_storage]
import functions_framework
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
import tempfile

# create storage client
storage_client = storage.Client()
# create bigquery client
bigquery_client = bigquery.Client()


# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def controller(cloud_event):
    data = cloud_event.data
    bucket = data["bucket"]
    name = data["name"]
    print("running add column....")
    df = add_column(bucket, name)
    print("runnning upload to destinaton bucket...")
    upload_to_destination_bucket(df, bucket)
    upload_to_bq(df)

def add_column(bucket, name):
    # get bucket with name
    bucket = storage_client.get_bucket(bucket)
    # get source bucket data as blob
    blob = bucket.blob(name)
    # convert to string
    data = blob.download_to_filename("/tmp/temp.csv")
    # change to pandas dataframe
    df = pd.read_csv('/tmp/temp.csv')
    # add new column and propagate cells with the file name
    df["file_name"] = name
    #print out head of the 
    # 
    df.columns = df.columns.str.replace(' ','_')
    # 
    df.columns = df.columns.str.replace('/','_')

    return df

def upload_to_destination_bucket(df, bucket):
    # connect to the destination bucket
    dest_bucket = storage_client.get_bucket("REPLACE")
    # upload file to destination bucket directly as dataframe 
    dest_bucket.blob('processed.csv').upload_from_string(df.to_csv(index=False), 'text/csv')

# [END functions_cloudevent_storage]

def upload_to_bq(df): 
    # set bigquery job config
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    # enter table id [PROJECT.DATASET.TABLENAME]
    table_id = "REPLACE"

    # instatiate job
    job = bigquery_client.load_table_from_dataframe(df, table_id, job_config=job_config)  # Make an API request.
    job.result()  # Wait for the job to complete.

    #get table data to check upload
    table = bigquery_client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows and {} columns to {}".format(table.num_rows, len(table.schema), table_id))
