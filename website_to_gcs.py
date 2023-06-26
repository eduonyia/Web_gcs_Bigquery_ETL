import io
import os
import requests
import pandas as pd
import gzip
import tempfile
from google.cloud import storage

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""


init_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"

# switch out the bucketname
BUCKET = os.getenv("GCP_GCS_BUCKET", "my-gcs-bucket1")


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python

    This pushes files or files to the gcs bucket location
    """
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year, service):
    for i in range(12):
        # sets the month part of the file_name string
        month = "0" + str(i + 1)
        month = month[-2:]

        # csv file_name
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"

        # download it using requests via into a tempfile a pandas df
        with tempfile.TemporaryDirectory() as tmpdirname:
            request_url = f"{init_url}{service}/{file_name}"

            r = requests.get(request_url)

            open(f"{tmpdirname}/{file_name}", "wb").write(r.content)
            print(f"Local: {file_name}")

            # read it back into a parquet file
            df = pd.read_csv(
                f"{tmpdirname}/{file_name}", encoding="ISO-8859-1", low_memory=False
            )
            file_name = file_name.replace(".csv.gz", ".parquet")

            # yellow_tripdata_2021-01.parquet
            df.to_parquet(f"{tmpdirname}/{file_name}", engine="pyarrow")
            print(f"Parquet: {file_name}")

            # upload it to gcs
            upload_to_gcs(
                BUCKET, f"{service}/{year}/{file_name}", f"{tmpdirname}/{file_name}"
            )
            # upload_to_gcs(BUCKET, f"/yellow/2020/yellow_tripdata_2021-01.parquet", f'{tmpdirname}/{file_name}')

            print(f"GCS: {service}/{year}/{file_name}")


years = ["2019"]
services = ["fhv", "green", "yellow"]

for service in services:
    for year in years:
        web_to_gcs(year, service)
