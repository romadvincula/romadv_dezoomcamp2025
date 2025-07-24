#!/usr/bin/env python
# !pip install google-cloud-storage

import requests
from google.cloud import storage
import os

# https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet
URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
DATALAKE_BUCKET = "romadv-nyc-taxi1-datalake"

def upload_to_gcs(bucket_name, object_name, file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    try:
      blob.upload_from_filename(file)
      print(f"Uploaded {bucket_name}/{object_name}")

    except Exception as e:
      print(f"Failed to upload {bucket_name}/{object_name}")
      raise e

def extract_load_to_gcs(year_month, service):
    filename = f"{service}_tripdata_{year_month}.parquet"
    request_url = f"{URL_PREFIX}{filename}"

    try:
        response = requests.get(request_url)
        open(filename, 'wb').write(response.content)
        print(f"Local: {filename}")

    except Exception as e:
        print(f"Failed to download {request_url}")
        raise e

    upload_to_gcs(DATALAKE_BUCKET, f"{service}/landing/{filename}", filename)


extract_load_to_gcs("2023-01", "yellow")

