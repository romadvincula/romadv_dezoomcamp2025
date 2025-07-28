import functions_framework
import os
import requests
from google.cloud import storage


URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
DATALAKE_BUCKET = os.environ.get("DATALAKE_BUCKET")

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
        response.raise_for_status()
        
        open(filename, 'wb').write(response.content)
        print(f"Local: {filename}")

    except Exception as e:
        print(f"Failed to download {request_url}")
        raise e

    upload_to_gcs(DATALAKE_BUCKET, f"landing/{service}/{filename}", filename)


@functions_framework.http
def hello_http(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and 'year_month' in request_json and 'service' in request_json:
        year_month = request_json['year_month']
        service = request_json['service']

    elif request_args and 'name' in request_args:
        name = request_args['name']
        return 'Hello {}\n!'.format(name)
    else:
        return "Invalid arguments.\n"

    try:
        extract_load_to_gcs(year_month, service)
        return "Done!"

    except Exception as e:
        return f"Error: {e}\n"