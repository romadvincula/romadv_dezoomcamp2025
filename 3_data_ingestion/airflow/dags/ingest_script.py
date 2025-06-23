import os
from time import time
import pandas as pd
from sqlalchemy import create_engine
import pyarrow.parquet as pq

from google.cloud import storage
# from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

CHUNK_SIZE=10000

# def read_parquet_in_chunks(file_path, chunk_size=10000):
#     pq_file = pq.ParquetFile(file_path)
#     for batch in pq_file.iter_batches(batch_size=chunk_size):
#         df = batch.to_pandas()
#         yield df

def ingest_by_read_row_group(user, password, host, port, db, table_name, file):
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    print(table_name, file)

    # read parquet file
    pq_file = pq.ParquetFile(file)

    # upload by row groups
    print(f"num row groups: {pq_file.num_row_groups}")
    for i in range(0, pq_file.num_row_groups):
        t_start = time()
        table = pq_file.read_row_group(i)
        df = table.to_pandas()
        print(f"i: {i}\n{df.head(5)}")

        # do simple transformations
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        if i == 0:
            df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()
        print(f'inserted row group {i + 1} in %.3f seconds' % (t_end - t_start))

def ingest_by_batch(user, password, host, port, db, table_name, file):
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    print(table_name, file)

    # read parquet file
    pq_file = pq.ParquetFile(file)
    
    # upload in chunks
    for i, batch in enumerate(pq_file.iter_batches(batch_size=CHUNK_SIZE)):
        t_start = time()
        df = batch.to_pandas()

        # do simple transformations
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        if i == 0:
            df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()
        print(f'inserted chunk {i + 1} in %.3f seconds' % (t_end - t_start))


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file, **context):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

    context['ti'].xcom_push(key='object_url', value=f"{bucket.name}/{object_name}")



