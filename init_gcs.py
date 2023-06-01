"""Initialize Google Storage data
"""

import logging
from os import scandir, environ
import os
import sys
import time
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def upload_contents(client, directory, bucket_name=None):
    """Upload recursively contents of specified directory.

    Args:
        client (google.cloud.storage.Client): Google Storage Client.
        directory (str): upload directory path.
        bucket_name (str, optional): Bucket name to use for upload. Defaults to
        None.
    """
    for entry in scandir(directory):
        print(entry.path)
        if entry.is_dir():
            if bucket_name is not None:
                # This is a normal directory inside a bucket
                upload_contents(client, directory + '/' +
                                entry.name, bucket_name)
            else:
                # This is a bucket directory
                upload_contents(client, directory + '/' +
                                entry.name, entry.name)
        elif entry.is_file():
            if bucket_name is not None:
                tokens = entry.path.split(bucket_name + '/')
                bucket_obj = client.bucket(bucket_name)
                if len(tokens) > 1:
                    gs_path = tokens[1]
                    blob_obj = bucket_obj.blob(gs_path)
                    blob_obj.upload_from_filename(entry.path)

time.sleep(5)
os.environ["STORAGE_EMULATOR_HOST"] = f"http://localhost:4443"
storage_client = storage.Client(credentials=AnonymousCredentials(),
                                project="1")

bucket = storage_client.bucket('golden')
bucket.location = 'eu'
bucket.create()

# Scan import data directory
upload_contents(storage_client, '/docker-entrypoint-init-storage', "golden")

logger.info('Successfully imported bucket data!')
logger.info('List:')
for bucket in storage_client.list_buckets():
    print(f'Bucket: {bucket}')
    for blob in bucket.list_blobs():
        print(f'|_Blob: {blob}')

# All OK
sys.exit(0)