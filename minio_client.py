import os
import logging
from minio import Minio
from minio.deleteobjects import DeleteObject

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(level=LOG_LEVEL)


def get_client():
    try:
        client = Minio(
            endpoint=os.getenv("MINIO_SERVER_URL", "127.0.0.1:9000"),
            access_key=os.getenv("MINIO_ACCESS_KEY"),
            secret_key=os.getenv("MINIO_SECRET_KEY"),
            secure=False,
        )

    except Exception as e:
        raise Exception("Failed to establish connection with minio server.") from e

    return client


def create_bucket(bucket):
    client = get_client()
    # Create the bucket if it does not exist
    if client.bucket_exists(bucket):
        logging.info("Bucket: " + bucket + " already exist")
    else:
        logging.info("Creating bucket: " + bucket)
        try:
            client.make_bucket(bucket)
        except Exception as e:
            raise Exception("Failed to create bucket: " + bucket) from e


def clear_bucket(bucket):
    logging.info("Clearing bucket: " + bucket)

    client = get_client()
    objects = client.list_objects(bucket_name=bucket, recursive=True)
    # remove the individual objects
    for obj in objects:
        try:
            client.remove_object(bucket, obj.object_name)
        except Exception as e:
            raise Exception("Failed to remove object: " + obj) from e


def delete_bucket(bucket):
    # at first, clear the bucket
    clear_bucket(bucket)

    # now, remove the bucket
    logging.info("Removing bucket: " + bucket)
    client = get_client()
    try:
        client.remove_bucket(bucket)
    except Exception as e:
        raise Exception("Failed to remove bucket: " + bucket) from e


def upload_file(bucket, filename):
    # Create client
    client = get_client()

    # Create bucket if does not exist
    create_bucket(bucket)

    # Put the data into the bucket
    try:
        logging.debug("Uploading file: " + filename)
        client.fput_object(bucket, filename, filename)
    except Exception as e:
        raise Exception("Failed to upload object: " + filename) from e

    logging.debug("Successfully uploaded file: " + filename)


def download_file(bucket, filename, output_file):
    # Create client
    client = get_client()

    # Download the file
    try:
        logging.debug("Downloading file: " + filename)
        client.fget_object(bucket, filename, output_file)
    except Exception as e:
        raise Exception("Failed to download file: " + filename) from e


def delete_file(bucket, filename):
    client = get_client()
    # remove the file
    try:
        logging.debug("Removing file: " + filename)
        client.remove_object(bucket, filename)
    except Exception as e:
        raise Exception("Failed to remove file: " + filename) from e
