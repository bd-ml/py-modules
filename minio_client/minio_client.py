import logging
import os

from minio import Minio

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(level=LOG_LEVEL)


class MinioClient:
    def __init__(self, bucket):
        self.bucket = bucket
        try:
            self.client = Minio(
                endpoint=os.getenv("MINIO_SERVER_URL"),
                access_key=os.getenv("MINIO_ACCESS_KEY"),
                secret_key=os.getenv("MINIO_SECRET_KEY"),
                secure=False,
            )

        except Exception as e:
            raise Exception("Failed to establish connection with minio server.") from e

    def create_bucket(self):
        # Create the bucket if it does not exist
        if self.client.bucket_exists(self.bucket):
            logging.info("Bucket: " + self.bucket + " already exist")
        else:
            logging.info("Creating bucket: " + self.bucket)
            try:
                self.client.make_bucket(self.bucket)
            except Exception as e:
                raise Exception("Failed to create bucket: " + self.bucket) from e

    def clear_bucket(self):
        logging.info("Clearing bucket: " + self.bucket)
        objects = self.client.list_objects(bucket_name=self.bucket, recursive=True)
        # remove the individual objects
        for obj in objects:
            try:
                self.client.remove_object(self.bucket, obj.object_name)
            except Exception as e:
                raise Exception("Failed to remove object: " + obj) from e

    def delete_bucket(self):
        # at first, clear the bucket
        self.clear_bucket()

        # now, remove the bucket
        logging.info("Removing bucket: " + self.bucket)
        try:
            self.client.remove_bucket(self.bucket)
        except Exception as e:
            raise Exception("Failed to remove bucket: " + self.bucket) from e

    def upload_file(self, filename):
        # Create bucket if the file does not exist
        self.create_bucket()

        # Put the data into the bucket
        try:
            logging.debug("Uploading file: " + filename)
            self.client.fput_object(self.bucket, filename, filename)
        except Exception as e:
            raise Exception("Failed to upload object: " + filename) from e

        logging.debug("Successfully uploaded file: " + filename)

    def download_file(self, filename, output_file):
        # Download the file
        try:
            logging.debug("Downloading file: " + filename)
            self.client.fget_object(self.bucket, filename, output_file)
        except Exception as e:
            raise Exception("Failed to download file: " + filename) from e

    def delete_file(self, filename):
        # remove the file
        try:
            logging.debug("Removing file: " + filename)
            self.client.remove_object(self.bucket, filename)
        except Exception as e:
            raise Exception("Failed to remove file: " + filename) from e
