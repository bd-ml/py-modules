import logging
import os
import shutil
import unittest

import minio_client

sample_file_name = "sample.txt"


def setup_minio_envs():
    os.environ["MINIO_SERVER_URL"] = "localhost:9000"
    os.environ["MINIO_ACCESS_KEY"] = "not@user"
    os.environ["MINIO_SECRET_KEY"] = "not@secret"


def write_sample_file(file_dir):
    os.mkdir(file_dir)
    filepath = file_dir + "/" + sample_file_name
    f = open(filepath, "w")
    f.write("Hello, world!")
    f.close()


def remove_dir(file_dir):
    shutil.rmtree(file_dir)


def get_test_name(test_id):
    # extract the test name
    arr = test_id.split(".")
    name = arr[len(arr) - 1]

    return name


def get_bucket_name(test_id):
    name = get_test_name(test_id)
    return name.replace("_", "-")


class TestMinioClient(unittest.TestCase):
    def setUp(self):
        setup_minio_envs()
        self.bucket = get_bucket_name(self.id())
        self.minio = minio_client.MinioClient(self.bucket)

    def tearDown(self):
        # cleanup sample data (if exist)
        data_dir = get_test_name(self.id())
        try:
            remove_dir(data_dir)
        except FileNotFoundError:
            logging.info("Directory: " + data_dir + " does not exist")
        except:
            self.fail("Failed to remove directory: " + data_dir)

        # delete the bucket from minio
        try:
            self.minio.delete_bucket()
        except Exception as e:
            self.fail(e)

    def test_create_bucket(self):
        try:
            self.minio.create_bucket()
        except Exception as e:
            self.fail(e)

    def test_upload_file(self):
        data_dir = get_test_name(self.id())
        filepath = data_dir + "/" + sample_file_name

        write_sample_file(data_dir)

        # upload file
        try:
            self.minio.upload_file(filepath)
        except Exception as e:
            self.fail(e)

    def test_download_file(self):
        data_dir = get_test_name(self.id())
        filepath = data_dir + "/" + sample_file_name

        write_sample_file(data_dir)

        # upload file
        try:
            self.minio.upload_file(filepath)
        except Exception as e:
            self.fail(e)

        # download file
        output_file = data_dir + "/download/" + sample_file_name
        try:
            self.minio.download_file(filepath, output_file)
        except Exception as e:
            self.fail(e)

        # compare source file and downloaded file
        src_file = open(filepath, "r")
        src_content = src_file.read()
        src_file.close()

        downloaded_file = open(output_file, "r")
        downloaded_content = downloaded_file.read()
        downloaded_file.close()

        self.assertEqual(src_content, downloaded_content)

    def test_delete_file(self):
        data_dir = get_test_name(self.id())
        filepath = data_dir + "/" + sample_file_name

        write_sample_file(data_dir)

        # upload file
        try:
            self.minio.upload_file(filepath)
        except Exception as e:
            self.fail(e)

        # delete the sample file
        try:
            self.minio.delete_file(filepath)
        except Exception as e:
            self.fail(e)


if __name__ == '__main__':
    unittest.main()
