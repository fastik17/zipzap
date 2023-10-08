import os
import sys
import logging
import tempfile

import requests
import zipfile
import environs
import concurrent.futures

import boto3

env = environs.Env()
env.read_env()

AWS_ACCESS_KEY_ID = env.str('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = env.str('AWS_SECRET_ACCESS_KEY')
AWS_BUCKET_NAME = env.str('AWS_BUCKET_NAME')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_s3_connection():
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    logger.info('S3 connection created successfully')
    return s3_client


def download_zip_to_temporary(zip_url):
    try:
        temp_dir = tempfile.mkdtemp()
        temp_zip_path = os.path.join(temp_dir, 'downloaded.zip')

        response = requests.get(zip_url)
        if response.status_code != 200:
            raise Exception(logger.error(f"Failed to download ZIP from {zip_url}"))

        with open(temp_zip_path, 'wb') as temp_zip_file:
            temp_zip_file.write(response.content)

        logger.info('Successfully downloaded')
        return temp_zip_path

    except Exception as e:
        logger.error(f"Failed to download ZIP: {str(e)}")
        return None


def extract_zip(zip_file_path, extract_dir):
    try:
        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            zip_ref.extractall(extract_dir)
    except Exception as e:
        logger.error(f"Failed to extract ZIP: {str(e)}")


def upload_file_to_s3(s3_client, local_file_path, s3_key):
    try:
        s3_client.upload_file(local_file_path, AWS_BUCKET_NAME, s3_key)
        logger.info(f"Uploaded {s3_key}")
    except Exception as e:
        logger.error(f"Failed to upload {s3_key}: {str(e)}")


def upload_files_to_s3_concurrently(s3_client, local_directory):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []

        for root, _, files in os.walk(local_directory):
            for file in files:
                local_file_path = os.path.join(root, file)
                s3_key = os.path.relpath(local_file_path, local_directory)
                futures.append(executor.submit(upload_file_to_s3, s3_client, local_file_path, s3_key))

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(str(e))


def main():
    zip_url = input("Enter the URL to the ZIP archive: ")
    local_directory = "temporary_zip_archives"

    temp_zip_path = download_zip_to_temporary(zip_url)

    if not temp_zip_path:
        sys.exit(1)

    try:
        extract_zip(temp_zip_path, local_directory)
    except Exception as e:
        logger.error(f"Failed to extract ZIP: {str(e)}")
        sys.exit(1)

    s3_client = create_s3_connection()

    if not os.path.exists(local_directory):
        logger.error(f"Local directory '{local_directory}' does not exist.")
        sys.exit(1)

    try:
        upload_files_to_s3_concurrently(s3_client, local_directory)
        print(f"Files uploaded to S3 bucket '{AWS_BUCKET_NAME}' successfully!")
    except Exception as e:
        logger.error(f"Failed to upload files to S3: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
