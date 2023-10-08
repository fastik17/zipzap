import unittest
from unittest.mock import Mock, patch
import environs


from zip_upload import create_s3_connection, download_zip_to_temporary, extract_zip, upload_file_to_s3


env = environs.Env()
env.read_env()


AWS_ACCESS_KEY_ID = env.str('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = env.str('AWS_SECRET_ACCESS_KEY')


class TestZipS3(unittest.TestCase):

    @patch('boto3.client')
    def test_create_s3_connection(self, mock_boto3_client):
        mock_s3_client = Mock()
        mock_boto3_client.return_value = mock_s3_client

        s3_connection = create_s3_connection()

        self.assertEqual(s3_connection, mock_s3_client)
        mock_boto3_client.assert_called_once_with(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )

    @patch('requests.get')
    @patch('tempfile.mkdtemp')
    @patch('builtins.open', create=True)
    def test_download_zip_to_temporary(self, mock_open, mock_mkdtemp, mock_requests_get):
        mock_requests_get.side_effect = Exception("Download failed")

        mock_temp_dir = "mocked_temp_dir"
        mock_mkdtemp.return_value = mock_temp_dir

        mock_file_handle = Mock()
        mock_open.return_value = mock_file_handle

        zip_url = "http://example.com/zip_file.zip"
        temp_zip_path = download_zip_to_temporary(zip_url)

        self.assertIsNone(temp_zip_path)
        mock_open.assert_not_called()
        mock_file_handle.write.assert_not_called()
        mock_mkdtemp.assert_called_once()

    @patch('zipfile.ZipFile')
    def test_extract_zip(self, mock_zipfile):
        mock_zip_ref = Mock()
        mock_zipfile.return_value.__enter__.return_value = mock_zip_ref

        zip_file_path = "path/to/your/zipfile.zip"
        extract_dir = "path/to/extract/directory"
        extract_zip(zip_file_path, extract_dir)

        mock_zipfile.assert_called_once_with(zip_file_path, "r")
        mock_zip_ref.extractall.assert_called_once_with(extract_dir)

    @patch('boto3.client')
    def test_upload_file_to_s3(self, mock_boto3_client):
        mock_s3_client = Mock()
        mock_boto3_client.return_value = mock_s3_client

        local_file_path = "path/local_file.zip"
        s3_key = "s3/key.txt"
        upload_file_to_s3(mock_s3_client, local_file_path, s3_key)

        # Assertions
        mock_s3_client.upload_file.assert_called_once_with(local_file_path, "your_bucket_name", s3_key)


if __name__ == '__main__':
    unittest.main()
