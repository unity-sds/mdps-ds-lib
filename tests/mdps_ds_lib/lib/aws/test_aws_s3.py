from unittest import TestCase

from mdps_ds_lib.lib.aws.aws_s3 import AwsS3


class TestAwsS3(TestCase):
    def test_delete_one_01(self):
        bucket = 'uds-sbx-cumulus-staging'
        s3 = AwsS3()
        single_path = 'tmp/unit-test/file1.txt'
        s3.set_s3_url(f's3://{bucket}/{single_path}').upload_bytes('This is a test'.encode())
        d = s3.delete_one()
        print(d)
        return

    def test_delete_multiple_01(self):
        bucket = 'uds-sbx-cumulus-staging'
        s3 = AwsS3()
        deleting_s3_urls = []
        for i in range(10):
            single_path = f'tmp/unit-test/file{i}.txt'
            deleting_s3_urls.append(f's3://{bucket}/{single_path}')
            s3.set_s3_url(deleting_s3_urls[-1]).upload_bytes(f'This is a test - {i}'.encode())
        d = s3.delete_multiple(s3_urls=deleting_s3_urls)
        print(d)
        return

    def test_delete_multiple_02(self):
        bucket = 'uds-sbx-cumulus-staging'
        s3 = AwsS3()
        deleting_s3_paths = []
        for i in range(10):
            single_path = f'tmp/unit-test/file{i}.txt'
            deleting_s3_paths.append(single_path)
            s3.set_s3_url(f's3://{bucket}/{single_path}').upload_bytes(f'This is a test - {i}'.encode())
        d = s3.delete_multiple(s3_bucket=bucket, s3_paths=deleting_s3_paths)
        print(d)
        return

    def test_delete_multiple_03(self):
        bucket = 'uds-sbx-cumulus-staging'
        s3 = AwsS3()
        with self.assertRaises(ValueError) as context:
            s3.delete_multiple(s3_bucket=bucket, s3_paths=[])
        self.assertTrue(str(context.exception).startswith('unable to delete empty list of URLs or Paths'))

        with self.assertRaises(ValueError) as context:
            s3.delete_multiple(s3_urls=[])
        self.assertTrue(str(context.exception).startswith('unable to delete empty list of URLs or Paths'))

        with self.assertRaises(ValueError) as context:
            s3.delete_multiple(s3_bucket='', s3_paths=['a', 'b', 'c'])
        self.assertTrue(str(context.exception).startswith('empty s3 bucket for paths'))

        with self.assertRaises(ValueError) as context:
            s3.delete_multiple(s3_urls=['s3://a/b', 's3://b/c'])
        self.assertTrue(str(context.exception).startswith('unable to delete multiple s3 buckets'))
        return
