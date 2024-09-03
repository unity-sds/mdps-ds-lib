from unittest import TestCase

from src.uds_lib.lib.utils.file_utils import FileUtils


class TestFileUtils(TestCase):
    def test_is_relative_path(self):
        self.assertFalse(FileUtils.is_relative_path('https://www.google.com'))
        self.assertFalse(FileUtils.is_relative_path('s3://bucket/key'))
        self.assertFalse(FileUtils.is_relative_path('ftp://localhost:22/sahara'))
        self.assertFalse(FileUtils.is_relative_path('file:///user/wphyo/test'))
        self.assertFalse(FileUtils.is_relative_path('/user/wphyo/test'))
        self.assertTrue(FileUtils.is_relative_path('test'))
        self.assertTrue(FileUtils.is_relative_path('./test'))
        self.assertTrue(FileUtils.is_relative_path('../test'))
        return
