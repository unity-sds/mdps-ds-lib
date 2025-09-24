import os
import tempfile
from glob import glob
from unittest import TestCase

from mdps_ds_lib.lib.utils.file_utils import FileUtils


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

    def test_copy_dir(self):
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            src_dir = os.path.join(tmp_dir_name, 'src_dir')
            dest_dir1 = os.path.join(tmp_dir_name, 'dest_dir1')
            dest_dir2 = os.path.join(tmp_dir_name, 'dest_dir2')

            src_dir1 = os.path.join(tmp_dir_name, 'src_dir', 'child1')
            src_dir2 = os.path.join(tmp_dir_name, 'src_dir', 'child2', 'g-child1')
            dest_dir22 = os.path.join(tmp_dir_name, 'dest_dir2', 'child2', 'g-child1')
            FileUtils.mk_dir_p(src_dir1)
            FileUtils.mk_dir_p(src_dir2)
            FileUtils.mk_dir_p(dest_dir22)

            FileUtils.write_json(os.path.join(src_dir1, 'test1.json'), {'test': 1}, True, False, True)
            FileUtils.write_json(os.path.join(src_dir2, 'test2.json'), {'test': 1}, True, False, True)
            FileUtils.write_json(os.path.join(src_dir2, 'test3.json'), {'test': 1}, True, False, True)
            FileUtils.write_json(os.path.join(dest_dir22, 'test3.json'), {'test': 100}, True, False, True)
            FileUtils.write_json(os.path.join(dest_dir22, 'test4.json'), {'test': 88}, True, False, True)

            FileUtils.copy_dir(src_dir, dest_dir1)
            FileUtils.copy_dir(src_dir, dest_dir2, True)

            result = [k for k in glob(os.path.join(dest_dir1, '**/*'), recursive=True)]
            print(len(result), 6, f'wrong length: {result}')
            result = [k for k in glob(os.path.join(dest_dir2, '**/*'), recursive=True)]
            print(len(result), 7, f'wrong length: {result}')

            with self.assertRaises(FileExistsError) as cm:
                FileUtils.copy_dir(src_dir, dest_dir2)

        return
