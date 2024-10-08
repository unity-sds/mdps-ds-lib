# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import hashlib
import json
import os
import re
import zlib
from functools import partial
from pathlib import Path
from subprocess import Popen, PIPE
from distutils.dir_util import copy_tree


class FileUtils:
    @staticmethod
    def remove_if_exists(file_path):
        if os.path.exists(file_path) and os.path.isfile(file_path):
            os.remove(file_path)
        return

    @staticmethod
    def is_relative_path(file_path):
        regex = re.compile('^([A-Za-z0-9-]+)://.+$')
        current_granule_id = re.findall(regex, file_path)
        if len(current_granule_id) > 0:
            return False
        return not os.path.isabs(file_path)

    @staticmethod
    def mk_dir_p(dir_path):
        Path(dir_path).mkdir(parents=True, exist_ok=True)
        return

    @staticmethod
    def copy_dir(source_dir, dest_dir):
        copy_tree(source_dir, dest_dir)
        return

    @staticmethod
    def gunzip_file_os(zipped_file_path, output_file_path=None):
        if not FileUtils.file_exist(zipped_file_path):
            raise ValueError('missing file: {}'.format(zipped_file_path))
        session = Popen(['gunzip', zipped_file_path], stdout=PIPE, stderr=PIPE)
        stdout, stderr = session.communicate()
        if stderr:
            raise RuntimeError('error while gunzipping the file with Popen. filename: {}. error: {}'.format(zipped_file_path, stderr))
        default_output_path = zipped_file_path[:-3]
        if not FileUtils.file_exist(default_output_path):
            raise ValueError('missing gunzipped file: {}'.format(default_output_path))
        if output_file_path is None:
            output_file_path = default_output_path
        if FileUtils.file_exist(output_file_path) and default_output_path != output_file_path:
            os.renames(default_output_path, output_file_path)
        return output_file_path

    @staticmethod
    def get_checksum(file_path, is_md5=False, chunk_size=25 * 2**20):
        with open(file_path, mode='rb') as f:
            d = hashlib.md5() if is_md5 else hashlib.sha512()
            for buf in iter(partial(f.read, chunk_size), b''):
                d.update(buf)
        return d.hexdigest()

    @staticmethod
    def get_size(file_path):
        return os.stat(file_path).st_size

    @staticmethod
    def file_exist(path):
        return Path(path).is_file()

    @staticmethod
    def dir_exist(path):
        return Path(path).is_dir()

    @staticmethod
    def del_file(path):
        if FileUtils.file_exist(path):
            Path(path).unlink()
        return

    @staticmethod
    def read_json(path):
        if not os.path.exists(path):
            raise IOError('{} does not exist'.format(path))
        with open(path, 'r') as ff:
            try:
                return json.loads(ff.read())
            except:
                return None

    @staticmethod
    def write_json(file_path, json_obj, overwrite=False, append=False, prettify=False):
        if os.path.exists(file_path) and not overwrite:
            raise ValueError('{} already exists, and not overwriting'.format(file_path))
        with open(file_path, 'a' if append else 'w') as ff:
            json_str = json.dumps(json_obj, indent=4) if prettify else json.dumps(json_obj)
            ff.write(json_str)
        return
