import os
import re
from datetime import datetime
from sys import argv


# os.environ['GITHUB_WORKSPACE'] = '/Users/wphyo/Projects/unity/unity-data-services'
# os.environ['PR_TITLE'] = 'breaking: test1'
# os.environ['PR_NUMBER'] = '342'
# PR_NUMBER: ${{ github.event.number }}
# PR_TITLE: ${{ github.event.pull_request.title }}


class VersionUpdate:
    def __init__(self):
        self.pr_title = os.environ.get('PR_TITLE')
        self.pr_number = os.environ.get('PR_NUMBER')
        self.root_dir = os.environ.get('GITHUB_WORKSPACE')
        self.pr_title = self.pr_title.strip().lower()
        self.major1, self.minor1, self.patch1 = 0, 0, 0
        self.change_log_line, self.setup_py_path = '', ''
        # Define a regular expression pattern to match the version
        self.version_pattern = r"version\s*=\s*['\"](.*?)['\"]"

    def get_new_bumps_from_title(self):
        if self.pr_title.startswith('breaking'):
            self.major1, self.minor1, self.patch1 = 1, 0, 0
            self.change_log_line = '### Added'
        elif self.pr_title.startswith('feat'):
            self.major1, self.minor1, self.patch1 = 0, 1, 0
            self.change_log_line = '### Changed'
        elif self.pr_title.startswith('fix') or self.pr_title.startswith('chore'):  # TODO chore is bumping up version?
            self.major1, self.minor1, self.patch1 = 0, 0, 1
            self.change_log_line = '### Fixed'
        else:
            raise RuntimeError(f'invalid PR Title: {self.pr_title}')

    def get_new_version(self, current_version):
        # Parse the current version and increment it
        major, minor, patch = map(int, current_version.split('.'))
        if self.major1 > 0:
            return f"{major + 1}.0.0"
        if self.minor1 > 0:
            return f"{major}.{minor + 1}.0"
        return f"{major}.{minor}.{patch + 1}"

    def update_version(self, is_release=False):
        # Specify the path to your setup.py file
        self.setup_py_path = os.path.join(self.root_dir, 'pyproject.toml')
        # Read the contents of setup.py
        with open(self.setup_py_path, 'r') as setup_file:
            setup_contents = setup_file.read()

        # Find the current version using the regular expression pattern
        current_version = re.search(self.version_pattern, setup_contents).group(1)
        print(f'current_version: {current_version}')
        if is_release and '.dev' not in current_version:
            raise ValueError(f'no development updates to release: {current_version}')

        main_version, dev_version = current_version.split('.dev') if '.dev' in current_version else [current_version, '0' *6]
        dev_version = '.'.join([dev_version[i:i+2] for i in range(0, 6, 2)])
        if is_release:
            print('this is a formal release')
            self.major1, self.minor1, self.patch1 = [int(k) for k in dev_version.split('.')]
            new_version = self.get_new_version(main_version)
        else:
            print('this is a feature merge')
            self.get_new_bumps_from_title()
            new_version = ''.join([f'{i}:02' for i in self.get_new_version(dev_version).split('.')])
            new_version = f'{main_version}.dev{new_version}'
        print(f'new_version: {new_version}')

        # Replace the old version with the new version in setup.py
        updated_setup_contents = re.sub(self.version_pattern, f'version="{new_version}"', setup_contents)

        # Write the updated contents back to setup.py
        with open(self.setup_py_path, 'w') as setup_file:
            setup_file.write(updated_setup_contents)

        print(f"Version bumped up from {current_version} to {new_version}")
        return new_version

    def update_change_log(self):
        change_log_path = os.path.join(self.root_dir, 'CHANGELOG.md')
        change_log_blob = [
            f'## [{new_version_from_setup}] - {datetime.now().strftime("%Y-%m-%d")}',
            self.change_log_line,
            f'- [#{self.pr_number}](https://github.com/unity-sds/unity-data-services/pull/{self.pr_number}) {self.pr_title}',
            ''
        ]
        with open(change_log_path, 'r') as change_log_file:
            change_logs = change_log_file.read().splitlines()
        pattern = r"## \[\d+\.\d+\.\d+\] - \d{4}-\d{2}-\d{2}"

        inserting_line = 0
        for i, each_line in enumerate(change_logs):
            if re.search(pattern, each_line):
                inserting_line = i
                break
        # inserting_line = inserting_line - 1 if inserting_line > 0 else inserting_line
        for i, each_line in enumerate(change_log_blob):
            change_logs.insert(inserting_line, each_line)
            inserting_line += 1
        change_logs = '\n'.join(change_logs)

        with open(change_log_path, 'w') as change_log_file:
            change_log_file.write(change_logs)
        return


if __name__ == '__main__':
    is_releasing = argv[1].strip().upper() == 'RELEASE'
    version_update = VersionUpdate()
    new_version_from_setup = version_update.update_version(is_releasing)
    version_update.update_change_log()
