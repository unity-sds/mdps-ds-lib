from unittest import TestCase

from mdps_ds_lib.lib.aws.aws_cognito import AwsCognito


class TestAwsCognitor(TestCase):
    def test_01(self):
        cognito = AwsCognito('us-west-2_yaOw3yj0z')
        sample_group_name = 'UNIT_TEST_GROUP_WPHYO'
        username = 'wphyo'
        result = cognito.add_group(sample_group_name)
        print(result)
        result = cognito.add_user_to_group(username, sample_group_name)
        print(result)
        wphyo_groups = cognito.get_groups(username)
        self.assertTrue(isinstance(wphyo_groups, list), f'response is not list. {wphyo_groups}')
        self.assertTrue(len(wphyo_groups) > 0, f'empty list')
        self.assertTrue(sample_group_name in wphyo_groups, f'empty list')
        result = cognito.remove_user_from_group(username, sample_group_name)
        print(result)
        wphyo_groups = cognito.get_groups(username)
        self.assertTrue(isinstance(wphyo_groups, list), f'response is not list. {wphyo_groups}')
        self.assertTrue(sample_group_name not in wphyo_groups, f'empty list')
        result = cognito.delete_group(sample_group_name)
        print(result)
        return
