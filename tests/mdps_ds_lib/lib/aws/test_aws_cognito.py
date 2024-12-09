from unittest import TestCase

from mdps_ds_lib.lib.aws.aws_cognito import AwsCognito


class TestAwsCognitor(TestCase):
    def test_01(self):
        cognito = AwsCognito('us-west-2_yaOw3yj0z')
        wphyo_groups = cognito.get_groups('wphyo')
        self.assertTrue(isinstance(wphyo_groups, list), f'response is not list. {wphyo_groups}')
        self.assertTrue(len(wphyo_groups) > 0, f'empty list')
        print(wphyo_groups)

        result = cognito.add_user_to_group('wphyo', 'MMM')
        print(result)
        wphyo_groups = cognito.get_groups('wphyo')
        print(wphyo_groups)
        result = cognito.remove_user_from_group('wphyo', 'MMM')
        print(result)
        result = cognito.remove_user_from_group('wphyo', 'MMM')
        print(result)
        wphyo_groups = cognito.get_groups('wphyo')
        print(wphyo_groups)
        return
