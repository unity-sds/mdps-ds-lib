from unittest import TestCase

from mdps_ds_lib.lib.aws.aws_sns import AwsSns


class TestAwsSns(TestCase):
    def test_01(self):
        sns = AwsSns().set_topic_arn('arn:aws:sns:us-west-2:429178552491:william-test-1')
        result = sns.create_sqs_subscription('arn:aws:sqs:us-west-2:237868187491:william-test-1')
        self.assertEqual(result, '')
        return
