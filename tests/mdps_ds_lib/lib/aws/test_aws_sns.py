from unittest import TestCase
from unittest.mock import MagicMock, patch

from mdps_ds_lib.lib.aws.aws_sns import AwsSns


class TestAwsSns(TestCase):
    def test_01(self):
        sns = AwsSns().set_topic_arn('arn:aws:sns:us-west-2:429178552491:william-test-1')
        result = sns.create_sqs_subscription('arn:aws:sqs:us-west-2:237868187491:william-test-1')
        self.assertEqual(result, '')
        return

    def _make_sns(self):
        with patch('mdps_ds_lib.lib.aws.aws_sns.AwsCred.get_client') as mock_get_client:
            mock_sns_client = MagicMock()
            mock_get_client.return_value = mock_sns_client
            sns = AwsSns()
            sns._AwsSns__sns_client = mock_sns_client
            sns.set_topic_arn('arn:aws:sns:us-west-2:123456789012:test-topic')
            return sns, mock_sns_client

    def test_publish_messages_batch_mismatched_msg_ids_raises(self):
        sns, _ = self._make_sns()
        with self.assertRaises(ValueError) as ctx:
            sns.publish_messages_batch(
                msg_list=['msg1', 'msg2'],
                msg_ids=['id1'],  # length 1, but msg_list has length 2
            )
        self.assertIn('equal length', str(ctx.exception))

    def test_publish_messages_batch_mismatched_msg_attrs_list_raises(self):
        sns, _ = self._make_sns()
        with self.assertRaises(ValueError) as ctx:
            sns.publish_messages_batch(
                msg_list=['msg1', 'msg2'],
                msg_attrs_list=[{'key': 'val'}],  # length 1, but msg_list has length 2
            )
        self.assertIn('equal length', str(ctx.exception))

    def test_publish_messages_batch_matching_lengths_succeeds(self):
        sns, mock_sns_client = self._make_sns()
        mock_sns_client.publish_batch.return_value = {'Successful': [], 'Failed': []}
        result = sns.publish_messages_batch(
            msg_list=['msg1', 'msg2'],
            msg_ids=['id1', 'id2'],
            msg_attrs_list=[{}, {}],
        )
        mock_sns_client.publish_batch.assert_called_once()
        call_kwargs = mock_sns_client.publish_batch.call_args[1]
        entries = call_kwargs['PublishBatchRequestEntries']
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0]['Id'], 'id1')
        self.assertEqual(entries[0]['Message'], 'msg1')
        self.assertEqual(entries[1]['Id'], 'id2')
        self.assertEqual(entries[1]['Message'], 'msg2')

    def test_publish_messages_batch_auto_generated_ids(self):
        sns, mock_sns_client = self._make_sns()
        mock_sns_client.publish_batch.return_value = {'Successful': [], 'Failed': []}
        sns.publish_messages_batch(msg_list=['msg1', 'msg2'])
        call_kwargs = mock_sns_client.publish_batch.call_args[1]
        entries = call_kwargs['PublishBatchRequestEntries']
        self.assertEqual(len(entries), 2)
        # IDs should be auto-generated UUIDs (non-empty strings)
        self.assertTrue(len(entries[0]['Id']) > 0)
        self.assertTrue(len(entries[1]['Id']) > 0)
        self.assertNotEqual(entries[0]['Id'], entries[1]['Id'])

    def test_publish_messages_batch_missing_topic_arn_raises(self):
        with patch('mdps_ds_lib.lib.aws.aws_sns.AwsCred.get_client'):
            sns = AwsSns()
        with self.assertRaises(ValueError) as ctx:
            sns.publish_messages_batch(msg_list=['msg1'])
        self.assertIn('missing topic arn', str(ctx.exception))
