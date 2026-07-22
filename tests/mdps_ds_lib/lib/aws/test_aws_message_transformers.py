import json
from unittest import TestCase

from mdps_ds_lib.lib.aws.aws_message_transformers import AwsMessageTransformers


class TestAwsMessageTransformers(TestCase):
    def test_01(self):
        event = {'Records': [{'EventSource': 'aws:sns', 'EventVersion': '1.0', 'EventSubscriptionArn': 'arn:aws:sns:us-west-2:429178552491:uds-sbx-cumulus-mock_daac_cnm_sns:35fc364f-2c1a-4139-af3f-bbc2921ea50b', 'Sns': {'Type': 'Notification', 'MessageId': '0ea4a024-e0d6-5c2c-a250-ff06f4c21805', 'TopicArn': 'arn:aws:sns:us-west-2:429178552491:uds-sbx-cumulus-mock_daac_cnm_sns', 'Subject': None, 'Message': '{"collection": "DAAC:MOCK:UDS_UNIT_COLLECTION", "identifier": "URN:NASA:UNITY:UDS_LOCAL_TEST:DEV:UDS_UNIT_COLLECTION___2407291400:abcd.1234.efgh.test_file05", "submissionTime": "2024-07-29T22:04:03.918874Z", "provider": "UDS_LOCAL_TEST", "version": "1.6.0", "product": {"name": "UDS_UNIT_COLLECTION___2407291400", "dataVersion": "9098", "files": [{"type": "data", "name": "abcd.1234.efgh.test_file05.data.stac.json", "uri": "https://uds-distribution-placeholder/uds-sbx-cumulus-staging/URN:NASA:UNITY:UDS_LOCAL_TEST:DEV:UDS_UNIT_COLLECTION___2407291400/URN:NASA:UNITY:UDS_LOCAL_TEST:DEV:UDS_UNIT_COLLECTION___2407291400:abcd.1234.efgh.test_file05/abcd.1234.efgh.test_file05.data.stac.json", "checksumType": "md5", "checksum": "unknown", "size": -1}, {"type": "metadata", "name": "abcd.1234.efgh.test_file05.cmr.xml", "uri": "https://uds-distribution-placeholder/uds-sbx-cumulus-staging/URN:NASA:UNITY:UDS_LOCAL_TEST:DEV:UDS_UNIT_COLLECTION___2407291400/URN:NASA:UNITY:UDS_LOCAL_TEST:DEV:UDS_UNIT_COLLECTION___2407291400:abcd.1234.efgh.test_file05/abcd.1234.efgh.test_file05.cmr.xml", "checksumType": "md5", "checksum": "da19420ead0e92f6855da49269e06017", "size": 1788}]}}', 'Timestamp': '2024-07-29T22:04:04.098Z', 'SignatureVersion': '1', 'Signature': 'qLCKInsVIif1kGSM5128Nio6p+eWLZG8Cpl/f7thLhqN0RjiKSRIpk60fmTuHJBVs33Fu34WqukGiI4ywxFkpPUdUG1NBTjS/0pLjkLotD78tNs2C0JCjqbSoqrvR2ftB7AQhq+Dpwg7Lx60wirDDK7ocUH3ZP3uQMef9Kj2vGdyvBA3Cd2PecwkGHpnjs4LBZgLBcIGrjMCaHJBw1D9kRUAC2F+EgphmP327HAM6zuemHsNjWaS7gYeR9sDlOnhpSgW3Nx7CBMRA9C0v4BS/my50luqq1TXkhfvvtNI9q6yXV58175a6Zx2xyFiSG67QeKeubJo+YCNrwS+dJz2Cw==', 'SigningCertUrl': 'https://sns.us-west-2.amazonaws.com/SimpleNotificationService-60eadc530605d63b8e62a523676ef735.pem', 'UnsubscribeUrl': 'https://sns.us-west-2.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-west-2:429178552491:uds-sbx-cumulus-mock_daac_cnm_sns:35fc364f-2c1a-4139-af3f-bbc2921ea50b', 'MessageAttributes': {}}}]}
        input_event = AwsMessageTransformers().get_message_from_sns_event(event)
        self.assertTrue('collection' in input_event, f'missing collection')
        self.assertTrue('identifier' in input_event, f'missing identifier')
        return

    def test_02(self):
        s3_msg_str = "{\"Records\":[{\"eventVersion\":\"2.1\",\"eventSource\":\"aws:s3\",\"awsRegion\":\"us-gov-west-1\",\"eventTime\":\"2022-02-07T17:31:04.498Z\",\"eventName\":\"ObjectCreated:Put\",\"userIdentity\":{\"principalId\":\"AWS:AROAWM7XM4I6Z3NL2ST2J:wphyo\"},\"requestParameters\":{\"sourceIPAddress\":\"128.149.246.219\"},\"responseElements\":{\"x-amz-request-id\":\"FM1CHA780PBP0YEY\",\"x-amz-id-2\":\"Vp12Q/ok1+Y/0WonTCoUjCCREZhJU3CO82uDbve6m6FqJsFGTMBcLdunqeMLmQ11ZECV6z2WFsak6EbjdIZTi/jL+crmwops\"},\"s3\":{\"s3SchemaVersion\":\"1.0\",\"configurationId\":\"all-obj-create\",\"bucket\":{\"name\":\"lsmd-data-bucket\",\"ownerIdentity\":{\"principalId\":\"440216117821\"},\"arn\":\"arn:aws-us-gov:s3:::lsmd-data-bucket\"},\"object\":{\"key\":\"manual_test/zipped_upload/jpl.calendar.2022.png\",\"size\":841141,\"eTag\":\"1477b70ad2cd03be3d72a49dc58fb52a\",\"sequencer\":\"0062015756ACFAA1FD\"}}}]}"

        sns_msg = {
          "Type": "Notification",
          "MessageId": "95df01b4-ee98-5cb9-9903-4c221d41eb5e",
          "TopicArn": "arn:aws:sns:us-gov-west-1:440216117821:my-s3-topic",
          "Subject": "Amazon S3 Notification",
          "Message": s3_msg_str,
          "Timestamp": "2024-02-07T19:37:27.321Z"
        }

        event = {
            "Records": [
                {
                    "messageId": "6210f778-d081-4ae9-a861-8534d612dfae",
                    "receiptHandle": "AQEBk55DchogyQzpVsH1A4YEj4K/PcVuIG9Em/a6/4AHIA4G5vLPiHVElNiuMfYc1ussk2U//JwZbD788Fv8u6W22L3AJ1U8EIcGJ57aibpmd6tSCWLS5q5FA4u2X2Jq5z+lCX5NZXzNDYMqMJaCGtBkcYi4a9LDXtD+U7HWX0V8OPhFFF2a1qUu+E05c16f5OmE7wRJ3SFrRmtJOhp2DigKKsw6VJtZklTm6uILMOL1ETOTlbA02dhF16fjcXlAACirDp0Yo9pi91FrpEljOYkqAO9AX4WMbEjAPZrnaATfYmRqCTOlnrIK8xvgEPgIu/OOub7KBYh6AQn7U8QBNoASkXkn31dqyM2I+KosKy2VeJO9cjPTahhXtkW7zUFA6863Czt2oHqL6Rvwsjr+7TikfQ==",
                    "body": json.dumps(sns_msg),
                    "attributes": {
                        "ApproximateReceiveCount": "6",
                        "SentTimestamp": "1644255065441",
                        "SenderId": "AIDALVP5ID7KAVBU2CQ3O",
                        "ApproximateFirstReceiveTimestamp": "1644255065441"
                    },
                    "messageAttributes": {},
                    "md5OfBody": "00cb0a5ed122862537ab6115dae36f69",
                    "eventSource": "aws:sqs",
                    "eventSourceARN": "arn:aws-us-gov:sqs:us-gov-west-1:440216117821:send_records_to_es",
                    "awsRegion": "us-gov-west-1"
                }
            ]
        }
        result = AwsMessageTransformers().sqs_sns(event)
        result1 = AwsMessageTransformers().get_s3_from_sns(result)
        print(result1)