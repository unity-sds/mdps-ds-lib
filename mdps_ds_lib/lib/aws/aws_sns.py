from uuid import uuid4

from docutils.core import Publisher

from mdps_ds_lib.lib.aws.aws_cred import AwsCred


class AwsSns(AwsCred):
    def __init__(self):
        super().__init__()
        self.__sns_client = self.get_client('sns')
        self.__special_sns_client = None
        self.__topic_arn = ''

    def set_topic_arn(self, topic_arn):
        self.__topic_arn = topic_arn
        return self

    def set_external_role(self, external_role_arn: str, external_role_session_name: str, external_role_duration: int =900):
        sts_client = self.get_client('sts')
        assumed_role = sts_client.assume_role(
            RoleArn=external_role_arn,
            RoleSessionName=external_role_session_name,
            DurationSeconds=external_role_duration  # 12 hours max
        )

        credentials = assumed_role['Credentials']

        self.__special_sns_client = self.get_session().client(
            "sns",
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken'],
        )
        return self

    def publish_message(self, msg_str: str, is_with_daac_role: bool=False, msg_attrs={}):
        if self.__topic_arn == '':
            raise ValueError('missing topic arn to publish message')
        if is_with_daac_role and self.__special_sns_client is None:
            raise ValueError('sns client with external role NOT set')
        my_sns = self.__special_sns_client if is_with_daac_role else self.__sns_client
        real_attrs = {k: {"DataType": "String", "StringValue": v} for k, v in msg_attrs.items() if v is not None and len(v.strip()) > 0}
        response = my_sns.publish(
            TopicArn=self.__topic_arn,
            # TargetArn='string',  # not needed coz of we are using topic arn
            # PhoneNumber='string',  # not needed coz of we are using topic arn
            Message=msg_str,
            # Subject='optional string', \
            # MessageStructure='string',
            MessageAttributes=real_attrs,
            # MessageAttributes={
            #     'string': {
            #         'DataType': 'string',
            #         'StringValue': 'string',
            #         'BinaryValue': b'bytes'
            #     }
            # },
            # MessageDeduplicationId='string',
            # MessageGroupId='string'
        )
        return response

    def publish_messages_batch(self, msg_list: list, is_with_daac_role: bool=False, msg_attrs_list: list=None, msg_ids: list=None):
        # https://docs.aws.amazon.com/boto3/latest/reference/services/sns/client/publish_batch.html
        if self.__topic_arn == '':
            raise ValueError('missing topic arn to publish message')
        if is_with_daac_role and self.__special_sns_client is None:
            raise ValueError('sns client with external role NOT set')
        my_sns = self.__special_sns_client if is_with_daac_role else self.__sns_client
        if msg_ids is None:
            msg_ids = [str(uuid4()) for _ in range(len(msg_list))]
        if msg_attrs_list is None:
            real_attrs_list = [{} for _ in range(len(msg_list))]
        else:
            real_attrs_list = [{k: {"DataType": "String", "StringValue": v} for k, v in msg_attrs.items() if v is not None and len(v.strip()) > 0} for msg_attrs in msg_attrs_list]

        if len(msg_ids) != len(msg_list) or len(msg_list) != len(real_attrs_list):
            raise ValueError(f'msg_ids ({len(msg_ids)}), msg_list ({len(msg_list)}), and msg_attrs_list ({len(real_attrs_list)}) must all have equal length')
        sending_msg_list = [
            {
                'Id': x,  # Required
                'Message': y,  # Required
                'MessageAttributes': z
            } for x,y,z in zip(msg_ids, msg_list, real_attrs_list)
        ]
        response = my_sns.publish_batch(
            TopicArn=self.__topic_arn,
            PublishBatchRequestEntries=sending_msg_list,
            # PublishBatchRequestEntries=[
            #     {
            #         'Id': 'string', # Required
            #         'Message': 'string', # Required
            #         'Subject': 'string',
            #         'MessageStructure': 'string',
            #         'MessageAttributes': {
            #             'string': {
            #                 'DataType': 'string', # Required
            #                 'StringValue': 'string',
            #                 'BinaryValue': b'bytes'
            #             }
            #         },
            #         'MessageDeduplicationId': 'string',
            #         'MessageGroupId': 'string'
            #     },
            # ]
        )
        return response

    def create_sqs_subscription(self, sqs_arn):
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/subscribe.html
        if self.__topic_arn == '':
            raise ValueError('missing topic arn to publish message')
        response = self.__sns_client.subscribe(
            TopicArn=self.__topic_arn,
            Protocol='sqs',
            Endpoint=sqs_arn,  # For the sqs protocol, the endpoint is the ARN of an Amazon SQS queue.
            # Attributes={
            #     'string': 'string'
            # },
            ReturnSubscriptionArn=True  # if the API request parameter ReturnSubscriptionArn is true, then the value is always the subscription ARN, even if the subscription requires confirmation.
        )
        return response['SubscriptionArn']
