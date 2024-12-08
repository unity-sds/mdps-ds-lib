import logging

from requests_aws4auth import AWS4Auth

from mdps_ds_lib.lib.aws.aws_cred import AwsCred
from mdps_ds_lib.lib.aws.es_middleware import ESMiddleware
from elasticsearch import Elasticsearch, RequestsHttpConnection
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth

LOGGER = logging.getLogger(__name__)

# https://opensearch.org/docs/latest/clients/python-low-level/

class OsMiddlewareAws(ESMiddleware):
    def __init__(self, index, base_url, port=443) -> None:
        super().__init__(index, base_url, port)
        base_url = base_url.replace('https://', '')  # hide https
        self._index = index
        service = 'es'
        aws_cred = AwsCred()
        credentials = aws_cred.get_session().get_credentials()
        aws_auth = AWSV4SignerAuth(credentials, aws_cred.region, service)

        self._engine = OpenSearch(
            hosts=[{'host': base_url, 'port': port}],
            http_auth=aws_auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            pool_maxsize=20
        )
