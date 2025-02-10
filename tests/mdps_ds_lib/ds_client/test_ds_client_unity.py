import os
from unittest import TestCase

from mdps_ds_lib.ds_client.auth_token.token_abstract import TokenAbstract
from mdps_ds_lib.ds_client.auth_token.token_factory import TokenFactory
from mdps_ds_lib.ds_client.ds_client_user import DsClientUser


class TestDsClientAdmin(TestCase):
    def test_query_granules_across_collections(self):
        os.environ['TRUST_ENV'] = 'TRUE'
        os.environ['PASSWORD_TYPE'] = 'PARAM_STORE'
        os.environ['USERNAME'] = '/unity/uds/user/wphyo/username'
        os.environ['PASSWORD'] = '/unity/uds/user/wphyo/dwssap'
        os.environ['CLIENT_ID'] = '71g0c73jl77gsqhtlfg2ht388c'
        os.environ['COGNITO_URL'] = 'https://cognito-idp.us-west-2.amazonaws.com'

        os.environ['TOKEN_FACTORY'] = 'COGNITO'
        token_retriever: TokenAbstract = TokenFactory().get_instance(os.getenv('TOKEN_FACTORY'))
        client = DsClientUser(token_retriever, 'https://d3vc8w9zcq658.cloudfront.net', 'data-sbx')

        client.urn = 'URN'
        client.org = 'NASA'
        client.project = 'UNITY'
        client.tenant = 'UDS_LOCAL_TEST_3'
        client.tenant_venue = 'DEV'
        result = client.query_granules_across_collections(sort_keys='+properties.datetime,-id')
        print(result)
        print(client.query_granules_next())
        return

    def test_query_granules(self):
        os.environ['TRUST_ENV'] = 'TRUE'
        os.environ['PASSWORD_TYPE'] = 'PARAM_STORE'
        os.environ['USERNAME'] = '/unity/uds/user/wphyo/username'
        os.environ['PASSWORD'] = '/unity/uds/user/wphyo/dwssap'
        os.environ['CLIENT_ID'] = '71g0c73jl77gsqhtlfg2ht388c'
        os.environ['COGNITO_URL'] = 'https://cognito-idp.us-west-2.amazonaws.com'

        os.environ['TOKEN_FACTORY'] = 'COGNITO'
        token_retriever: TokenAbstract = TokenFactory().get_instance(os.getenv('TOKEN_FACTORY'))
        client = DsClientUser(token_retriever, 'https://d3vc8w9zcq658.cloudfront.net', 'data-sbx')
        # client = DsClientAdmin(token_retriever, 'http://localhost:8005', 'data')

        client.urn = 'URN'
        client.org = 'NASA'
        client.project = 'UNITY'
        client.tenant = 'UDS_LOCAL_TEST_3'
        client.tenant_venue = 'DEV'
        client.collection = 'DDD-01'
        client.collection_venue = '001'
        result = client.query_granules(sort_keys='+properties.datetime,-id')  # bbox='-114,32.5,-113,33.5'
        print(result)
        print(client.query_granules_next())
        return
