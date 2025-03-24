import json
import os
from unittest import TestCase

from mdps_ds_lib.ds_client.auth_token.token_abstract import TokenAbstract
from mdps_ds_lib.ds_client.auth_token.token_factory import TokenFactory
from mdps_ds_lib.ds_client.ds_client_admin import DsClientAdmin
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

    def test_query_collections(self):
        os.environ['TRUST_ENV'] = 'TRUE'
        os.environ['PASSWORD_TYPE'] = 'PARAM_STORE'
        os.environ['USERNAME'] = '/unity/uds/user/wphyo/username'
        os.environ['PASSWORD'] = '/unity/uds/user/wphyo/dwssap'
        os.environ['CLIENT_ID'] = '71g0c73jl77gsqhtlfg2ht388c'
        os.environ['COGNITO_URL'] = 'https://cognito-idp.us-west-2.amazonaws.com'

        os.environ['TOKEN_FACTORY'] = 'COGNITO'
        token_retriever: TokenAbstract = TokenFactory().get_instance(os.getenv('TOKEN_FACTORY'))
        client = DsClientUser(token_retriever, 'https://d3vc8w9zcq658.cloudfront.net', 'data-sbx')
        print(client.query_collections(10))
        print(client.query_collections_next())
        return

    def test_query_single_collection(self):
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
        client.collection = 'DDD-01'
        client.collection_venue = '001'

        print(client.query_single_collection())
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

    def test_query_granules02(self):
        os.environ['TRUST_ENV'] = 'TRUE'
        os.environ['PASSWORD_TYPE'] = 'PARAM_STORE'
        os.environ['USERNAME'] = '/unity/uds/user/wphyo/username'
        os.environ['PASSWORD'] = '/unity/uds/user/wphyo/dwssap'
        os.environ['CLIENT_ID'] = '71g0c73jl77gsqhtlfg2ht388c'
        os.environ['COGNITO_URL'] = 'https://cognito-idp.us-west-2.amazonaws.com'

        os.environ['TOKEN_FACTORY'] = 'COGNITO'
        token_retriever: TokenAbstract = TokenFactory().get_instance(os.getenv('TOKEN_FACTORY'))
        client = DsClientUser(token_retriever, 'https://d3vc8w9zcq658.cloudfront.net', 'am-uds-dapa')
        # client = DsClientAdmin(token_retriever, 'http://localhost:8005', 'data')
        'uds_local_test:DEV1:CHRP_16_DAY_REBIN___5/items?limit=1000'
        client.urn = 'urn'
        client.org = 'nasa'
        client.project = 'unity'
        client.tenant = 'uds_local_test'
        client.tenant_venue = 'DEV1'
        client.collection = 'CHRP_16_DAY_REBIN'
        client.collection_venue = '5'

        client.urn = 'urn'
        client.org = 'nasa'
        client.project = 'unity'
        client.tenant = 'uds_local_test'
        client.tenant_venue = 'DEV1'
        client.collection = 'CHRP_16_DAY_REBIN'
        client.collection_venue = '10'
        # 3749
        result = client.query_granules(limit=50)  # bbox='-114,32.5,-113,33.5'
        print(json.dumps(result, indent=4))
        # print(json.dumps(client.query_granules_next(), indent=4))
        return

    def test_query_custom_properties(self):
        os.environ['TRUST_ENV'] = 'TRUE'
        os.environ['PASSWORD_TYPE'] = 'PARAM_STORE'
        os.environ['USERNAME'] = '/unity/uds/user/wphyo/username'
        os.environ['PASSWORD'] = '/unity/uds/user/wphyo/dwssap'
        os.environ['CLIENT_ID'] = '71g0c73jl77gsqhtlfg2ht388c'
        os.environ['COGNITO_URL'] = 'https://cognito-idp.us-west-2.amazonaws.com'

        os.environ['TOKEN_FACTORY'] = 'COGNITO'
        token_retriever: TokenAbstract = TokenFactory().get_instance(os.getenv('TOKEN_FACTORY'))
        client = DsClientUser(token_retriever, 'https://d3vc8w9zcq658.cloudfront.net', 'am-uds-dapa')
        # client = DsClientAdmin(token_retriever, 'http://localhost:8005', 'data')
        'uds_local_test:DEV1:CHRP_16_DAY_REBIN___5/items?limit=1000'
        client.urn = 'urn'
        client.org = 'nasa'
        client.project = 'unity'
        client.tenant = 'uds_local_test'
        client.tenant_venue = 'DEV1'
        client.collection = 'CHRP_16_DAY_REBIN'
        client.collection_venue = '5'

        # 3749
        result = client.query_custom_properties()  # bbox='-114,32.5,-113,33.5'
        print(json.dumps(result, indent=4))
        # print(json.dumps(client.query_granules_next(), indent=4))
        return

    def test_query_single_granule(self):
        os.environ['TRUST_ENV'] = 'TRUE'
        os.environ['PASSWORD_TYPE'] = 'PARAM_STORE'
        os.environ['USERNAME'] = '/unity/uds/user/wphyo/username'
        os.environ['PASSWORD'] = '/unity/uds/user/wphyo/dwssap'
        os.environ['CLIENT_ID'] = '71g0c73jl77gsqhtlfg2ht388c'
        os.environ['COGNITO_URL'] = 'https://cognito-idp.us-west-2.amazonaws.com'

        os.environ['TOKEN_FACTORY'] = 'COGNITO'
        token_retriever: TokenAbstract = TokenFactory().get_instance(os.getenv('TOKEN_FACTORY'))
        client = DsClientUser(token_retriever, 'https://d3vc8w9zcq658.cloudfront.net', 'am-uds-dapa')
        client.urn = 'urn'
        client.org = 'nasa'
        client.project = 'unity'
        client.tenant = 'uds_local_test'
        client.tenant_venue = 'DEV1'
        client.collection = 'CHRP_16_DAY_REBIN'
        client.collection_venue = '10'
        client.granule = 'SNDR.SS1330.CHIRP.20230101T0000.m06.g001.L1_J1.std.v02_48.G.200101070318_REBIN'
        # urn:nasa:unity:uds_local_test:DEV1:CHRP_16_DAY_REBIN___10:SNDR.SS1330.CHIRP.20230101T0000.m06.g001.L1_J1.std.v02_48.G.200101070318_REBIN
        print(client.query_single_granule())
        return


    def test_update_admin(self):
        os.environ['TRUST_ENV'] = 'TRUE'
        os.environ['PASSWORD_TYPE'] = 'PARAM_STORE'
        os.environ['USERNAME'] = '/unity/uds/user/wphyo/username'
        os.environ['PASSWORD'] = '/unity/uds/user/wphyo/dwssap'
        os.environ['CLIENT_ID'] = '71g0c73jl77gsqhtlfg2ht388c'
        os.environ['COGNITO_URL'] = 'https://cognito-idp.us-west-2.amazonaws.com'

        os.environ['TOKEN_FACTORY'] = 'COGNITO'
        token_retriever: TokenAbstract = TokenFactory().get_instance(os.getenv('TOKEN_FACTORY'))
        client = DsClientAdmin(token_retriever, 'https://d3vc8w9zcq658.cloudfront.net', 'data-sbx')

        # URN:NASA:UNITY:UDS_LOCAL_TEST_3:DEV:DDD-01___001:test_file10
        client.urn = 'URN'
        client.org = 'NASA'
        client.project = 'UNITY'
        client.tenant = 'UDS_LOCAL_TEST_3'
        client.tenant_venue = 'DEV'
        client.collection = 'DDD-01'
        client.collection_venue = '001'
        client.add_admin_group(['CREATE', 'READ', 'DELETE'], 'wphyo')
        return

    def test_delete_single_granule(self):
        os.environ['TRUST_ENV'] = 'TRUE'
        os.environ['PASSWORD_TYPE'] = 'PARAM_STORE'
        os.environ['USERNAME'] = '/unity/uds/user/wphyo/username'
        os.environ['PASSWORD'] = '/unity/uds/user/wphyo/dwssap'
        os.environ['CLIENT_ID'] = '71g0c73jl77gsqhtlfg2ht388c'
        os.environ['COGNITO_URL'] = 'https://cognito-idp.us-west-2.amazonaws.com'

        os.environ['TOKEN_FACTORY'] = 'COGNITO'
        token_retriever: TokenAbstract = TokenFactory().get_instance(os.getenv('TOKEN_FACTORY'))
        client = DsClientUser(token_retriever, 'https://d3vc8w9zcq658.cloudfront.net', 'data-sbx')

        # URN:NASA:UNITY:UDS_LOCAL_TEST_3:DEV:DDD-01___001:test_file10
        client.urn = 'URN'
        client.org = 'NASA'
        client.project = 'UNITY'
        client.tenant = 'UDS_LOCAL_TEST_3'
        client.tenant_venue = 'DEV'
        client.collection = 'DDD-01'
        client.collection_venue = '001'
        client.granule = 'test_file10'
        # urn:nasa:unity:uds_local_test:DEV1:CHRP_16_DAY_REBIN___10:SNDR.SS1330.CHIRP.20230101T0000.m06.g001.L1_J1.std.v02_48.G.200101070318_REBIN
        print(client.delete_single_granule())
        return
