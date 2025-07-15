import json
import os
from datetime import datetime
from time import sleep
from unittest import TestCase

from dotenv import load_dotenv
from pystac import Collection, Extent, SpatialExtent, TemporalExtent, Provider, Summaries
from requests import HTTPError

from mdps_ds_lib.ds_client.auth_token.token_abstract import TokenAbstract
from mdps_ds_lib.ds_client.auth_token.token_factory import TokenFactory
from mdps_ds_lib.ds_client.ds_client_admin import DsClientAdmin
from mdps_ds_lib.ds_client.ds_client_user import DsClientUser
from mdps_ds_lib.lib.utils.file_utils import FileUtils
from mdps_ds_lib.stac_fast_api_client.sfa_client_factory import SFAClientFactory


class TestDsClientAdmin(TestCase):
    def setUp(self) -> None:
        super().setUp()
        load_dotenv()
    def test_01_admin(self):
        os.environ['TRUST_ENV'] = 'TRUE'
        os.environ['TOKEN_FACTORY'] = 'COGNITO'
        token_retriever: TokenAbstract = TokenFactory().get_instance(os.getenv('TOKEN_FACTORY'))
        client = DsClientAdmin(token_retriever, 'https://d3vc8w9zcq658.cloudfront.net', 'am-uds-dapa')
        # client.setup_database()

        client.urn = 'URN'
        client.org = 'NASA'
        client.project = 'UNITY'
        client.tenant = 'UDS_LOCAL_TEST_3'
        client.tenant_venue = 'DEV'

        client.add_admin_group(['CREATE', 'READ', 'DELETE'], 'Unity_Viewer')
        return

    def test_stac_get_collections_01(self):
        my_session = 'b00e27e4-f7f6-4ee9-9f84-91fad175e438'
        sfa_client = SFAClientFactory().get_instance(SFAClientFactory.COOKIE_AUTH, auth_key='mod_auth_openidc_session', auth_value=my_session, ds_url='https://www.dev.mdps.mcp.nasa.gov:4443', ds_stage='stac_fast_api')
        my_collection = 'URN:NASA:UNITY:UDS_LOCAL_TEST_3:DEV:DDD-01___001'
        result = sfa_client.get_collections()
        print(json.dumps(result, indent=4))
        # self.assertTrue('type' in result, f'missing type in result')
        # self.assertEqual('FeatureCollection', result['type'], 'wrong FeatureCollection')
        # self.assertTrue('features' in result, f'missing features in result')
        return

    def test_stac_get_single_collection_01(self):
        my_session = 'e5f96453-d146-41e3-aba9-00e8a21fb826'
        sfa_client = SFAClientFactory().get_instance(SFAClientFactory.COOKIE_AUTH, auth_key='mod_auth_openidc_session', auth_value=my_session, ds_url='https://www.dev.mdps.mcp.nasa.gov:4443', ds_stage='stac_fast_api')
        my_collection = 'URN:NASA:UNITY:UDS_LOCAL_TEST_3:DEV:DDD-01___001'
        result = sfa_client.get_collection(my_collection)
        print(json.dumps(result, indent=4))
        # self.assertTrue('type' in result, f'missing type in result')
        # self.assertEqual('FeatureCollection', result['type'], 'wrong FeatureCollection')
        # self.assertTrue('features' in result, f'missing features in result')
        return

    def test_stac_get_single_collection_02(self):
        my_session = '3618762f-283e-4841-a48a-26f77e453edf'
        sfa_client = SFAClientFactory().get_instance(SFAClientFactory.COOKIE_AUTH, auth_key='mod_auth_openidc_session',
                                                     auth_value=my_session, ds_url='https://www.dev.mdps.mcp.nasa.gov:4443',
                                                     ds_stage='stac_fast_api')
        my_collection = 'Invalid-Collection'
        try:
            result = sfa_client.get_collection(my_collection)
            print(json.dumps(result, indent=4))
            self.assertTrue(False, 'needs to fail')
        except Exception as e:
            self.assertTrue('NotFoundError' in str(e))
        # self.assertTrue('type' in result, f'missing type in result')
        # self.assertEqual('FeatureCollection', result['type'], 'wrong FeatureCollection')
        # self.assertTrue('features' in result, f'missing features in result')
        return

    def test_stac_create_collection_01(self):
        my_session = '1b384b78-e2de-4363-a462-da90b1b18c86'
        sfa_client = SFAClientFactory().get_instance(SFAClientFactory.COOKIE_AUTH, auth_key='mod_auth_openidc_session',
                                                     auth_value=my_session, ds_url='https://www.dev.mdps.mcp.nasa.gov:4443',
                                                     ds_stage='stac_fast_api')
        my_collection = 'URN:NASA:UNITY:UDS_LOCAL_TEST_3:DEV:DDD-02___001'
        new_collection = Collection(id=my_collection,
                                    description='TODO',
                                    extent=Extent(SpatialExtent([[-180, -90, 180, 90]]),
                                                  TemporalExtent([[datetime.utcnow(), datetime.utcnow()]])),
                                    title=my_collection,
                                    providers=[Provider('NA')],
                                    summaries=Summaries({
                                    }),
                                    )
        collection_result = new_collection.to_dict(False, False)
        result = sfa_client.create_collection(collection_result)
        debug = 1
        # self.assertTrue('type' in result, f'missing type in result')
        # self.assertEqual('FeatureCollection', result['type'], 'wrong FeatureCollection')
        # self.assertTrue('features' in result, f'missing features in result')
        return

    def test_stac_fast_get_granules_01(self):
        my_session = 'd55aa6dd-c6af-49fb-9984-88ee8653102f'
        sfa_client = SFAClientFactory().get_instance(SFAClientFactory.COOKIE_AUTH, auth_key='mod_auth_openidc_session', auth_value=my_session, ds_url='https://www.dev.mdps.mcp.nasa.gov:4443', ds_stage='stac_fast_api')
        my_collection = 'URN:NASA:UNITY:UDS_LOCAL_TEST_3:DEV:DDD-01___001'
        result = sfa_client.get_items(my_collection)
        print(json.dumps(result, indent=4))
        self.assertTrue('type' in result, f'missing type in result')
        self.assertEqual('FeatureCollection', result['type'], 'wrong FeatureCollection')
        self.assertTrue('features' in result, f'missing features in result')
        return

    def test_stac_fast_add_granules_01(self):
        """
        curl -v -L -X POST 'https://www.dev.mdps.mcp.nasa.gov:4443/stac_fast_api/collections' \
  -H 'Content-Type:application/json' \
  -H 'cookie: mod_auth_openidc_session=01dc038a-9e14-4f88-b034-4762603729d3' \
-d @/tmp/sample.json
        :return:
        """
        my_session = '8e97d7a5-af90-41fe-9d27-f7ea7835bbf8'
        sfa_client = SFAClientFactory().get_instance(SFAClientFactory.COOKIE_AUTH, auth_key='mod_auth_openidc_session', auth_value=my_session, ds_url='https://www.dev.mdps.mcp.nasa.gov:4443', ds_stage='stac_fast_api')
        my_collection = 'URN:NASA:UNITY:UDS_LOCAL_TEST_3:DEV:DDD-01___001'
        granules = FileUtils.read_json(f'/Users/wphyo/Downloads/stac.fast.api.example.3.json')
        result = sfa_client.create_item(my_collection, granules)
        print(json.dumps(result, indent=4))
        return


    def test_02_custom_metadata(self):
        os.environ['TRUST_ENV'] = 'TRUE'
        os.environ['TOKEN_FACTORY'] = 'COGNITO'
        token_retriever: TokenAbstract = TokenFactory().get_instance(os.getenv('TOKEN_FACTORY'))
        client = DsClientAdmin(token_retriever, 'https://d3vc8w9zcq658.cloudfront.net', 'am-uds-dapa')

        client.urn = 'URN'
        client.org = 'NASA'
        client.project = 'UNITY'
        client.tenant = 'UDS_LOCAL_TEST_3'
        client.tenant_venue = 'DEV'

        client.add_tenant_database_index({
            'tag': {'type': 'keyword'},
            'c_data1': {'type': 'long'},
            'c_data2': {'type': 'boolean'},
            'c_data3': {'type': 'keyword'},
        })
        return

    def test_query_granules_across_collections(self):
        os.environ['TRUST_ENV'] = 'TRUE'
        os.environ['TOKEN_FACTORY'] = 'COGNITO'
        token_retriever: TokenAbstract = TokenFactory().get_instance(os.getenv('TOKEN_FACTORY'))
        client = DsClientUser(token_retriever, 'https://d3vc8w9zcq658.cloudfront.net', 'am-uds-dapa')

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
        os.environ['TOKEN_FACTORY'] = 'COGNITO'
        token_retriever: TokenAbstract = TokenFactory().get_instance(os.getenv('TOKEN_FACTORY'))
        client = DsClientUser(token_retriever, 'https://d3vc8w9zcq658.cloudfront.net', 'am-uds-dapa')
        print(client.query_collections(10))
        print(client.query_collections_next())
        return

    def test_query_single_collection(self):
        os.environ['TRUST_ENV'] = 'TRUE'
        os.environ['TOKEN_FACTORY'] = 'COGNITO'
        token_retriever: TokenAbstract = TokenFactory().get_instance(os.getenv('TOKEN_FACTORY'))
        client = DsClientUser(token_retriever, 'https://d3vc8w9zcq658.cloudfront.net', 'am-uds-dapa')
        client.urn = 'URN'
        client.org = 'NASA'
        client.project = 'UNITY'
        client.tenant = 'UDS_LOCAL_TEST_3'
        client.tenant_venue = 'DEV'
        client.collection = 'DDD-01'
        client.collection_venue = '001'

        print(client.query_single_collection())
        return

    def test_delete_collection(self):
        os.environ['TRUST_ENV'] = 'TRUE'
        os.environ['TOKEN_FACTORY'] = 'COGNITO'
        token_retriever: TokenAbstract = TokenFactory().get_instance(os.getenv('TOKEN_FACTORY'))
        client = DsClientUser(token_retriever, 'https://d3vc8w9zcq658.cloudfront.net', 'am-uds-dapa')

        client.urn = 'URN'
        client.org = 'NASA'
        client.project = 'UNITY'
        client.tenant = 'UDS_LOCAL_TEST_3'
        client.tenant_venue = 'DEV'
        client.collection = 'DDD-01'
        client.collection_venue = '001'

        with self.assertRaises(HTTPError) as context:
            result = client.delete_collection()  # bbox='-114,32.5,-113,33.5'
            print(context)
        self.assertEqual(context.exception.response.status_code, 409)
        return

    def test_create_delete_empty_collection(self):
        os.environ['TRUST_ENV'] = 'TRUE'
        os.environ['TOKEN_FACTORY'] = 'COGNITO'
        token_retriever: TokenAbstract = TokenFactory().get_instance(os.getenv('TOKEN_FACTORY'))
        client = DsClientUser(token_retriever, 'https://d3vc8w9zcq658.cloudfront.net', 'am-uds-dapa')

        client.urn = 'URN'
        client.org = 'NASA'
        client.project = 'UNITY'
        client.tenant = 'UDS_LOCAL_TEST_3'
        client.tenant_venue = 'DEV'
        client.collection = 'DDD-01'
        client.collection_venue = datetime.now().strftime("%Y%m%d%H%M%S")

        result = client.create_new_collection(False)
        print(result)
        sleep(70)
        result = client.delete_collection()  # bbox='-114,32.5,-113,33.5'
        print(result)
        sample_granules = client.query_granules(1)
        return

    def test_query_granules(self):
        os.environ['TRUST_ENV'] = 'TRUE'
        os.environ['TOKEN_FACTORY'] = 'COGNITO'
        token_retriever: TokenAbstract = TokenFactory().get_instance(os.getenv('TOKEN_FACTORY'))
        client = DsClientUser(token_retriever, 'https://d3vc8w9zcq658.cloudfront.net', 'am-uds-dapa')
        # client = DsClientAdmin(token_retriever, 'http://localhost:8005', 'data')

        # URN:NASA:UNITY:UDS_LOCAL_TEST_3:DEV:DDD-01___001
        client.urn = 'URN'
        client.org = 'NASA'
        client.project = 'UNITY'
        client.tenant = 'UDS_LOCAL_TEST_3'
        client.tenant_venue = 'DEV'
        client.collection = 'DDD-01'
        client.collection_venue = '001'
        result = client.query_granules(sort_keys='+properties.datetime,-id')  # bbox='-114,32.5,-113,33.5'

        i = 1
        for each in result['features']:
            from mdps_ds_lib.lib.utils.file_utils import FileUtils
            FileUtils.write_json(f'/tmp/sample_granules_{i}.json', each, overwrite=True, prettify=True)
            i += 1
        for each in client.query_granules_next()['features']:
            from mdps_ds_lib.lib.utils.file_utils import FileUtils
            FileUtils.write_json(f'/tmp/sample_granules_{i}.json', each, overwrite=True, prettify=True)
            i += 1

        # print(json.dumps(result, indent=4))
        # print(json.dumps(client.query_granules_next(), indent=4))
        return

    def test_query_granules02(self):
        os.environ['TRUST_ENV'] = 'TRUE'
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
        os.environ['TOKEN_FACTORY'] = 'COGNITO'
        token_retriever: TokenAbstract = TokenFactory().get_instance(os.getenv('TOKEN_FACTORY'))
        client = DsClientAdmin(token_retriever, 'https://d3vc8w9zcq658.cloudfront.net', 'am-uds-dapa')

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
        os.environ['TOKEN_FACTORY'] = 'COGNITO'
        token_retriever: TokenAbstract = TokenFactory().get_instance(os.getenv('TOKEN_FACTORY'))
        client = DsClientUser(token_retriever, 'https://d3vc8w9zcq658.cloudfront.net', 'am-uds-dapa')

        # URN:NASA:UNITY:UDS_LOCAL_TEST_3:DEV:DDD-01___001:test_file10
        client.urn = 'URN'
        client.org = 'NASA'
        client.project = 'UNITY'
        client.tenant = 'UDS_LOCAL_TEST_3'
        client.tenant_venue = 'DEV'
        client.collection = 'DDD-01'
        client.collection_venue = '001'
        # urn:nasa:unity:uds_local_test:DEV1:CHRP_16_DAY_REBIN___10:SNDR.SS1330.CHIRP.20230101T0000.m06.g001.L1_J1.std.v02_48.G.200101070318_REBIN
        print(client.delete_single_granule())
        return

    def test_query_catalog(self):
        os.environ['TRUST_ENV'] = 'TRUE'
        os.environ['TOKEN_FACTORY'] = 'COGNITO'
        token_retriever: TokenAbstract = TokenFactory().get_instance(os.getenv('TOKEN_FACTORY'))
        client = DsClientUser(token_retriever, 'https://d3vc8w9zcq658.cloudfront.net', 'am-uds-dapa')  # am-uds-dapa'
        print(json.dumps(client.query_catalog(), indent=4))
        return

    def test_query_granules01(self):
        os.environ['TRUST_ENV'] = 'TRUE'
        # https://api.test.mdps.mcp.nasa.gov/am-uds-dapa/collections/URN:NASA:UNITY:unity:test:TRPSDL2ALLCRS1MGLOS___2/items
        os.environ['TOKEN_FACTORY'] = 'COGNITO'
        token_retriever: TokenAbstract = TokenFactory().get_instance(os.getenv('TOKEN_FACTORY'))
        client = DsClientUser(token_retriever, 'https://api.test.mdps.mcp.nasa.gov', 'am-uds-dapa')
        # client = DsClientAdmin(token_retriever, 'http://localhost:8005', 'data')

        client.urn = 'URN'
        client.org = 'NASA'
        client.project = 'UNITY'
        client.tenant = 'unity'
        client.tenant_venue = 'test'
        client.collection = 'TRPSDL2ALLCRS1MGLOS'
        client.collection_venue = '2'
        result = client.query_granules(sort_keys='+properties.datetime,-id')  # bbox='-114,32.5,-113,33.5'
        print(json.dumps(result, indent=4))
        return

    def test_query_single_granule01(self):
        os.environ['TRUST_ENV'] = 'TRUE'
        os.environ['TOKEN_FACTORY'] = 'COGNITO'
        token_retriever: TokenAbstract = TokenFactory().get_instance(os.getenv('TOKEN_FACTORY'))
        client = DsClientUser(token_retriever, 'https://api.test.mdps.mcp.nasa.gov', 'am-uds-dapa')
        client.urn = 'URN'
        client.org = 'NASA'
        client.project = 'UNITY'
        client.tenant = 'unity'
        client.tenant_venue = 'test'
        client.collection = 'TRPSDL2ALLCRS1MGLOS'
        client.collection_venue = '2'
        client.granule = 'TROPESS_CrIS-JPSS1_L2_Standard_TATM_20250108_MUSES_R1p23_megacity_los_angeles_MGLOS_F2p5_J0'
        # urn:nasa:unity:uds_local_test:DEV1:CHRP_16_DAY_REBIN___10:SNDR.SS1330.CHIRP.20230101T0000.m06.g001.L1_J1.std.v02_48.G.200101070318_REBIN
        print(client.query_single_granule())
        return

    def test_archive_one(self):
        os.environ['TRUST_ENV'] = 'TRUE'
        # https://api.test.mdps.mcp.nasa.gov/am-uds-dapa/collections/URN:NASA:UNITY:unity:test:TRPSDL2ALLCRS1MGLOS___2/items
        os.environ['TOKEN_FACTORY'] = 'COGNITO'
        token_retriever: TokenAbstract = TokenFactory().get_instance(os.getenv('TOKEN_FACTORY'))
        client = DsClientUser(token_retriever, 'https://api.test.mdps.mcp.nasa.gov', 'am-uds-dapa')
        # client = DsClientAdmin(token_retriever, 'http://localhost:8005', 'data')

        client.urn = 'URN'
        client.org = 'NASA'
        client.project = 'UNITY'
        client.tenant = 'unity'
        client.tenant_venue = 'test'
        client.collection = 'TRPSDL2ALLCRS1MGLOS'
        client.collection_venue = '2'
        client.granule = 'TROPESS_CrIS-JPSS1_L2_Standard_NH3_20250108_MUSES_R1p23_megacity_los_angeles_MGLOS_F2p5_J0'
        client.granule = 'TROPESS_CrIS-JPSS1_L2_Standard_TATM_20250108_MUSES_R1p23_megacity_los_angeles_MGLOS_F2p5_J0'
        print(client.archive_granule())
        return

    def test_add_archive_config(self):
        os.environ['TRUST_ENV'] = 'TRUE'
        # https://api.test.mdps.mcp.nasa.gov/am-uds-dapa/collections/URN:NASA:UNITY:unity:test:TRPSDL2ALLCRS1MGLOS___2/items
        os.environ['TOKEN_FACTORY'] = 'COGNITO'
        token_retriever: TokenAbstract = TokenFactory().get_instance(os.getenv('TOKEN_FACTORY'))
        client = DsClientUser(token_retriever, 'https://d3vc8w9zcq658.cloudfront.net', 'am-uds-dapa')
        # client = DsClientAdmin(token_retriever, 'http://localhost:8005', 'data')

        client.urn = 'URN'
        client.org = 'NASA'
        client.project = 'UNITY'
        client.tenant = 'UDS_LOCAL_TEST_3'  # 'uds_local_test'  # 'uds_sandbox'
        client.tenant_venue = 'DEV'
        client.collection = 'TRPSDL2ALLCRS1MGLOS'
        client.collection_venue = '2'
        daac_config = {
            "daac_collection_id": f"daac-mock-collection",
            "daac_provider": f"daac-provider--mock-collection",
            "daac_sns_topic_arn": "arn:aws:sns:us-west-2:561555463819:uds-test-cumulus-mock_daac_cnm_sns",
            "daac_role_arn": "mock",
            "daac_role_session_name": "mock",
            "daac_data_version": "123",
            "archiving_types": [
                {"data_type": "data", "file_extension": [".json", ".nc"]},
                {"data_type": "metadata", "file_extension": [".xml"]},
                {"data_type": "browse"}
            ]
        }
        print(client.add_archive_config(daac_config))
        return
