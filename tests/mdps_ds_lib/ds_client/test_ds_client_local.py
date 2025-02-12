import base64
import json
import os
from unittest import TestCase

from pystac import Item

from mdps_ds_lib.ds_client.auth_token.token_abstract import TokenAbstract
from mdps_ds_lib.ds_client.auth_token.token_factory import TokenFactory
from mdps_ds_lib.ds_client.ds_client_admin import DsClientAdmin
from mdps_ds_lib.ds_client.ds_client_user import DsClientUser


class TestDsClientAdmin(TestCase):
    def __get_dummy_token(self):
        dummy_token = {
            "username": "dummy_user",
            "cognito:groups": ["Test_Admin", "Test_User"],
            # "sub": "822b6d0c-9054-4c43-a90e-8b59b2616b35",
            # "iss": "https:\\/\\/cognito-idp.us-west-2.amazonaws.com\\/us-west-2_yaOw3yj0z",
            # "client_id": "71g0c73jl77gsqhtlfg2ht388c", "origin_jti": "c082d9e0-1d1a-48a5-b1a9-84834ba70636",
            # "event_id": "b5e75d2d-c6be-48db-a9b8-780277e82b4d", "token_use": "access",
            # "scope": "aws.cognito.signin.user.admin", "auth_time": 1737146950, "exp": 1737150550,
            # "iat": 1737146950, "jti": "e7665b81-c4d6-4d35-93bd-36f92010555b",
        }
        encoded_token = base64.standard_b64encode(json.dumps(dummy_token).encode()).decode()
        encoded_token = [encoded_token, encoded_token, encoded_token]
        encoded_token = '.'.join(encoded_token)
        return encoded_token

    def test_01_admin(self):
        encoded_token = self.__get_dummy_token()
        os.environ['TOKEN_FACTORY'] = 'DUMMY'
        os.environ['DS_TOKEN'] = encoded_token
        os.environ['DS_URL'] = encoded_token
        os.environ['DS_STAGE'] = encoded_token
        token_retriever: TokenAbstract = TokenFactory().get_instance(os.getenv('TOKEN_FACTORY'))
        client = DsClientAdmin(token_retriever, 'http://localhost:8005', 'data')
        # client = DsClientAdmin(token_retriever, 'http://localhost:8005', 'data')
        client.setup_database()

        client.urn = 'URN'
        client.org = 'NASA'
        client.project = 'GEMX'
        client.tenant = 'DEMO'
        client.tenant_venue = 'DEV'

        client.add_admin_group(['CREATE', 'READ', 'DELETE'], 'Test_User')
        gemx_custom_properties = {
            "acquisition_date_l1b": {"type": "date",
                                     "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd'T'HH:mm:ssZ||yyyy-MM-dd'T'HH:mm:ss'Z'||yyyy-MM-dd'T'HH:mm:ss.SSSSSSZ||yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'||yyyy-MM-dd||epoch_millis"},
            "acquisition_date_2": {"type": "date",
                                   "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd'T'HH:mm:ssZ||yyyy-MM-dd'T'HH:mm:ss'Z'||yyyy-MM-dd'T'HH:mm:ss.SSSSSSZ||yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'||yyyy-MM-dd||epoch_millis"},

            "site_name": {"type": "keyword"},
            "investigator": {"type": "keyword"},
            "site_info": {"type": "keyword"},
            "additional_sensors": {"type": "keyword"},
            "nasa_log": {"type": "keyword"},
            "comments": {"type": "keyword"},

            "heading_deg": {"type": "double"},
            "length_scanlines": {"type": "long"},
            "altitude_meters": {"type": "long"},
            "total_tracks": {"type": "long"},
            "scan_lines": {"type": "long"},
        }
        client.add_tenant_database_index(gemx_custom_properties)
        return

    def test_02_user_add_collection(self):
        encoded_token = self.__get_dummy_token()
        os.environ['TOKEN_FACTORY'] = 'DUMMY'
        os.environ['DS_TOKEN'] = encoded_token
        # os.environ['DS_URL'] = encoded_token
        # os.environ['DS_STAGE'] = encoded_token
        token_retriever: TokenAbstract = TokenFactory().get_instance(os.getenv('TOKEN_FACTORY'))
        client = DsClientUser(token_retriever, 'http://localhost:8005', 'data')
        # client = DsClientAdmin(token_retriever, 'http://localhost:8005', 'data')

        client.urn = 'URN'
        client.org = 'NASA'
        client.project = 'GEMX'
        client.tenant = 'DEMO'
        client.tenant_venue = 'DEV'
        client.collection = 'AVIRIS'
        result = client.create_new_collection(True)
        print(result)
        client.collection = 'MASTER'

        result = client.create_new_collection(True)
        print(result)
        return

    def test_03_user_add_granule(self):
        encoded_token = self.__get_dummy_token()
        os.environ['TOKEN_FACTORY'] = 'DUMMY'
        os.environ['DS_TOKEN'] = encoded_token
        # os.environ['DS_URL'] = encoded_token
        # os.environ['DS_STAGE'] = encoded_token
        token_retriever: TokenAbstract = TokenFactory().get_instance(os.getenv('TOKEN_FACTORY'))
        client = DsClientUser(token_retriever, 'http://localhost:8005', 'data')
        # client = DsClientAdmin(token_retriever, 'http://localhost:8005', 'data')

        client.urn = 'URN'
        client.org = 'NASA'
        client.project = 'GEMX'
        client.tenant = 'DEMO'
        client.tenant_venue = 'DEV'
        client.collection = 'GEMX'
        client.collection_venue = '001'
        # client.granule = 'f240424t01_p00_r10'
        temp_granule =    {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "f230331t01-p00_r09",
        "properties": {
            "start_datetime": "2023-03-31T18:52:00.000000Z",
            "end_datetime": "2023-03-31T19:07:00.000000Z",
            "site_name": "Yosemite-NEON Box 4 (YN38)(orthocorrected)",
            "nasa_log": "232017",
            "investigator": "Robert Green",
            "comments": "s-)nclr",
            "site_info": "WDTS - Yosemite",
            "datetime": "1970-01-01T00:00:00Z"
        },
        "geometry": {
            "type": "Point",
            "coordinates": [
                0.0,
                0.0
            ]
        },
        "links": [],
        "assets": {
            "l1b": {
                "href": "https://popo.jpl.nasa.gov/gemx/data_products/l1b/f230331t01p00r09rdn_g.tar.gz",
                "title": "f230331t01p00r09rdn_g.tar.gz",
                "description": "2024-11-05 09:22",
                "file:size": 3435973836.8
            },
            "l2": {
                "href": "https://popo.jpl.nasa.gov/gemx/data_products/l2/f230331t01p00r09rfl.tar.gz",
                "title": "f230331t01p00r09rfl.tar.gz",
                "description": "2024-11-05 09:09",
                "file:size": 6442450944.0
            },
            "quicklook": {
                "href": "http://aviris.jpl.nasa.gov/ql/23qlook/f230331t01p00r09_geo.jpeg",
                "title": "f230331t01p00r09_geo.jpeg",
                "description": "Quicklook Link"
            }
        },
        "bbox": [
            -119.2020323,
            36.6539295,
            -119.1875901,
            38.1015035
        ],
        "stac_extensions": [
            "https://stac-extensions.github.io/file/v2.1.0/schema.json"
        ],
        "collection": "MASTER"
    }


        temp_granule = Item.from_dict(temp_granule)
        client.granule = temp_granule.id
        result = client.create_new_granule(temp_granule)
        print(result)
        client.tenant = 'MASTERS'
        client.tenant_venue = 'OPS'
        client.collection = '2024'
        client.collection_venue = '001'
        # result = client.create_new_granule()
        print(result)
        return

    def test_query_granules(self):
        encoded_token = self.__get_dummy_token()
        os.environ['TOKEN_FACTORY'] = 'DUMMY'
        os.environ['DS_TOKEN'] = encoded_token
        # os.environ['DS_URL'] = encoded_token
        # os.environ['DS_STAGE'] = encoded_token
        token_retriever: TokenAbstract = TokenFactory().get_instance(os.getenv('TOKEN_FACTORY'))
        client = DsClientUser(token_retriever, 'http://localhost:8005', 'data')
        # client = DsClientAdmin(token_retriever, 'http://localhost:8005', 'data')

        client.urn = 'URN'
        client.org = 'NASA'
        client.project = 'GEMX'
        client.tenant = 'AVIRIS'
        client.tenant_venue = 'OPS'
        client.collection = 'GEMX_2024'
        client.collection_venue = '001'
        result = client.query_granules(bbox='-114,32.5,-113,33.5')
        print(result)
        print(client.query_granules_next())
        return

    def test_query_granules_across_collections(self):
        encoded_token = self.__get_dummy_token()
        os.environ['TOKEN_FACTORY'] = 'DUMMY'
        os.environ['DS_TOKEN'] = encoded_token
        # os.environ['DS_URL'] = encoded_token
        # os.environ['DS_STAGE'] = encoded_token
        token_retriever: TokenAbstract = TokenFactory().get_instance(os.getenv('TOKEN_FACTORY'))
        client = DsClientUser(token_retriever, 'http://localhost:8005', 'data')
        # client = DsClientAdmin(token_retriever, 'http://localhost:8005', 'data')

        client.urn = 'URN'
        client.org = 'NASA'
        client.project = 'GEMX'
        client.tenant = 'AVIRIS'
        client.tenant_venue = 'OPS'
        result = client.query_granules_across_collections(bbox='-114.314407,32.5,-114.3144078,33.5')
        print(result)
        print(client.query_granules_next())
        return
