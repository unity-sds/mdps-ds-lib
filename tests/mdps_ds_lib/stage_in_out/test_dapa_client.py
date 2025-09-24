from unittest import TestCase

from mdps_ds_lib.stage_in_out.dapa_client import DapaClient
from dotenv import load_dotenv


class TestDapaClient(TestCase):
    def setUp(self) -> None:
        super().setUp()
        load_dotenv()

    def test_get_collection_01(self):
        dapa_client = DapaClient().with_verify_ssl(False)
        my_collection = 'urn:nasa:unity:unity:dev:SBG-L1B_PRE___1'
        collection = dapa_client.get_collection(my_collection)
        print(collection)
        self.assertEqual(collection['id'], my_collection)
        return
