from datetime import datetime
from unittest import TestCase

from mdps_ds_lib.lib.cumulus_stac.unity_collection_stac import UnityCollectionStac
from mdps_ds_lib.lib.aws.aws_lambda import AwsLambda


class TestAwsLambda(TestCase):
    def test_01(self):
        dapa_collection = UnityCollectionStac() \
            .with_id(f'CUMULUS_DAPA_UNIT_TEST___{int(datetime.utcnow().timestamp())}') \
            .with_graule_id_regex("^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}0$") \
            .with_granule_id_extraction_regex("(P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}0).+") \
            .with_title("P1570515ATMSSCIENCEAXT11344000000001.PDS") \
            .with_process('modis') \
            .add_file_type("P1570515ATMSSCIENCEAXT11344000000000.PDS.cmr.xml",
                           "^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}00.PDS.cmr.xml$", 'internal', 'metadata', 'item') \
            .add_file_type("P1570515ATMSSCIENCEAXT11344000000001.PDS.xml",
                           "^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}01\\.PDS\\.xml$", 'internal', 'metadata', 'item') \
            .add_file_type("P1570515ATMSSCIENCEAXT11344000000000.PDS", "^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}00\\.PDS$",
                           'internal', 'data', 'item')
        stac_collection = dapa_collection.start()

        response = AwsLambda().invoke_function(
            function_name='am-uds-dev-cumulus-cumulus_collections_creation_dapa',
            payload=stac_collection,
        )
        self.assertTrue('ResponseMetadata' in response, f'missing ResponseMetadata in response {response}')
        self.assertTrue('HTTPStatusCode' in response['ResponseMetadata'], f'missing HTTPStatusCode in response#ResponseMetadata {response}')
        print(response)
        return
