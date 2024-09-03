import json
from datetime import datetime
from unittest import TestCase

import pystac

from uds_lib.lib.cumulus_stac.unity_collection_stac import \
    UnityCollectionStac


class TestUnityCollectionStac(TestCase):
    def test_01(self):
        dapa_collection = UnityCollectionStac()\
            .with_id(f'CUMULUS_DAPA_UNIT_TEST___{int(datetime.utcnow().timestamp())}')\
            .with_graule_id_regex("^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}0$")\
            .with_granule_id_extraction_regex("(P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}0).+")\
            .with_title("P1570515ATMSSCIENCEAXT11344000000001.PDS")\
            .with_provider('test123')\
            .with_process('modis')\
            .add_file_type("P1570515ATMSSCIENCEAXT11344000000001.PDS", "^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}01.PDS$", 'internal', 'metadata', 'root') \
            .add_file_type("P1570515ATMSSCIENCEAXT11344000000000.PDS.cmr.xml", "^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}00.PDS.cmr.xml$", 'internal', 'metadata', 'item') \
            .add_file_type("P1570515ATMSSCIENCEAXT11344000000001.PDS.xml", "^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}01\\.PDS\\.xml$", 'internal', 'metadata', 'item') \
            .add_file_type("P1570515ATMSSCIENCEAXT11344000000000.PDS", "^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}00\\.PDS$", 'internal', 'data', 'item')

        stac_collection = dapa_collection.start()
        validation_result = pystac.Collection.from_dict(stac_collection).validate()
        self.assertTrue(isinstance(validation_result, list), f'wrong validation for : {json.dumps(stac_collection, indent=4)}. details: {validation_result}')
        return
