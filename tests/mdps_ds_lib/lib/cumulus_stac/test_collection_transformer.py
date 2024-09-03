import json
from unittest import TestCase


from mdps_ds_lib.lib.cumulus_stac.collection_transformer import STAC_COLLECTION_SCHEMA, CollectionTransformer
from mdps_ds_lib.lib.utils.json_validator import JsonValidator


class TestCollectionTransformer(TestCase):
    def test_01(self):
        stac_validator = JsonValidator(json.loads(STAC_COLLECTION_SCHEMA))
        source = {
            "createdAt": 1647992847582,
            "granuleId": "^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}0$",
            "process": "modis",
            "dateFrom": "1990-01-01T00:00:00Z",
            "dateTo": "1991-01-01T00:00:00Z",
            "sampleFileName": "P1570515ATMSSCIENCEAXT11344000000001.PDS",
            "name": "ATMS_SCIENCE_Group",
            "files": [
                {
                    "bucket": "internal",
                    "regex": "^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}00\\.PDS$",
                    "sampleFileName": "P1570515ATMSSCIENCEAXT11344000000000.PDS",
                    "type": "data",
                    "reportToEms": True
                },
                {
                    "bucket": "internal",
                    "regex": "^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}01\\.PDS$",
                    "sampleFileName": "P1570515ATMSSCIENCEAXT11344000000001.PDS",
                    "reportToEms": True,
                    "type": "metadata"
                },
                {
                    "bucket": "internal",
                    "regex": "^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}01\\.PDS\\.xml$",
                    "sampleFileName": "P1570515ATMSSCIENCEAXT11344000000001.PDS.xml",
                    "reportToEms": True,
                    "type": "metadata"
                },
                {
                    "bucket": "internal",
                    "regex": "^P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}00.PDS.cmr.xml$",
                    "sampleFileName": "P1570515ATMSSCIENCEAXT11344000000000.PDS.cmr.xml",
                    "reportToEms": True,
                    "type": "metadata"
                }
            ],
            "granuleIdExtraction": "(P[0-9]{3}[0-9]{4}[A-Z]{13}T[0-9]{12}0).+",
            "reportToEms": True,
            "version": "001",
            "duplicateHandling": "skip",
            "updatedAt": 1647992847582,
            "url_path": "{cmrMetadata.Granule.Collection.ShortName}___{cmrMetadata.Granule.Collection.VersionId}",
            "timestamp": 1647992849273
        }
        converted_stac = {
            "type": "Collection",
            "stac_version": "1.0.0",
            # "stac_extensions": [],
            "id": "sentinel-2",
            "description": "Sentinel-2 is a wide-swath, high-resolution, multi-spectral\nimaging mission supporting Copernicus Land Monitoring studies,\nincluding the monitoring of vegetation, soil and water cover,\nas well as observation of inland waterways and coastal areas.\n\nThe Sentinel-2 data contain 13 UINT16 spectral bands representing\nTOA reflectance scaled by 10000. See the [Sentinel-2 User Handbook](https://sentinel.esa.int/documents/247904/685211/Sentinel-2_User_Handbook)\nfor details. In addition, three QA bands are present where one\n(QA60) is a bitmask band with cloud mask information. For more\ndetails, [see the full explanation of how cloud masks are computed.](https://sentinel.esa.int/web/sentinel/technical-guides/sentinel-2-msi/level-1c/cloud-masks)\n\nEach Sentinel-2 product (zip archive) may contain multiple\ngranules. Each granule becomes a separate Earth Engine asset.\nEE asset ids for Sentinel-2 assets have the following format:\nCOPERNICUS/S2/20151128T002653_20151128T102149_T56MNN. Here the\nfirst numeric part represents the sensing date and time, the\nsecond numeric part represents the product generation date and\ntime, and the final 6-character string is a unique granule identifier\nindicating its UTM grid reference (see [MGRS](https://en.wikipedia.org/wiki/Military_Grid_Reference_System)).\n\nFor more details on Sentinel-2 radiometric resoltuon, [see this page](https://earth.esa.int/web/sentinel/user-guides/sentinel-2-msi/resolutions/radiometric).\n",
            "license": "proprietary",
            # "keywords": [],
            "providers": [{"name": "Test123"}],
            "extent": {
                "spatial": {
                    "bbox": [[0, 0, 0, 0]]
                },
                "temporal": {
                    "interval": [[None, None]]
                }
            },
            "assets": {},
            "summaries": {},
            "links": [
                {
                    "rel": "root",
                    "href": "./collection.json",
                },
            ]
        }
        converted_stac = CollectionTransformer(include_date_range=True).to_stac(source)
        self.assertEqual(None, stac_validator.validate(converted_stac), f'invalid stac format: {stac_validator}')
        if 'providers' not in converted_stac:
            converted_stac['providers'] = []
        converted_stac['providers'].append({'name': 'Test123'})
        print(converted_stac)
        transformer = CollectionTransformer(include_date_range=True)
        converted_cumulus = transformer.from_stac(converted_stac)
        for k, v in source.items():
            if k in ['updatedAt', 'timestamp', 'createdAt']:
                continue
            self.assertTrue(k in converted_cumulus, f'missing {k}')
            if k not in ['files', 'dateFrom', 'dateTo']:
                self.assertEqual(v, converted_cumulus[k], f'wrong value for {k}')
        self.assertEqual(sorted(json.dumps(source['files'])), sorted(json.dumps(converted_cumulus['files'])), f"wrong files content: {source['files']} vs. {converted_cumulus['files']}")
        self.assertEqual(transformer.output_provider, 'Test123', f'wrong provider')
        return
