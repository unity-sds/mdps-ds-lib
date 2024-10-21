import json
import os
import tempfile
from unittest import TestCase

import pystac

from mdps_ds_lib.lib.cumulus_stac.granules_catalog import GranulesCatalog
from mdps_ds_lib.lib.utils.file_utils import FileUtils
from mdps_ds_lib.lib.utils.json_validator import JsonValidator


class TestGranulesCatalog(TestCase):
    def test_get_child_link_hrefs(self):
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            granules_catalog = {
                "type": "Catalog",
                "stac_version": "1.0.0",
                "id": "sample-id",
                "description": "Reference: https://github.com/radiantearth/stac-spec/blob/master/examples/catalog.json",
                "links": [
                    {
                        "href": "/absolute/path/to/stac/granules/json/file",
                        "rel": "child",
                        "type": "application/json",
                        "title": "<granules-id>"
                    },
                    {
                        "href": "/absolute/path/to/stac/granules/json/file2",
                        "rel": "child",
                        "type": "application/json",
                        "title": "<granules-id>"
                    }
                ]
            }
            granules_catalog_path = os.path.join(tmp_dir_name, 'sample_granules.json')
            FileUtils.write_json(granules_catalog_path, granules_catalog)
            pystac_catalog = GranulesCatalog()
            hrefs = pystac_catalog.get_child_link_hrefs(granules_catalog_path, 'child')
            self.assertEqual(hrefs, ['/absolute/path/to/stac/granules/json/file', '/absolute/path/to/stac/granules/json/file2'])
        return

    def test_get_child_link_relative_hrefs(self):
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            granules_catalog = {
                "type": "Catalog",
                "stac_version": "1.0.0",
                "id": "sample-id",
                "description": "Reference: https://github.com/radiantearth/stac-spec/blob/master/examples/catalog.json",
                "links": [
                    {
                        "href": "./file",
                        "rel": "child",
                        "type": "application/json",
                        "title": "<granules-id>"
                    },
                    {
                        "href": "file2",
                        "rel": "child",
                        "type": "application/json",
                        "title": "<granules-id>"
                    },
                    {
                        "href": "/absolute/path/to/stac/granules/json/file3",
                        "rel": "child",
                        "type": "application/json",
                        "title": "<granules-id>"
                    }
                ]
            }
            granules_catalog_path = os.path.join(tmp_dir_name, 'sample_granules.json')
            FileUtils.write_json(granules_catalog_path, granules_catalog)
            pystac_catalog = GranulesCatalog()
            hrefs = pystac_catalog.get_child_link_hrefs(granules_catalog_path, 'child')
            expecting_links = [
                os.path.join(tmp_dir_name, './file'),  # TODO "./" is ok? should be fine. but just in case
                os.path.join(tmp_dir_name, 'file2'),
                '/absolute/path/to/stac/granules/json/file3',
            ]
            self.assertEqual(hrefs, expecting_links)
        return

    def test_get_granules_item(self):
        sample_granules = {
          "type": "Feature",
          "stac_version": "1.0.0",
          "id": "SNDR.SNPP.ATMS.L1A.nominal2.12",
          "properties": {
            "start_datetime": "2016-01-14T11:00:00Z",
            "end_datetime": "2016-01-14T11:06:00Z",
            "created": "2020-12-14T13:50:00Z",
            "updated": "2022-08-15T06:26:25.344000Z",
            "datetime": "2022-08-15T06:26:17.938000Z"
          },
          "geometry": {
            "type": "Point",
            "coordinates": [
              0.0,
              0.0
            ]
          },
          "links": [
            {
              "rel": "collection",
              "href": "."
            }
          ],
          "assets": {
              "SNDR.SNPP.ATMS.L1A.nominal2.12.nc": {
                  "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc",
                  "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc",
                  "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc",
                  "roles": ["data"],
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.12.1.nc": {
                  "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.1.nc",
                  "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.1.nc",
                  "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.1.nc",
                  "roles": ["data"],
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas": {
                  "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas",
                  "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas",
                  "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas",
                  "roles": ["metadata__data"],
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml": {
                  "href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml",
                  "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml",
                  "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml",
                  "roles": ["metadata__cmr"],
              }
          },
          "bbox": [
            0.0,
            0.0,
            0.0,
            0.0
          ],
          "stac_extensions": [],
          "collection": "SNDR_SNPP_ATMS_L1A___1"
        }
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            granules_catalog_path = os.path.join(tmp_dir_name, 'sample_granules.json')
            FileUtils.write_json(granules_catalog_path, sample_granules)
            pystac_catalog = GranulesCatalog().get_granules_item(granules_catalog_path)
            self.assertEqual(pystac_catalog.id, 'SNDR.SNPP.ATMS.L1A.nominal2.12')
        return

    def test_extract_assets_href(self):
        sample_granules = {
          "type": "Feature",
          "stac_version": "1.0.0",
          "id": "SNDR.SNPP.ATMS.L1A.nominal2.12",
          "properties": {
            "start_datetime": "2016-01-14T11:00:00Z",
            "end_datetime": "2016-01-14T11:06:00Z",
            "created": "2020-12-14T13:50:00Z",
            "updated": "2022-08-15T06:26:25.344000Z",
            "datetime": "2022-08-15T06:26:17.938000Z"
          },
          "geometry": {
            "type": "Point",
            "coordinates": [
              0.0,
              0.0
            ]
          },
          "links": [
            {
              "rel": "collection",
              "href": "."
            }
          ],
          "assets": {
              "SNDR.SNPP.ATMS.L1A.nominal2.12.nc": {
                  "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc",
                  "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc",
                  "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc",
                  "roles": ["data"],
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas": {
                  "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas",
                  "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas",
                  "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas",
                  "roles": ["metadata__data"],
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml": {
                  "href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml",
                  "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml",
                  "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml",
                  "roles": ["metadata__cmr"],
              }
          },
          "bbox": [
            0.0,
            0.0,
            0.0,
            0.0
          ],
          "stac_extensions": [],
          "collection": "SNDR_SNPP_ATMS_L1A___1"
        }
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            granules_catalog_path = os.path.join(tmp_dir_name, 'sample_granules.json')
            FileUtils.write_json(granules_catalog_path, sample_granules)
            gc = GranulesCatalog()
            pystac_catalog = gc.get_granules_item(granules_catalog_path)
            self.assertEqual(pystac_catalog.id, 'SNDR.SNPP.ATMS.L1A.nominal2.12')
            assets = gc.extract_assets_href(pystac_catalog)
            expected_assets = {'data': {'SNDR.SNPP.ATMS.L1A.nominal2.12.nc': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc'},
                               'metadata__data': {'SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas'},
                               'metadata__cmr': {'SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml'}
                               }
            self.assertEqual(assets, expected_assets, 'wrong assets')
        return

    def test_extract_assets_relative_href_01(self):
        sample_granules = {
          "type": "Feature",
          "stac_version": "1.0.0",
          "id": "SNDR.SNPP.ATMS.L1A.nominal2.12",
          "properties": {
            "start_datetime": "2016-01-14T11:00:00Z",
            "end_datetime": "2016-01-14T11:06:00Z",
            "created": "2020-12-14T13:50:00Z",
            "updated": "2022-08-15T06:26:25.344000Z",
            "datetime": "2022-08-15T06:26:17.938000Z"
          },
          "geometry": {
            "type": "Point",
            "coordinates": [
              0.0,
              0.0
            ]
          },
          "links": [
            {
              "rel": "collection",
              "href": "."
            }
          ],
          "assets": {
            "SNDR.SNPP.ATMS.L1A.nominal2.12.nc": {
                  "href": "./SNDR.SNPP.ATMS.L1A.nominal2.12.nc",
                  "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc",
                  "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc",
                  "roles": ["data"],
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas": {
                  "href": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas",
                  "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas",
                  "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas",
                  "roles": ["metadata__data"],
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml": {
                  "href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml",
                  "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml",
                  "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml",
                  "roles": ["metadata__cmr"],
              }
          },
          "bbox": [
            0.0,
            0.0,
            0.0,
            0.0
          ],
          "stac_extensions": [],
          "collection": "SNDR_SNPP_ATMS_L1A___1"
        }
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            granules_catalog_path = os.path.join(tmp_dir_name, 'sample_granules.json')
            FileUtils.write_json(granules_catalog_path, sample_granules)
            gc = GranulesCatalog()
            pystac_catalog = gc.get_granules_item(granules_catalog_path)
            self.assertEqual(pystac_catalog.id, 'SNDR.SNPP.ATMS.L1A.nominal2.12')
            assets = gc.extract_assets_href(pystac_catalog)
            expected_assets = {
                'data': {'SNDR.SNPP.ATMS.L1A.nominal2.12.nc': './SNDR.SNPP.ATMS.L1A.nominal2.12.nc'},
                'metadata__data': {'SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas': 'SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas'},
                'metadata__cmr': {'SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml'}}
            self.assertEqual(assets, expected_assets, 'wrong assets')
        return

    def test_extract_assets_relative_href_02(self):
        sample_granules = {
          "type": "Feature",
          "stac_version": "1.0.0",
          "id": "SNDR.SNPP.ATMS.L1A.nominal2.12",
          "properties": {
            "start_datetime": "2016-01-14T11:00:00Z",
            "end_datetime": "2016-01-14T11:06:00Z",
            "created": "2020-12-14T13:50:00Z",
            "updated": "2022-08-15T06:26:25.344000Z",
            "datetime": "2022-08-15T06:26:17.938000Z"
          },
          "geometry": {
            "type": "Point",
            "coordinates": [
              0.0,
              0.0
            ]
          },
          "links": [
            {
              "rel": "collection",
              "href": "."
            }
          ],
          "assets": {
            "SNDR.SNPP.ATMS.L1A.nominal2.12.nc": {
                  "href": "./SNDR.SNPP.ATMS.L1A.nominal2.12.nc",
                  "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc",
                  "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc",
                  "roles": ["data"],
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.12.1.nc": {
                  "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.1.nc",
                  "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.1.nc",
                  "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.1.nc",
                  "roles": ["data"],
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas": {
                  "href": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas",
                  "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas",
                  "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas",
                  "roles": ["metadata__data"],
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml": {
                  "href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml",
                  "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml",
                  "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml",
                  "roles": ["metadata__cmr"],
              }
          },
          "bbox": [
            0.0,
            0.0,
            0.0,
            0.0
          ],
          "stac_extensions": [],
          "collection": "SNDR_SNPP_ATMS_L1A___1"
        }
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            granules_catalog_path = os.path.join(tmp_dir_name, 'sample_granules.json')
            FileUtils.write_json(granules_catalog_path, sample_granules)
            gc = GranulesCatalog()
            pystac_catalog = gc.get_granules_item(granules_catalog_path)
            self.assertEqual(pystac_catalog.id, 'SNDR.SNPP.ATMS.L1A.nominal2.12')
            assets = gc.extract_assets_href(pystac_catalog, '/some/temp/directory/../hehe')
            expected_assets = {
                'data': {'SNDR.SNPP.ATMS.L1A.nominal2.12.nc': '/some/temp/directory/../hehe/./SNDR.SNPP.ATMS.L1A.nominal2.12.nc',
                         'SNDR.SNPP.ATMS.L1A.nominal2.12.1.nc': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.1.nc'},
                'metadata__data': {'SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas': '/some/temp/directory/../hehe/SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas'},
                'metadata__cmr': {'SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml'}}
            self.assertEqual(assets, expected_assets, 'wrong assets')
        return

    def test_extract_assets_relative_href_03(self):
        sample_granules = {
          "type": "Feature",
          "stac_version": "1.0.0",
          "id": "SNDR.SNPP.ATMS.L1A.nominal2.12",
          "properties": {
            "start_datetime": "2016-01-14T11:00:00Z",
            "end_datetime": "2016-01-14T11:06:00Z",
            "created": "2020-12-14T13:50:00Z",
            "updated": "2022-08-15T06:26:25.344000Z",
            "datetime": "2022-08-15T06:26:17.938000Z"
          },
          "geometry": {
            "type": "Point",
            "coordinates": [
              0.0,
              0.0
            ]
          },
          "links": [
            {
              "rel": "collection",
              "href": "."
            },
              {
                  "rel": "self",
                  "href": "/some/temp/directory/../hehe/item.json"
              }
          ],
          "assets": {
              "SNDR.SNPP.ATMS.L1A.nominal2.12.nc": {
                  "href": "./SNDR.SNPP.ATMS.L1A.nominal2.12.nc",
                  "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc",
                  "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc",
                  "roles": ["data"],
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas": {
                  "href": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas",
                  "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas",
                  "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas",
                  "roles": ["metadata__data"],
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.2.cas": {
                  "href": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.2.cas",
                  "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.2.cas",
                  "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.2.cas",
                  "roles": ["metadata__data"],
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml": {
                  "href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml",
                  "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml",
                  "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml",
                  "roles": ["metadata__cmr"],
              }
          },
          "bbox": [
            0.0,
            0.0,
            0.0,
            0.0
          ],
          "stac_extensions": [],
          "collection": "SNDR_SNPP_ATMS_L1A___1"
        }
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            granules_catalog_path = os.path.join(tmp_dir_name, 'sample_granules.json')
            FileUtils.write_json(granules_catalog_path, sample_granules)
            gc = GranulesCatalog()
            pystac_catalog = gc.get_granules_item(granules_catalog_path)
            self.assertEqual(pystac_catalog.id, 'SNDR.SNPP.ATMS.L1A.nominal2.12')
            assets = gc.extract_assets_href(pystac_catalog)
            expected_assets = {
                'data': {'SNDR.SNPP.ATMS.L1A.nominal2.12.nc': '/some/temp/directory/../hehe/./SNDR.SNPP.ATMS.L1A.nominal2.12.nc'},
                'metadata__data': {'SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas': '/some/temp/directory/../hehe/SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas',
                                   'SNDR.SNPP.ATMS.L1A.nominal2.12.nc.2.cas': '/some/temp/directory/../hehe/SNDR.SNPP.ATMS.L1A.nominal2.12.nc.2.cas'},
                'metadata__cmr': {'SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml'}}
            self.assertEqual(assets, expected_assets, 'wrong assets')
        return

    def test_update_assets_href(self):
        sample_granules = {
          "type": "Feature",
          "stac_version": "1.0.0",
          "id": "SNDR.SNPP.ATMS.L1A.nominal2.12",
          "properties": {
            "start_datetime": "2016-01-14T11:00:00Z",
            "end_datetime": "2016-01-14T11:06:00Z",
            "created": "2020-12-14T13:50:00Z",
            "updated": "2022-08-15T06:26:25.344000Z",
            "datetime": "2022-08-15T06:26:17.938000Z"
          },
          "geometry": {
            "type": "Point",
            "coordinates": [
              0.0,
              0.0
            ]
          },
          "links": [
            {
              "rel": "collection",
              "href": "."
            }
          ],
          "assets": {
              "SNDR.SNPP.ATMS.L1A.nominal2.12.nc": {
                  "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc",
                  "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc",
                  "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc",
                  "roles": ["data"],
              },
            "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas": {
              "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas",
              "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas",
              "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas",
                             "roles": ["metadata__data"],
        },
            "SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml": {
              "href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml",
              "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml",
              "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml",
                             "roles": ["metadata__cmr"],
        }
          },
          "bbox": [
            0.0,
            0.0,
            0.0,
            0.0
          ],
          "stac_extensions": [],
          "collection": "SNDR_SNPP_ATMS_L1A___1"
        }
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            granules_catalog_path = os.path.join(tmp_dir_name, 'sample_granules.json')
            FileUtils.write_json(granules_catalog_path, sample_granules)
            gc = GranulesCatalog()
            pystac_catalog = gc.get_granules_item(granules_catalog_path)
            self.assertEqual(pystac_catalog.id, 'SNDR.SNPP.ATMS.L1A.nominal2.12')
            assets = gc.extract_assets_href(pystac_catalog)
            expected_assets = {
                'data': {'SNDR.SNPP.ATMS.L1A.nominal2.12.nc': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc'},
                'metadata__data': {'SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas'},
                'metadata__cmr': {'SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml'}
            }
            self.assertEqual(assets, expected_assets, 'wrong assets')
            updating_assets = {
                'SNDR.SNPP.ATMS.L1A.nominal2.12.nc': 'file:///absolute/file/some/file/data',
                'SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas',
                'other.name': '/absolute/file/some/file/metadata__extra',
                'SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml'
            }

            updating_assets_result = {
                'data': {'SNDR.SNPP.ATMS.L1A.nominal2.12.nc': 'file:///absolute/file/some/file/data'},
                'metadata__data': {'SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas'},
                'metadata__cmr': {'SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml'}
            }
            gc.update_assets_href(pystac_catalog, updating_assets)
            updated_assets = gc.extract_assets_href(pystac_catalog)
            self.assertEqual(updated_assets, updating_assets_result, 'wrong updated assets')

        return

    def test_update_assets_href_02(self):
        sample_granules = {
          "type": "Feature",
          "stac_version": "1.0.0",
          "id": "SNDR.SNPP.ATMS.L1A.nominal2.12",
          "properties": {
            "start_datetime": "2016-01-14T11:00:00Z",
            "end_datetime": "2016-01-14T11:06:00Z",
            "created": "2020-12-14T13:50:00Z",
            "updated": "2022-08-15T06:26:25.344000Z",
            "datetime": "2022-08-15T06:26:17.938000Z"
          },
          "geometry": {
            "type": "Point",
            "coordinates": [
              0.0,
              0.0
            ]
          },
          "links": [
            {
              "rel": "collection",
              "href": "."
            }
          ],
          "assets": {
              "data1": {
                  "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc",
                  "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc",
                  "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc",
                  "roles": ["data"],
              },
            "metadata1": {
              "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas",
              "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas",
              "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas",
                             "roles": ["metadata"],
        },
            "metadata2": {
              "href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml",
              "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml",
              "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml",
                             "roles": ["metadata"],
        }
          },
          "bbox": [
            0.0,
            0.0,
            0.0,
            0.0
          ],
          "stac_extensions": [],
          "collection": "SNDR_SNPP_ATMS_L1A___1"
        }
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            granules_catalog_path = os.path.join(tmp_dir_name, 'sample_granules.json')
            FileUtils.write_json(granules_catalog_path, sample_granules)
            gc = GranulesCatalog()
            pystac_catalog = gc.get_granules_item(granules_catalog_path)
            self.assertEqual(pystac_catalog.id, 'SNDR.SNPP.ATMS.L1A.nominal2.12')
            assets = gc.extract_assets_href(pystac_catalog)
            expected_assets = {
                'data': {'data1': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc'},
                'metadata': {'metadata1': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas',
                             'metadata2': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml'
                             },
            }
            self.assertEqual(assets, expected_assets, 'wrong assets')
            updating_assets = {
                'data1': 'file:///absolute/file/some/file/data',
                'metadata1': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas',
                'other.name': '/absolute/file/some/file/metadata__extra',
                'metadata2': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml'
            }

            updating_assets_result = {
                'data': {'data1': 'file:///absolute/file/some/file/data'},
                'metadata': {'metadata1': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas',
                           'metadata2': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml'
                                   },
            }
            gc.update_assets_href(pystac_catalog, updating_assets)
            updated_assets = gc.extract_assets_href(pystac_catalog)
            self.assertEqual(updated_assets, updating_assets_result, 'wrong updated assets')

        return

    def test_manual_validdate_stac(self):
        raw_json = '''{
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "URN:NASA:UNITY:UDS_BLACK:DEV:UDS_UNIT_COLLECTION___2410010608:abcd.1234.efgh.test_file-24.08.13.13.53",
            "properties": {
                "tag": "#sample",
                "c_data1": [
                    1,
                    10,
                    100,
                    1000
                ],
                "c_data2": [
                    false,
                    true,
                    true,
                    false,
                    true
                ],
                "c_data3": [
                    "Bellman Ford"
                ],
                "soil10": {
                    "0_0": 0,
                    "0_1": 1,
                    "0_2": 0
                },
                "datetime": "2024-10-02T21:12:11.995000Z",
                "start_datetime": "2016-01-31T18:00:00.009000Z",
                "end_datetime": "2016-01-31T19:59:59.991000Z",
                "created": "1970-01-01T00:00:00Z",
                "updated": "2024-10-02T21:12:54.300000Z",
                "status": "completed",
                "provider": "unity",
                "archive_status": "cnm_r_failed",
                "archive_error_message": "[{\\"uri\\": \\"https://uds-distribution-placeholder/uds-sbx-cumulus-staging/URN:NASA:UNITY:UDS_BLACK:DEV:UDS_UNIT_COLLECTION___2410010608/URN:NASA:UNITY:UDS_BLACK:DEV:UDS_UNIT_COLLECTION___2410010608:abcd.1234.efgh.test_file-24.08.13.13.53/abcd.1234.efgh.test_file-24.08.13.13.53.data.stac.json\\", \\"error\\": \\"mismatched size: 11 v. -1\\"}]",
                "archive_error_code": "VALIDATION_ERROR"
            },
            "geometry": {
                "type": "Point",
                "coordinates": [
                    0.0,
                    0.0
                ]
            },
            "links": [
                {
                    "rel": "collection",
                    "href": "."
                },
                {
                    "rel": "self",
                    "href": "https://d3vc8w9zcq658.cloudfront.net/data-sbx/collections/URN:NASA:UNITY:UDS_BLACK:DEV:UDS_UNIT_COLLECTION___2410010608/items/URN:NASA:UNITY:UDS_BLACK:DEV:UDS_UNIT_COLLECTION___2410010608:abcd.1234.efgh.test_file-24.08.13.13.53",
                    "type": "application/json",
                    "title": "URN:NASA:UNITY:UDS_BLACK:DEV:UDS_UNIT_COLLECTION___2410010608:abcd.1234.efgh.test_file-24.08.13.13.53"
                }
            ],
            "assets": {
                "abcd.1234.efgh.test_file-24.08.13.13.53.cmr.xml": {
                    "href": "s3://uds-sbx-cumulus-staging/URN:NASA:UNITY:UDS_BLACK:DEV:UDS_UNIT_COLLECTION___2410010608/URN:NASA:UNITY:UDS_BLACK:DEV:UDS_UNIT_COLLECTION___2410010608:abcd.1234.efgh.test_file-24.08.13.13.53/abcd.1234.efgh.test_file-24.08.13.13.53.cmr.xml",
                    "title": "abcd.1234.efgh.test_file-24.08.13.13.53.cmr.xml",
                    "description": "size=1796;checksumType=md5;checksum=788d1ca2dcef19744c2a1efde2f69693;",
                    "file:size": 1796,
                    "file:checksum": "788d1ca2dcef19744c2a1efde2f69693",
                    "roles": [
                        "metadata"
                    ]
                },
                "abcd.1234.efgh.test_file-24.08.13.13.53.nc.cas": {
                    "href": "s3://uds-sbx-cumulus-staging/URN:NASA:UNITY:UDS_BLACK:DEV:UDS_UNIT_COLLECTION___2410010608/URN:NASA:UNITY:UDS_BLACK:DEV:UDS_UNIT_COLLECTION___2410010608:abcd.1234.efgh.test_file-24.08.13.13.53/abcd.1234.efgh.test_file-24.08.13.13.53.nc.cas",
                    "title": "abcd.1234.efgh.test_file-24.08.13.13.53.nc.cas",
                    "description": "size=-1;checksumType=md5;checksum=unknown;",
                    "file:size": 0,
                    "file:checksum": "00000000000000000000000000000000",
                    "roles": [
                        "metadata"
                    ]
                },
                "abcd.1234.efgh.test_file-24.08.13.13.53.nc.stac.json": {
                    "href": "s3://uds-sbx-cumulus-staging/URN:NASA:UNITY:UDS_BLACK:DEV:UDS_UNIT_COLLECTION___2410010608/URN:NASA:UNITY:UDS_BLACK:DEV:UDS_UNIT_COLLECTION___2410010608:abcd.1234.efgh.test_file-24.08.13.13.53/abcd.1234.efgh.test_file-24.08.13.13.53.nc.stac.json",
                    "title": "abcd.1234.efgh.test_file-24.08.13.13.53.nc.stac.json",
                    "description": "size=-1;checksumType=md5;checksum=unknown;",
                    "file:size": 0,
                    "file:checksum": "00000000000000000000000000000000",
                    "roles": [
                        "metadata"
                    ]
                },
                "abcd.1234.efgh.test_file-24.08.13.13.53.data.stac.json": {
                    "href": "s3://uds-sbx-cumulus-staging/URN:NASA:UNITY:UDS_BLACK:DEV:UDS_UNIT_COLLECTION___2410010608/URN:NASA:UNITY:UDS_BLACK:DEV:UDS_UNIT_COLLECTION___2410010608:abcd.1234.efgh.test_file-24.08.13.13.53/abcd.1234.efgh.test_file-24.08.13.13.53.data.stac.json",
                    "title": "abcd.1234.efgh.test_file-24.08.13.13.53.data.stac.json",
                    "description": "size=-1;checksumType=md5;checksum=unknown;",
                    "file:size": 0,
                    "file:checksum": "00000000000000000000000000000000",
                    "roles": [
                        "data"
                    ]
                }
            },
            "bbox": [
                -180.0,
                -90.0,
                180.0,
                90.0
            ],
            "stac_extensions": [
                "https://stac-extensions.github.io/file/v2.1.0/schema.json"
            ],
            "collection": "URN:NASA:UNITY:UDS_BLACK:DEV:UDS_UNIT_COLLECTION___2410010608"
        }'''
        raw_json = json.loads(raw_json)
        schema1 = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "$id": "https://stac-extensions.github.io/file/v2.1.0/schema.json#",
            "title": "File Info Extension",
            "description": "STAC File Info Extension for STAC Items, STAC Catalogs, and STAC Collections.",
            "oneOf": [
                {
                    "$comment": "This is the schema for STAC Items.",
                    "allOf": [
                        {
                            "type": "object",
                            "required": [
                                "type",
                                "assets"
                            ],
                            "properties": {
                                "type": {
                                    "const": "Feature"
                                },
                                "assets": {
                                    "type": "object",
                                    "additionalProperties": {
                                        "$ref": "#/definitions/fields"
                                    }
                                },
                                "links": {
                                    "type": "array",
                                    "items": {
                                        "$ref": "#/definitions/fields"
                                    }
                                }
                            }
                        },
                        {
                            "$ref": "#/definitions/stac_extensions"
                        }
                    ]
                },
                {
                    "$comment": "This is the schema for STAC Catalogs.",
                    "allOf": [
                        {
                            "type": "object",
                            "required": [
                                "type"
                            ],
                            "properties": {
                                "type": {
                                    "const": "Catalog"
                                },
                                "links": {
                                    "type": "array",
                                    "items": {
                                        "$ref": "#/definitions/fields"
                                    }
                                }
                            }
                        },
                        {
                            "$ref": "#/definitions/stac_extensions"
                        }
                    ]
                },
                {
                    "$comment": "This is the schema for STAC Collections.",
                    "allOf": [
                        {
                            "type": "object",
                            "required": [
                                "type"
                            ],
                            "properties": {
                                "type": {
                                    "const": "Collection"
                                },
                                "assets": {
                                    "type": "object",
                                    "additionalProperties": {
                                        "$ref": "#/definitions/fields"
                                    }
                                },
                                "links": {
                                    "type": "array",
                                    "items": {
                                        "$ref": "#/definitions/fields"
                                    }
                                },
                                "item_assets": {
                                    "type": "object",
                                    "additionalProperties": {
                                        "$ref": "#/definitions/fields"
                                    }
                                }
                            }
                        },
                        {
                            "$ref": "#/definitions/stac_extensions"
                        }
                    ]
                }
            ],
            "definitions": {
                "stac_extensions": {
                    "type": "object",
                    "required": [
                        "stac_extensions"
                    ],
                    "properties": {
                        "stac_extensions": {
                            "type": "array",
                            "contains": {
                                "const": "https://stac-extensions.github.io/file/v2.1.0/schema.json"
                            }
                        }
                    }
                },
                "fields": {
                    "$comment": "Add your new fields here. Don't require them here, do that above in the item schema.",
                    "type": "object",
                    "properties": {
                        "file:byte_order": {
                            "type": "string",
                            "enum": [
                                "big-endian",
                                "little-endian"
                            ],
                            "title": "File Byte Order"
                        },
                        "file:checksum": {
                            "type": "string",
                            "pattern": "^[a-f0-9]+$",
                            "title": "File Checksum (Multihash)"
                        },
                        "file:header_size": {
                            "type": "integer",
                            "minimum": 0,
                            "title": "File Header Size"
                        },
                        "file:size": {
                            "type": "integer",
                            "minimum": 0,
                            "title": "File Size"
                        },
                        "file:values": {
                            "type": "array",
                            "minItems": 1,
                            "items": {
                                "type": "object",
                                "required": [
                                    "values",
                                    "summary"
                                ],
                                "properties": {
                                    "values": {
                                        "type": "array",
                                        "minItems": 1,
                                        "items": {
                                            "description": "Any data type is allowed"
                                        }
                                    },
                                    "summary": {
                                        "type": "string",
                                        "minLength": 1
                                    }
                                }
                            }
                        },
                        "file:local_path": {
                            "type": "string",
                            "pattern": "^[^\\r\\n\\t\\\\:'\"/]+(/[^\\r\\n\\t\\\\:'\"/]+)*/?$",
                            "title": "Relative File Path"
                        }
                    },
                    "patternProperties": {
                        "^(?!file:)": {
                            "$comment": "Above, change `template` to the prefix of this extension"
                        }
                    },
                    "additionalProperties": False
                }
            }
        }
        from jsonschema import validate
        try:
            result = validate(instance=raw_json, schema=schema1)
        except Exception as e:
            self.assertTrue(False, f'{e}')
        stac_item = pystac.Item.from_dict(raw_json)
        validation_result = stac_item.validate()
        return

