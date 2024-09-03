import os
import tempfile
from unittest import TestCase

from uds_lib.lib.cumulus_stac.granules_catalog import GranulesCatalog
from uds_lib.lib.utils.file_utils import FileUtils


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
            expected_assets = {'data': ['s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc'],
                               'metadata__data': ['s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas'],
                               'metadata__cmr': ['s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml']}
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
                'data': ['./SNDR.SNPP.ATMS.L1A.nominal2.12.nc'],
                'metadata__data': ['SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas'],
                'metadata__cmr': ['s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml']}
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
                'data': ['/some/temp/directory/../hehe/./SNDR.SNPP.ATMS.L1A.nominal2.12.nc', 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.1.nc'],
                'metadata__data': ['/some/temp/directory/../hehe/SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas'],
                'metadata__cmr': ['s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml']}
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
                'data': ['/some/temp/directory/../hehe/./SNDR.SNPP.ATMS.L1A.nominal2.12.nc'],
                'metadata__data': ['/some/temp/directory/../hehe/SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas', '/some/temp/directory/../hehe/SNDR.SNPP.ATMS.L1A.nominal2.12.nc.2.cas'],
                'metadata__cmr': ['s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml']}
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
                'data': ['s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc'],
                'metadata__data': ['s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas'],
                'metadata__cmr': ['s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml']
            }
            self.assertEqual(assets, expected_assets, 'wrong assets')
            updating_assets = {
                'SNDR.SNPP.ATMS.L1A.nominal2.12.nc': 'file:///absolute/file/some/file/data',
                'SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas': 's3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas',
                'other.name': '/absolute/file/some/file/metadata__extra',
                'SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml': 's3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml'
            }

            updating_assets_result = {
                'data': ['file:///absolute/file/some/file/data'],
                'metadata__data': ['s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas'],
                'metadata__cmr': ['s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml']
            }
            gc.update_assets_href(pystac_catalog, updating_assets)
            updated_assets = gc.extract_assets_href(pystac_catalog)
            self.assertEqual(updated_assets, updating_assets_result, 'wrong updated assets')

        return
