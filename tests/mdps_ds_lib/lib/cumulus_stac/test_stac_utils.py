import json
from unittest import TestCase

from mdps_ds_lib.lib.cumulus_stac.stac_utils import StacUtils


class TestStacUtils(TestCase):
    def test_01_reduce_stac_list_to_data_links(self):
        granule_json = [
          {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "SNDR.SNPP.ATMS.L1A.nominal2.01",
            "properties": {
              "start_datetime": "2016-01-14T09:54:00Z",
              "end_datetime": "2016-01-14T10:00:00Z",
              "created": "2020-12-14T13:50:00Z",
              "updated": "2022-08-15T06:26:39.830000Z",
              "datetime": "2022-08-15T06:26:37.029000Z"
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
              "SNDR.SNPP.ATMS.L1A.nominal2.01.nc": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.01.nc",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.01.nc",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.01.nc",
                "roles": [
                  "data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.01_2.nc": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.01_2.nc",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.01_2.nc",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.01_2.nc",
                "roles": [
                  "data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.01.nc.cas": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.01.nc.cas",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.01.nc.cas",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.01.nc.cas",
                "roles": [
                  "metadata__data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.01.cmr.xml": {
                "href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.01.cmr.xml",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.01.cmr.xml",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.01.cmr.xml",
                "roles": [
                  "metadata__cmr"
                ]
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
          },
          {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "SNDR.SNPP.ATMS.L1A.nominal2.08",
            "properties": {
              "start_datetime": "2016-01-14T10:36:00Z",
              "end_datetime": "2016-01-14T10:42:00Z",
              "created": "2020-12-14T13:50:00Z",
              "updated": "2022-08-15T06:26:26.078000Z",
              "datetime": "2022-08-15T06:26:19.333000Z"
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
              "SNDR.SNPP.ATMS.L1A.nominal2.08.nc": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.08.nc",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.08.nc",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.08.nc",
                "roles": [
                  "data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.08.nc.cas": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.08.nc.cas",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.08.nc.cas",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.08.nc.cas",
                "roles": [
                  "metadata__data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.08.cmr.xml": {
                "href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.08.cmr.xml",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.08.cmr.xml",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.08.cmr.xml",
                "roles": [
                  "metadata__cmr"
                ]
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
          },
          {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "SNDR.SNPP.ATMS.L1A.nominal2.06",
            "properties": {
              "start_datetime": "2016-01-14T10:24:00Z",
              "end_datetime": "2016-01-14T10:30:00Z",
              "created": "2020-12-14T13:50:00Z",
              "updated": "2022-08-15T06:26:26.068000Z",
              "datetime": "2022-08-15T06:26:18.641000Z"
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
              "SNDR.SNPP.ATMS.L1A.nominal2.06.nc": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.06.nc",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.06.nc",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.06.nc",
                "roles": [
                  "data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.06.nc.cas": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.06.nc.cas",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.06.nc.cas",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.06.nc.cas",
                "roles": [
                  "metadata__data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.06.cmr.xml": {
                "href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.06.cmr.xml",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.06.cmr.xml",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.06.cmr.xml",
                "roles": [
                  "metadata__cmr"
                ]
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
          },
          {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "SNDR.SNPP.ATMS.L1A.nominal2.18",
            "properties": {
              "start_datetime": "2016-01-14T11:36:00Z",
              "end_datetime": "2016-01-14T11:42:00Z",
              "created": "2020-12-14T13:50:00Z",
              "updated": "2022-08-15T06:26:26.060000Z",
              "datetime": "2022-08-15T06:26:19.698000Z"
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
              "SNDR.SNPP.ATMS.L1A.nominal2.18.nc": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.18.nc",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.18.nc",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.18.nc",
                "roles": [
                  "data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.18.nc.cas": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.18.nc.cas",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.18.nc.cas",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.18.nc.cas",
                "roles": [
                  "metadata__data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.18.cmr.xml": {
                "href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.18.cmr.xml",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.18.cmr.xml",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.18.cmr.xml",
                "roles": [
                  "metadata__cmr"
                ]
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
          },
          {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "SNDR.SNPP.ATMS.L1A.nominal2.04",
            "properties": {
              "start_datetime": "2016-01-14T10:12:00Z",
              "end_datetime": "2016-01-14T10:18:00Z",
              "created": "2020-12-14T13:50:00Z",
              "updated": "2022-08-15T06:26:26.050000Z",
              "datetime": "2022-08-15T06:26:19.491000Z"
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
              "SNDR.SNPP.ATMS.L1A.nominal2.04.nc": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.04.nc",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.04.nc",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.04.nc",
                "roles": [
                  "data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.04.nc.cas": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.04.nc.cas",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.04.nc.cas",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.04.nc.cas",
                "roles": [
                  "metadata__data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.04.cmr.xml": {
                "href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.04.cmr.xml",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.04.cmr.xml",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.04.cmr.xml",
                "roles": [
                  "metadata__cmr"
                ]
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
          },
          {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "SNDR.SNPP.ATMS.L1A.nominal2.16",
            "properties": {
              "start_datetime": "2016-01-14T11:24:00Z",
              "end_datetime": "2016-01-14T11:30:00Z",
              "created": "2020-12-14T13:50:00Z",
              "updated": "2022-08-15T06:26:25.917000Z",
              "datetime": "2022-08-15T06:26:19.027000Z"
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
              "SNDR.SNPP.ATMS.L1A.nominal2.16.nc": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.16.nc",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.16.nc",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.16.nc",
                "roles": [
                  "data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.16.nc.cas": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.16.nc.cas",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.16.nc.cas",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.16.nc.cas",
                "roles": [
                  "metadata__data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.16.cmr.xml": {
                "href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.16.cmr.xml",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.16.cmr.xml",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.16.cmr.xml",
                "roles": [
                  "metadata__cmr"
                ]
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
          },
          {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "SNDR.SNPP.ATMS.L1A.nominal2.17",
            "properties": {
              "start_datetime": "2016-01-14T11:30:00Z",
              "end_datetime": "2016-01-14T11:36:00Z",
              "created": "2020-12-14T13:50:00Z",
              "updated": "2022-08-15T06:26:25.907000Z",
              "datetime": "2022-08-15T06:26:19.042000Z"
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
              "SNDR.SNPP.ATMS.L1A.nominal2.17.nc": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.17.nc",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.17.nc",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.17.nc",
                "roles": [
                  "data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.17.nc.cas": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.17.nc.cas",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.17.nc.cas",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.17.nc.cas",
                "roles": [
                  "metadata__data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.17.cmr.xml": {
                "href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.17.cmr.xml",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.17.cmr.xml",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.17.cmr.xml",
                "roles": [
                  "metadata__cmr"
                ]
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
          },
          {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "SNDR.SNPP.ATMS.L1A.nominal2.10",
            "properties": {
              "start_datetime": "2016-01-14T10:48:00Z",
              "end_datetime": "2016-01-14T10:54:00Z",
              "created": "2020-12-14T13:50:00Z",
              "updated": "2022-08-15T06:26:25.446000Z",
              "datetime": "2022-08-15T06:26:18.730000Z"
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
              "SNDR.SNPP.ATMS.L1A.nominal2.10.nc": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.10.nc",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.10.nc",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.10.nc",
                "roles": [
                  "data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.10.nc.cas": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.10.nc.cas",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.10.nc.cas",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.10.nc.cas",
                "roles": [
                  "metadata__data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.10.cmr.xml": {
                "href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.10.cmr.xml",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.10.cmr.xml",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.10.cmr.xml",
                "roles": [
                  "metadata__cmr"
                ]
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
          },
          {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "SNDR.SNPP.ATMS.L1A.nominal2.14",
            "properties": {
              "start_datetime": "2016-01-14T11:12:00Z",
              "end_datetime": "2016-01-14T11:18:00Z",
              "created": "2020-12-14T13:50:00Z",
              "updated": "2022-08-15T06:26:25.354000Z",
              "datetime": "2022-08-15T06:26:17.758000Z"
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
              "SNDR.SNPP.ATMS.L1A.nominal2.14.nc": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.14.nc",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.14.nc",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.14.nc",
                "roles": [
                  "data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.14.nc.cas": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.14.nc.cas",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.14.nc.cas",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.14.nc.cas",
                "roles": [
                  "metadata__data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.14.cmr.xml": {
                "href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.14.cmr.xml",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.14.cmr.xml",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.14.cmr.xml",
                "roles": [
                  "metadata__cmr"
                ]
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
          },
          {
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
                "roles": [
                  "data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc.cas",
                "roles": [
                  "metadata__data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml": {
                "href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.cmr.xml",
                "roles": [
                  "metadata__cmr"
                ]
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
          },
          {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "SNDR.SNPP.ATMS.L1A.nominal2.09",
            "properties": {
              "start_datetime": "2016-01-14T10:42:00Z",
              "end_datetime": "2016-01-14T10:48:00Z",
              "created": "2020-12-14T13:50:00Z",
              "updated": "2022-08-15T06:26:24.910000Z",
              "datetime": "2022-08-15T06:26:20.688000Z"
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
              "SNDR.SNPP.ATMS.L1A.nominal2.09.nc": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.09.nc",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.09.nc",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.09.nc",
                "roles": [
                  "data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.09.nc.cas": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.09.nc.cas",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.09.nc.cas",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.09.nc.cas",
                "roles": [
                  "metadata__data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.09.cmr.xml": {
                "href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.09.cmr.xml",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.09.cmr.xml",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.09.cmr.xml",
                "roles": [
                  "metadata__cmr"
                ]
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
          },
          {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "SNDR.SNPP.ATMS.L1A.nominal2.20",
            "properties": {
              "start_datetime": "2016-01-14T11:48:00Z",
              "end_datetime": "2016-01-14T11:54:00Z",
              "created": "2020-12-14T13:50:00Z",
              "updated": "2022-08-15T06:26:23.929000Z",
              "datetime": "2022-08-15T06:26:19.091000Z"
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
              "SNDR.SNPP.ATMS.L1A.nominal2.20.nc": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.20.nc",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.20.nc",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.20.nc",
                "roles": [
                  "data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.20.nc.cas": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.20.nc.cas",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.20.nc.cas",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.20.nc.cas",
                "roles": [
                  "metadata__data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.20.cmr.xml": {
                "href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.20.cmr.xml",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.20.cmr.xml",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.20.cmr.xml",
                "roles": [
                  "metadata__cmr"
                ]
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
          },
          {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "SNDR.SNPP.ATMS.L1A.nominal2.15",
            "properties": {
              "start_datetime": "2016-01-14T11:18:00Z",
              "end_datetime": "2016-01-14T11:24:00Z",
              "created": "2020-12-14T13:50:00Z",
              "updated": "2022-08-15T06:26:23.732000Z",
              "datetime": "2022-08-15T06:26:19.282000Z"
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
              "SNDR.SNPP.ATMS.L1A.nominal2.15.nc": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.15.nc",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.15.nc",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.15.nc",
                "roles": [
                  "data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.15.nc.cas": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.15.nc.cas",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.15.nc.cas",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.15.nc.cas",
                "roles": [
                  "metadata__data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.15.cmr.xml": {
                "href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.15.cmr.xml",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.15.cmr.xml",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.15.cmr.xml",
                "roles": [
                  "metadata__cmr"
                ]
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
          },
          {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "SNDR.SNPP.ATMS.L1A.nominal2.07",
            "properties": {
              "start_datetime": "2016-01-14T10:30:00Z",
              "end_datetime": "2016-01-14T10:36:00Z",
              "created": "2020-12-14T13:50:00Z",
              "updated": "2022-08-15T06:26:23.371000Z",
              "datetime": "2022-08-15T06:26:19.047000Z"
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
              "SNDR.SNPP.ATMS.L1A.nominal2.07.nc": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.07.nc",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.07.nc",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.07.nc",
                "roles": [
                  "data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.07.nc.cas": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.07.nc.cas",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.07.nc.cas",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.07.nc.cas",
                "roles": [
                  "metadata__data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.07.cmr.xml": {
                "href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.07.cmr.xml",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.07.cmr.xml",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.07.cmr.xml",
                "roles": [
                  "metadata__cmr"
                ]
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
          },
          {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "SNDR.SNPP.ATMS.L1A.nominal2.19",
            "properties": {
              "start_datetime": "2016-01-14T11:42:00Z",
              "end_datetime": "2016-01-14T11:48:00Z",
              "created": "2020-12-14T13:50:00Z",
              "updated": "2022-08-15T06:26:23.268000Z",
              "datetime": "2022-08-15T06:26:18.576000Z"
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
              "SNDR.SNPP.ATMS.L1A.nominal2.19.nc": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.19.nc",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.19.nc",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.19.nc",
                "roles": [
                  "data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.19.nc.cas": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.19.nc.cas",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.19.nc.cas",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.19.nc.cas",
                "roles": [
                  "metadata__data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.19.cmr.xml": {
                "href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.19.cmr.xml",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.19.cmr.xml",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.19.cmr.xml",
                "roles": [
                  "metadata__cmr"
                ]
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
          },
          {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "SNDR.SNPP.ATMS.L1A.nominal2.03",
            "properties": {
              "start_datetime": "2016-01-14T10:06:00Z",
              "end_datetime": "2016-01-14T10:12:00Z",
              "created": "2020-12-14T13:50:00Z",
              "updated": "2022-08-15T06:26:22.930000Z",
              "datetime": "2022-08-15T06:26:17.714000Z"
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
              "SNDR.SNPP.ATMS.L1A.nominal2.03.nc": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.03.nc",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.03.nc",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.03.nc",
                "roles": [
                  "data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.03.nc.cas": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.03.nc.cas",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.03.nc.cas",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.03.nc.cas",
                "roles": [
                  "metadata__data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.03.cmr.xml": {
                "href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.03.cmr.xml",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.03.cmr.xml",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.03.cmr.xml",
                "roles": [
                  "metadata__cmr"
                ]
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
          },
          {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "SNDR.SNPP.ATMS.L1A.nominal2.11",
            "properties": {
              "start_datetime": "2016-01-14T10:54:00Z",
              "end_datetime": "2016-01-14T11:00:00Z",
              "created": "2020-12-14T13:50:00Z",
              "updated": "2022-08-15T06:26:22.863000Z",
              "datetime": "2022-08-15T06:26:17.648000Z"
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
              "SNDR.SNPP.ATMS.L1A.nominal2.11.nc": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.11.nc",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.11.nc",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.11.nc",
                "roles": [
                  "data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.11.nc.cas": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.11.nc.cas",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.11.nc.cas",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.11.nc.cas",
                "roles": [
                  "metadata__data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.11.cmr.xml": {
                "href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.11.cmr.xml",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.11.cmr.xml",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.11.cmr.xml",
                "roles": [
                  "metadata__cmr"
                ]
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
          },
          {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "SNDR.SNPP.ATMS.L1A.nominal2.05",
            "properties": {
              "start_datetime": "2016-01-14T10:18:00Z",
              "end_datetime": "2016-01-14T10:24:00Z",
              "created": "2020-12-14T13:50:00Z",
              "updated": "2022-08-15T06:26:22.649000Z",
              "datetime": "2022-08-15T06:26:18.060000Z"
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
              "SNDR.SNPP.ATMS.L1A.nominal2.05.nc": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.05.nc",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.05.nc",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.05.nc",
                "roles": [
                  "data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.05.nc.cas": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.05.nc.cas",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.05.nc.cas",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.05.nc.cas",
                "roles": [
                  "metadata__data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.05.cmr.xml": {
                "href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.05.cmr.xml",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.05.cmr.xml",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.05.cmr.xml",
                "roles": [
                  "metadata__cmr"
                ]
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
          },
          {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "SNDR.SNPP.ATMS.L1A.nominal2.13",
            "properties": {
              "start_datetime": "2016-01-14T11:06:00Z",
              "end_datetime": "2016-01-14T11:12:00Z",
              "created": "2020-12-14T13:50:00Z",
              "updated": "2022-08-15T06:26:22.277000Z",
              "datetime": "2022-08-15T06:26:18.090000Z"
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
              "SNDR.SNPP.ATMS.L1A.nominal2.13.nc": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.13.nc",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.13.nc",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.13.nc",
                "roles": [
                  "data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.13.nc.cas": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.13.nc.cas",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.13.nc.cas",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.13.nc.cas",
                "roles": [
                  "metadata__data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.13.cmr.xml": {
                "href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.13.cmr.xml",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.13.cmr.xml",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.13.cmr.xml",
                "roles": [
                  "metadata__cmr"
                ]
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
          },
          {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "SNDR.SNPP.ATMS.L1A.nominal2.02",
            "properties": {
              "start_datetime": "2016-01-14T10:00:00Z",
              "end_datetime": "2016-01-14T10:06:00Z",
              "created": "2020-12-14T13:50:00Z",
              "updated": "2022-08-15T06:26:22.169000Z",
              "datetime": "2022-08-15T06:26:17.466000Z"
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
              "SNDR.SNPP.ATMS.L1A.nominal2.02.nc": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.02.nc",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.02.nc",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.02.nc",
                "roles": [
                  "data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.02.nc.cas": {
                "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.02.nc.cas",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.02.nc.cas",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.02.nc.cas",
                "roles": [
                  "metadata__data"
                ]
              },
              "SNDR.SNPP.ATMS.L1A.nominal2.02.cmr.xml": {
                "href": "s3://uds-test-cumulus-private/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.02.cmr.xml",
                "title": "SNDR.SNPP.ATMS.L1A.nominal2.02.cmr.xml",
                "description": "SNDR.SNPP.ATMS.L1A.nominal2.02.cmr.xml",
                "roles": [
                  "metadata__cmr"
                ]
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
        ]
        data_assets = StacUtils.reduce_stac_list_to_data_links(granule_json)
        expecting_data_assets = [
  {
    "SNDR.SNPP.ATMS.L1A.nominal2.01.nc": {
      "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.01.nc",
      "title": "SNDR.SNPP.ATMS.L1A.nominal2.01.nc",
      "description": "SNDR.SNPP.ATMS.L1A.nominal2.01.nc",
      "roles": [
        "data"
      ]
    },
    "SNDR.SNPP.ATMS.L1A.nominal2.01_2.nc": {
      "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.01_2.nc",
      "title": "SNDR.SNPP.ATMS.L1A.nominal2.01_2.nc",
      "description": "SNDR.SNPP.ATMS.L1A.nominal2.01_2.nc",
      "roles": [
        "data"
      ]
    }
  },
  {
    "SNDR.SNPP.ATMS.L1A.nominal2.08.nc": {
      "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.08.nc",
      "title": "SNDR.SNPP.ATMS.L1A.nominal2.08.nc",
      "description": "SNDR.SNPP.ATMS.L1A.nominal2.08.nc",
      "roles": [
        "data"
      ]
    }
  },
  {
    "SNDR.SNPP.ATMS.L1A.nominal2.06.nc": {
      "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.06.nc",
      "title": "SNDR.SNPP.ATMS.L1A.nominal2.06.nc",
      "description": "SNDR.SNPP.ATMS.L1A.nominal2.06.nc",
      "roles": [
        "data"
      ]
    }
  },
  {
    "SNDR.SNPP.ATMS.L1A.nominal2.18.nc": {
      "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.18.nc",
      "title": "SNDR.SNPP.ATMS.L1A.nominal2.18.nc",
      "description": "SNDR.SNPP.ATMS.L1A.nominal2.18.nc",
      "roles": [
        "data"
      ]
    }
  },
  {
    "SNDR.SNPP.ATMS.L1A.nominal2.04.nc": {
      "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.04.nc",
      "title": "SNDR.SNPP.ATMS.L1A.nominal2.04.nc",
      "description": "SNDR.SNPP.ATMS.L1A.nominal2.04.nc",
      "roles": [
        "data"
      ]
    }
  },
  {
    "SNDR.SNPP.ATMS.L1A.nominal2.16.nc": {
      "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.16.nc",
      "title": "SNDR.SNPP.ATMS.L1A.nominal2.16.nc",
      "description": "SNDR.SNPP.ATMS.L1A.nominal2.16.nc",
      "roles": [
        "data"
      ]
    }
  },
  {
    "SNDR.SNPP.ATMS.L1A.nominal2.17.nc": {
      "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.17.nc",
      "title": "SNDR.SNPP.ATMS.L1A.nominal2.17.nc",
      "description": "SNDR.SNPP.ATMS.L1A.nominal2.17.nc",
      "roles": [
        "data"
      ]
    }
  },
  {
    "SNDR.SNPP.ATMS.L1A.nominal2.10.nc": {
      "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.10.nc",
      "title": "SNDR.SNPP.ATMS.L1A.nominal2.10.nc",
      "description": "SNDR.SNPP.ATMS.L1A.nominal2.10.nc",
      "roles": [
        "data"
      ]
    }
  },
  {
    "SNDR.SNPP.ATMS.L1A.nominal2.14.nc": {
      "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.14.nc",
      "title": "SNDR.SNPP.ATMS.L1A.nominal2.14.nc",
      "description": "SNDR.SNPP.ATMS.L1A.nominal2.14.nc",
      "roles": [
        "data"
      ]
    }
  },
  {
    "SNDR.SNPP.ATMS.L1A.nominal2.12.nc": {
      "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.12.nc",
      "title": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc",
      "description": "SNDR.SNPP.ATMS.L1A.nominal2.12.nc",
      "roles": [
        "data"
      ]
    }
  },
  {
    "SNDR.SNPP.ATMS.L1A.nominal2.09.nc": {
      "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.09.nc",
      "title": "SNDR.SNPP.ATMS.L1A.nominal2.09.nc",
      "description": "SNDR.SNPP.ATMS.L1A.nominal2.09.nc",
      "roles": [
        "data"
      ]
    }
  },
  {
    "SNDR.SNPP.ATMS.L1A.nominal2.20.nc": {
      "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.20.nc",
      "title": "SNDR.SNPP.ATMS.L1A.nominal2.20.nc",
      "description": "SNDR.SNPP.ATMS.L1A.nominal2.20.nc",
      "roles": [
        "data"
      ]
    }
  },
  {
    "SNDR.SNPP.ATMS.L1A.nominal2.15.nc": {
      "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.15.nc",
      "title": "SNDR.SNPP.ATMS.L1A.nominal2.15.nc",
      "description": "SNDR.SNPP.ATMS.L1A.nominal2.15.nc",
      "roles": [
        "data"
      ]
    }
  },
  {
    "SNDR.SNPP.ATMS.L1A.nominal2.07.nc": {
      "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.07.nc",
      "title": "SNDR.SNPP.ATMS.L1A.nominal2.07.nc",
      "description": "SNDR.SNPP.ATMS.L1A.nominal2.07.nc",
      "roles": [
        "data"
      ]
    }
  },
  {
    "SNDR.SNPP.ATMS.L1A.nominal2.19.nc": {
      "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.19.nc",
      "title": "SNDR.SNPP.ATMS.L1A.nominal2.19.nc",
      "description": "SNDR.SNPP.ATMS.L1A.nominal2.19.nc",
      "roles": [
        "data"
      ]
    }
  },
  {
    "SNDR.SNPP.ATMS.L1A.nominal2.03.nc": {
      "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.03.nc",
      "title": "SNDR.SNPP.ATMS.L1A.nominal2.03.nc",
      "description": "SNDR.SNPP.ATMS.L1A.nominal2.03.nc",
      "roles": [
        "data"
      ]
    }
  },
  {
    "SNDR.SNPP.ATMS.L1A.nominal2.11.nc": {
      "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.11.nc",
      "title": "SNDR.SNPP.ATMS.L1A.nominal2.11.nc",
      "description": "SNDR.SNPP.ATMS.L1A.nominal2.11.nc",
      "roles": [
        "data"
      ]
    }
  },
  {
    "SNDR.SNPP.ATMS.L1A.nominal2.05.nc": {
      "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.05.nc",
      "title": "SNDR.SNPP.ATMS.L1A.nominal2.05.nc",
      "description": "SNDR.SNPP.ATMS.L1A.nominal2.05.nc",
      "roles": [
        "data"
      ]
    }
  },
  {
    "SNDR.SNPP.ATMS.L1A.nominal2.13.nc": {
      "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.13.nc",
      "title": "SNDR.SNPP.ATMS.L1A.nominal2.13.nc",
      "description": "SNDR.SNPP.ATMS.L1A.nominal2.13.nc",
      "roles": [
        "data"
      ]
    }
  },
  {
    "SNDR.SNPP.ATMS.L1A.nominal2.02.nc": {
      "href": "s3://uds-test-cumulus-protected/SNDR_SNPP_ATMS_L1A___1/SNDR.SNPP.ATMS.L1A.nominal2.02.nc",
      "title": "SNDR.SNPP.ATMS.L1A.nominal2.02.nc",
      "description": "SNDR.SNPP.ATMS.L1A.nominal2.02.nc",
      "roles": [
        "data"
      ]
    }
  }
]
        self.assertEqual(json.dumps(data_assets, sort_keys=True), json.dumps(expecting_data_assets, sort_keys=True), f'wrong results')
        print(data_assets)
        return
