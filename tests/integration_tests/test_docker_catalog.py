import os
import tempfile
import json
from datetime import datetime
from unittest import TestCase

import pystac
from pygeoif import Polygon

from mdps_ds_lib.lib.utils.file_utils import FileUtils
from mdps_ds_lib.stage_in_out.catalog_granules_factory import CatalogGranulesFactory


class TestDockerCatalog(TestCase):
    def test_01(self):
        # Create a temporary directory
        # Create a hysds_stac_output directory
        # Create a catalog.json file. This is a STAC catalog file.
        # Create an algorithm_output_1 directory inside hysds_stac_output
        # Create a file in algorithm_output_1 directory
        # Create a "S1A_IW_GRDH_2SDV_20250330T171421_20250330T171446_058537_073E4F_985B-GRD_HD.json" file in algorithm_output_1
        # This is a STAC item file which has "S1A_IW_GRDH_2SDV" as collection name and its name without extension as id.
        # It should have some assets which are URLs to dummy S3 locations. Each asset should have an "asset" key where 1 is data file and 3 metadat file including this one. 

        # Repeat similar directories for algorithm_output_2 to algorithm_output_5. Similar filename. Same collection. 
        # Repeat for algorithm_output_6. Similar filename, but different collection. "S1B_IW_GRDH_1SDV".

        # teh catalog.json should have relative links to all 6 stac item file locations. 

        with tempfile.TemporaryDirectory() as tmp_dir_name:
            hysds_dir = os.path.join(tmp_dir_name, 'hysds_stac_output')
            FileUtils.mk_dir_p(hysds_dir)
            catalog_path = os.path.join(hysds_dir, 'catalog.json')
            
            # Create STAC catalog using PySTAC
            catalog = pystac.Catalog(
                id="hysds-stac-catalog",
                description="STAC catalog for HYSDS algorithm outputs"
            )
            
            # Define a common geometry for all items
            geometry = Polygon([(-180, -90), (180, -90), (180, 90), (-180, 90), (-180, -90)])
            
            # Create algorithm_output directories and files
            stac_items = []
            
            # algorithm_output_1 to algorithm_output_5 (S1A_IW_GRDH_2SDV collection)
            for i in range(1, 6):
                output_dir = os.path.join(hysds_dir, f'algorithm_output_{i}')
                FileUtils.mk_dir_p(output_dir)

                # Create a sample data file in the directory
                data_file_path = os.path.join(output_dir, f'data_file_{i}.dat')
                with open(data_file_path, 'w') as f:
                    f.write(f'Sample data for algorithm_output_{i}')

                # Create STAC item file using PySTAC
                stac_filename = f"S1A_IW_GRDH_2SDV_20250330T17142{i}_20250330T17144{i}_058537_073E4F_985B-GRD_HD.json"
                stac_item_path = os.path.join(output_dir, stac_filename)
                item_id = stac_filename.replace('.json', '')

                # Create PySTAC Item
                item = pystac.Item(
                    id=item_id,
                    geometry=geometry.__geo_interface__,
                    bbox=list(geometry.bounds),
                    datetime=datetime.fromisoformat(f"2025-03-30T17:14:2{i}+00:00"),
                    properties={},
                    collection="S1A_IW_GRDH_2SDV"
                )

                # Add assets to the item
                data_asset = pystac.Asset(
                    href=f"s3://dummy-bucket/data/{item_id}.tif",
                    media_type=pystac.MediaType.GEOTIFF,
                    roles=["data"],
                    extra_fields={"asset": 1}
                )

                metadata_asset = pystac.Asset(
                    href=f"s3://dummy-bucket/metadata/{item_id}.xml",
                    media_type=pystac.MediaType.XML,
                    roles=["metadata"],
                    extra_fields={"asset": 3}
                )

                stac_item_asset = pystac.Asset(
                    href=f"s3://dummy-bucket/stac/{stac_filename}",
                    media_type=pystac.MediaType.JSON,
                    roles=["metadata"],
                    extra_fields={"asset": 3}
                )

                item.add_asset("data", data_asset)
                item.add_asset("metadata", metadata_asset)
                item.add_asset("stac_item", stac_item_asset)

                # Convert to dict and write to file
                with open(stac_item_path, 'w') as f:
                    json.dump(item.to_dict(False, False), f, indent=2)

                stac_items.append((f"algorithm_output_{i}/{stac_filename}", item))

            # algorithm_output_6 (S1B_IW_GRDH_1SDV collection)
            output_dir = os.path.join(hysds_dir, 'algorithm_output_6')
            FileUtils.mk_dir_p(output_dir)

            # Create a sample data file in the directory
            data_file_path = os.path.join(output_dir, 'data_file_6.dat')
            with open(data_file_path, 'w') as f:
                f.write('Sample data for algorithm_output_6')

            # Create STAC item file for S1B collection using PySTAC
            stac_filename = "S1B_IW_GRDH_1SDV_20250330T171426_20250330T171446_058537_073E4F_985B-GRD_HD.json"
            stac_item_path = os.path.join(output_dir, stac_filename)
            item_id = stac_filename.replace('.json', '')

            # Create PySTAC Item
            item = pystac.Item(
                id=item_id,
                geometry=geometry.__geo_interface__,
                bbox=list(geometry.bounds),
                datetime=datetime.fromisoformat("2025-03-30T17:14:26+00:00"),
                properties={},
                collection="S1B_IW_GRDH_2SDV"
            )
            
            # Add assets to the item
            data_asset = pystac.Asset(
                href=f"s3://dummy-bucket/data/{item_id}.tif",
                media_type=pystac.MediaType.GEOTIFF,
                roles=["data"],
                extra_fields={"asset": 1}
            )
            
            metadata_asset = pystac.Asset(
                href=f"s3://dummy-bucket/metadata/{item_id}.xml",
                media_type=pystac.MediaType.XML,
                roles=["metadata"],
                extra_fields={"asset": 3}
            )
            
            stac_item_asset = pystac.Asset(
                href=f"s3://dummy-bucket/stac/{stac_filename}",
                media_type=pystac.MediaType.JSON,
                roles=["metadata"],
                extra_fields={"asset": 3}
            )
            
            item.add_asset("data", data_asset)
            item.add_asset("metadata", metadata_asset)
            item.add_asset("stac_item", stac_item_asset)
            
            # Convert to dict and write to file
            with open(stac_item_path, 'w') as f:
                json.dump(item.to_dict(False, False), f, indent=2)
            
            stac_items.append((f"algorithm_output_6/{stac_filename}", item))
            
            # Create catalog manually without PySTAC auto-linking to avoid path resolution issues
            catalog_dict = catalog.to_dict(False, False)
            
            # Manually add item links with proper relative paths
            for item_path, _ in stac_items:
                item_link = {
                    "rel": "item",
                    "href": f"./{item_path}",
                    "type": "application/json"
                }
                catalog_dict["links"].append(item_link)
            
            # Write catalog to file
            with open(catalog_path, 'w') as f:
                json.dump(catalog_dict, f, indent=2)

            catalog_to_ds = CatalogGranulesFactory().get_instance(CatalogGranulesFactory.STAC_FAST_API)

            os.environ['SFA_AUTH_KEY'] = 'mod_auth_openidc_session'
            os.environ['SFA_AUTH_VALUE'] = '886dab98-5d3b-4e75-a190-31c08b35a5fc'
            os.environ['DS_URL'] = 'https://www.dev.mdps.mcp.nasa.gov:4443'
            os.environ['DS_STAGE'] = 'stac_fast_api'
            os.environ['UPLOADED_FILES_JSON'] = catalog_path
            os.environ['PROVIDER_ID'] = 'NA'
            result = catalog_to_ds.catalog()
            print(result)
            debug = 2
        return

