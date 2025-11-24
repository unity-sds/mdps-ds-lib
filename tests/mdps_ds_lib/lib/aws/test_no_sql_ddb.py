import os
from time import sleep
from unittest import TestCase

from mdps_ds_lib.lib.aws.no_sql_abstract import NoSqlProps
from mdps_ds_lib.lib.aws.no_sql_ddb import NoSqlDdb
from mdps_ds_lib.lib.aws.no_sql_factory import NoSqlFactory


class TestNoSqlDdb(TestCase):
    def test_create_table_01(self):

        ddb_props = NoSqlProps()
        ddb_props.table = 'h5s_on_disk_william_local'
        ddb_props.primary_key = 'userGroup'
        ddb_props.secondary_key = 'projectMap'

        param = ddb_props.to_json()
        param['file_repo'] = 'AWS_DDB'

        ddb: NoSqlDdb = NoSqlFactory().get_instance(**param)
        self.assertTrue(isinstance(ddb, NoSqlDdb), 'not NoSqlDdb instance')
        self.assertFalse(ddb.has_table(), f'Already has table?')

        ddb.create_tbl(gsi=[
            {
                'IndexName': 'GSI1_UserGroup',
                'KeySchema': [
                    {
                        'AttributeName': 'userGroup',
                        'KeyType': 'HASH'  # Partition key
                    }
                ],
                'Projection': {
                    'ProjectionType': 'KEYS_ONLY'
                },
            }
        ])
        sleep(1.5)
        self.assertTrue(ddb.has_table(), f'NO Table?')
        return

    def test_add_items(self):
        ddb_props = NoSqlProps()
        ddb_props.table = 'h5s_on_disk_william_local'
        ddb_props.primary_key = 'userGroup'
        ddb_props.secondary_key = 'projectMap'

        param = ddb_props.to_json()
        param['file_repo'] = 'AWS_DDB'

        ddb: NoSqlDdb = NoSqlFactory().get_instance(**param)
        self.assertTrue(ddb.has_table(), f'NO Table?')
        adding_item_1 = {
            'userGroup': 'A',
            'projectMap': '',
            'sourceProject': 'X:Y:.*',
            'targetProject': '.*',
            'access': False,
        }
        adding_item_2 = {
            'userGroup': 'A',
            'projectMap': '',
            'sourceProject': 'X:Y:L0.*',
            'targetProject': 'M:N:L0.*',
            'access': False,
        }

        adding_item_3 = {
            'userGroup': 'A',
            'projectMap': '',
            'sourceProject': 'X:Y:L0_V1',
            'targetProject': 'M:N:L0.*',
            'access': False,
        }

        adding_item_4 = {
            'userGroup': 'A',
            'projectMap': '',
            'sourceProject': 'X:Y:L0.*',
            'targetProject': 'M:N:L0.*',
            'access': True,
        }

        adding_item_5 = {
            'userGroup': 'A',
            'projectMap': '',
            'sourceProject': 'X:Y:L1_V1',
            'targetProject': 'M:N:L1.*',
            'access': True,
        }

        adding_items = [adding_item_1,  adding_item_2, adding_item_3, adding_item_4, adding_item_5]
        for each_adding_item in adding_items:
            sk1 = f'{each_adding_item["sourceProject"]}->{each_adding_item["targetProject"]}'
            ddb.add(each_adding_item['userGroup'], sk1, each_adding_item, replace=True)

        return

    def test_query_items(self):
        ddb_props = NoSqlProps()
        ddb_props.table = 'h5s_on_disk_william_local'
        ddb_props.primary_key = 'userGroup'
        ddb_props.secondary_key = 'projectMap'

        param = ddb_props.to_json()
        param['file_repo'] = 'AWS_DDB'

        ddb: NoSqlDdb = NoSqlFactory().get_instance(**param)
        self.assertTrue(ddb.has_table(), f'NO Table?')
        results = ddb.get('A', secondary_key=None)
        self.assertEqual(len(results), 4, f'wrong number of results: {results}')
        results = ddb.get('A', secondary_key='X:Y:L0.*', secondary_key_operation='begins_with')
        self.assertEqual(len(results), 1, f'wrong number of results: {results}')
        results = ddb.get('A', secondary_key='X:Y:L0', secondary_key_operation='begins_with')
        self.assertEqual(len(results), 2, f'wrong number of results: {results}')
        results = ddb.get('A', secondary_key='X:Y:L0', secondary_key_operation='eq')
        self.assertEqual(results, None, f'wrong number of results: {results}')
        results = ddb.get('A', secondary_key='X:Y:L1_V1->M:N:L1.*', secondary_key_operation='eq')
        self.assertEqual(len(results), 1, f'wrong number of results: {results}')
        debug = 1
        return

    def test_query_gsi(self):
        ddb_props = NoSqlProps()
        ddb_props.table = 'h5s_on_disk_william_local'
        ddb_props.primary_key = 'userGroup'
        ddb_props.secondary_key = 'projectMap'

        param = ddb_props.to_json()
        param['file_repo'] = 'AWS_DDB'

        ddb: NoSqlDdb = NoSqlFactory().get_instance(**param)
        self.assertTrue(ddb.has_table(), f'NO Table?')
        result = ddb.query_gsi('GSI1_UserGroup')
        debug = 1
        return
