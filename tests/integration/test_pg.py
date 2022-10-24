import pytest
import unittest
from tests.assets.data import DF_INITIAL
from tests.assets.config import DB_NAME, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD

from in_n_out_sdk import write_data, read_data

TEST_TABLE = 'pg_test_asset'
class TestPG(unittest.TestCase):

    @pytest.mark.dependency(depends=['health_check'], scope='session')
    def test_create(self):
        msg, status_code = write_data(
            database_name=DB_NAME,
            table_name=TEST_TABLE,
            database_type='pg',
            df=DF_INITIAL,
            content_type='parquet',
            conflict_resolution_strategy='replace',
            username=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT,
            host=DB_HOST
        )

        assert status_code == 201


    def test_flow(self):
        pass

'''# TODO add test to make sure that df.to_sql has expected behaviour with conflict resolution!

from tests.assets.data import DF_INITIAL

from in_n_out_sdk import write_data
def test_write_pg():
    msg, status_code = write_data(

    )'''
