import pytest
import unittest
from tests.assets.data import DF_INITIAL, DF_INITIAL_LOCALIZED, DF_MIXED, DF_MIXED_LOCALIZED
from tests.assets.config import DB_NAME, DB_HOST, DB_PORT, DB_USER, DB_PASSWORD
import pandas as pd
from pandas.testing import assert_frame_equal

from in_n_out_sdk import write_data, read_data

TEST_TABLE = 'pg_test_asset'
class TestPG(unittest.TestCase):

    @pytest.mark.dependency(name='pg_create', depends=['health_check'], scope='session')
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
        # TODO need to work on your status_codes!
        assert status_code == 201

    @pytest.mark.dependency(depends=['pg_create'], scope='session')
    def test_flow(self):

        df, status_code = read_data(
            ingestion_param=dict(
                sql_query=f'select * from {TEST_TABLE}',
                username=DB_USER,
                password=DB_PASSWORD,
                port=DB_PORT,
                host=DB_HOST,
                database_name=DB_NAME,
            ),
            stream_data=False
        )
        assert status_code == 200
        assert_frame_equal(df, DF_INITIAL_LOCALIZED, check_like=True)

        # TODO add a test that tries to push datetimetz to datetime and vice versa!
        df, status_code = write_data(
            database_name=DB_NAME,
            table_name=TEST_TABLE,
            database_type='pg',
            df=DF_MIXED,
            content_type='parquet',
            conflict_resolution_strategy='append',
            username=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT,
            host=DB_HOST
        )
        assert status_code == 201

        df, status_code = read_data(
            ingestion_param=dict(
                sql_query=f'select * from {TEST_TABLE}',
                username=DB_USER,
                password=DB_PASSWORD,
                port=DB_PORT,
                host=DB_HOST,
                database_name=DB_NAME,
            ),
            stream_data=False
        )

        assert status_code == 200
        df_expected = pd.concat([DF_INITIAL_LOCALIZED, DF_MIXED_LOCALIZED]).reset_index(drop=True).assign(
            datetime=lambda x: pd.to_datetime(x['datetime'], utc=True).dt.tz_localize(None),
            datetime_utc=lambda x: pd.to_datetime(x['datetime_utc'], utc=True)

        )
        assert_frame_equal(df, df_expected, check_like=True)
