# TODO must move test if moving the functions to separate pandas sdk!
import unittest

from in_n_out_sdk.utils import _convert_timezone_columns_to_utc

from tests.assets.data import DF_INITIAL
import pandas as pd
from pandas.testing import assert_frame_equal

class TestUtils(unittest.TestCase):

    def test_tz_conversion(self):

        df_output = _convert_timezone_columns_to_utc(DF_INITIAL)
        df_expected = DF_INITIAL.assign(datetime_pacific=lambda x: pd.to_datetime(x['datetime_pacific'], utc=True))

        assert_frame_equal(df_output, df_expected)

if __name__ == '__main__':
    unittest.main()