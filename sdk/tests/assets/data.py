import pandas as pd
import numpy as np
import datetime as dt
import pytz
# TODO this import needs to come from somewhere else
from in_n_out_sdk.utils import _convert_timezone_columns_to_utc
from functools import partial

_gen_date = partial(dt.datetime, 1999, 12, 31)
MILLENNIUM_END = _gen_date()
MILLENNIUM_END_UTC = _gen_date(tzinfo=pytz.timezone('UTC'))
MILLENNIUM_END_PACIFIC = _gen_date(tzinfo=pytz.timezone('US/Pacific'))
MILLENNIUM_END_OFFSET = _gen_date(tzinfo=pytz.FixedOffset(1))
DF_INITIAL = pd.DataFrame({
    'datetime': [MILLENNIUM_END],
    'datetime_utc': [MILLENNIUM_END_UTC],
    'datetime_pacific': [MILLENNIUM_END_PACIFIC],
    'datetime_offset': [MILLENNIUM_END_OFFSET],
    'float': [1.0],
    'string': ['test_string'],
    'int': [1],
    'boolean': [True],
})
# TODO also missing coverage is having NaT on datetime with offsets?
DF_MIXED = pd.DataFrame({
    'datetime': [MILLENNIUM_END_OFFSET, None],
    'datetime_utc': [MILLENNIUM_END, np.datetime64('NaT')],
    'datetime_pacific': [MILLENNIUM_END_UTC, np.nan],
    # missing final datetime (offset)
    'float': [np.nan, None],
    'string': ['', None],
    'int': [2, 3], # TODO missing test coverage: int that gets corrupted into float by nan
    'boolean': [False, None]
})

# TODO bug! if you pass a date to date+tz then pandas will not cause an error!
DF_INITIAL_LOCALIZED = _convert_timezone_columns_to_utc(DF_INITIAL.copy())
DF_MIXED_LOCALIZED = _convert_timezone_columns_to_utc(DF_MIXED.copy())