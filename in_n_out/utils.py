import pandas as pd
from pandas.api.types import is_datetime64tz_dtype

def _convert_timezone_columns_to_utc(df):
    for col, dtype in df.dtypes.items():
        if is_datetime64tz_dtype(dtype):
            df[col] = pd.to_datetime(df[col], utc=True)

    return df