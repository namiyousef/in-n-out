import pandas as pd
import numpy as np
import datetime as dt
import pytz

DF_INITIAL = pd.DataFrame({
    'datetime': [dt.datetime.now()],
    'datetime_utc': [dt.datetime.now(tz=pytz.timezone('UTC'))],
    'datetime_pacific': [dt.datetime.now(tz=pytz.timezone('US/Pacific'))],
    'float': [1.0],
    'string': ['test_string'],
    'int': [1],
    'boolean': [True],
})

DF_NULLS = pd.DataFrame({
    'datetime': [np.datetime64('NaT'), None, np.nan],
    #'float': [],
    #'string': [],
    #'boolean': []
})

if __name__ == '__main__':
    DF_INITIAL.info()

    DF_NULLS.info()
    '''save_type = 'parquet'
    stress = False
    name = f'df_initial.{save_type}' if not stress else f'df_stress.{save_type}'
    df_save = DF_INITIAL if not stress else pd.concat([DF_INITIAL]*50000)
    if save_type == 'parquet':
        df_save.to_parquet(name, engine='pyarrow')'''

    df_1 = pd.DataFrame({'test': [dt.datetime.now(tz=pytz.timezone('UTC'))]})
    df_2 = pd.DataFrame({'test': [dt.datetime.now(tz=pytz.timezone('US/Pacific'))]})
    df_1.info()
    df_2.info()
