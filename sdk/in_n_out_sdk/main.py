import json
import requests
import pandas as pd
import io

from in_n_out_sdk.config import IN_N_OUT_URL
# TODO needs to be moved in a separate module entirely
from in_n_out_sdk.utils import _convert_timezone_columns_to_utc

# TODO need to understand what content-type and accept do...
# TODO need to run speed comparison for Streaming vs. nonStreaming response!

# TODO need to see what file response does!
# TODO need to see if what we've done now applies for get/put and post request!

headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/octet-stream'
    }

def _is_status_code_valid(status_code):
    if str(status_code).startswith('2'):
        return True


def health_check():
    """Checks if API healthy

    :return: `True` if API healthy, else `False`
    :rtype: bool
    """
    url = f'{IN_N_OUT_URL}/health_check'

    resp = requests.get(url)
    status_code = resp.status_code

    return _is_status_code_valid(status_code)


def write_data(
        database_name,
        table_name,
        limit=-1,
        database_type='pg',
        df=pd.DataFrame(),
        content_type='csv',
        conflict_resolution_strategy='fail',
        username='postgres', # TODO think about how to change API specs to have authentication params instead? MOre generic?
        password='postgres',
        port=5432,
        host='localhost',
   ):

    url = f'{IN_N_OUT_URL}/insert?limit={limit}'

    #df = _convert_timezone_columns_to_utc(df)

    if content_type == 'parquet':
        memory_buffer = io.BytesIO()
        df.to_parquet(
            memory_buffer,
            engine='pyarrow'
        )
        memory_buffer.seek(0)

    data = {
        'insertion_params': json.dumps(dict(
        username=username,
        password=password,
        port=port,
        database_name=database_name,
        table_name=table_name,
        conflict_resolution_strategy=conflict_resolution_strategy,
        host=host
    ))
    }
    # TODO need to update this for diff content types
    files = {
        'file': ('Test', memory_buffer, 'application/octet-stream')
    }
    resp = requests.post(url, data=data, files=files)

    return resp.text, resp.status_code


def read_data(
        ingestion_param,
        limit=-1,
        stream_data=False,
):
    url = f'{IN_N_OUT_URL}/ingest?limit={limit}'

    if not stream_data:
        resp = requests.post(url, json=ingestion_param, headers=headers)
        status_code = resp.status_code
        if _is_status_code_valid(status_code):
            df = pd.read_parquet(io.BytesIO(resp.content), engine='pyarrow')
            return df, status_code
    else:
        with requests.post(url, json=ingestion_param, headers=headers, stream=True) as resp:
            data = b''
            for line in resp.iter_content(1048):
                data += line
            memory_buffer = io.BytesIO(data)
            print(len(data))
            print(pd.read_parquet(memory_buffer, engine='pyarrow'))
        return None, None
    status_code = resp.status_code

    if _is_status_code_valid(status_code):

        df = pd.read_parquet(resp.text, engine='pyarrow')
        #df = resp.json()
        return df, status_code
    return resp.text, resp.status_code

if __name__ == '__main__':

    import datetime as dt
    import pytz
    DF_INITIAL = pd.DataFrame({
        'datetime': [dt.datetime.now()],
        'datetime_with_timestamp': [dt.datetime.now(tz=pytz.timezone('UTC'))],
        'float': [1.0],
        'string': ['test_string'],
        'int': [1],
        'boolean': [True]
    })

    limit=-1
    database_type='test'
    df = DF_INITIAL
    content_type = 'parquet'
    conflict_resolution_strategy = 'replace'
    username = 'postgres'
    password = 'postgres'
    port = 5432
    database_name = 'postgres'
    table_name = 'testing_2'
    host='localhost'
    text, status_code = write_data(
        limit=limit,
        database_type=database_type,
        port=port,
        df=df,
        content_type=content_type,
        conflict_resolution_strategy=conflict_resolution_strategy,
        username=username,
        password=password,
        database_name=database_name,
        table_name=table_name,
        host=host
    )

    print(text, status_code)

