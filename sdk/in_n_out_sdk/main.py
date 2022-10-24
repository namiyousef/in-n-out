import json
import requests
import pandas as pd
import io

from in_n_out_sdk.config import IN_N_OUT_URL

headers = {
        'Content-Type': 'application/json',
        'Accept': 'text/plain'
    }

def _is_status_code_valid(status_code):
    if str(status_code).startswith('2'):
        return True


def health_check():
    url = f'{IN_N_OUT_URL}/health_check'

    resp = requests.get(url)
    status_code = resp.status_code

    return _is_status_code_valid(status_code)


def write_data(limit,
               database_type,
               df,
               content_type,
               conflict_resolution_strategy,
               username, # TODO think about how to change API specs to have authentication params instead? MOre generic?
               password,
               port,
               host,
               database_name,
               table_name,
   ):

    url = f'{IN_N_OUT_URL}/insert?limit={limit}'

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
    files = {
        'file': ('Test', memory_buffer, 'application/octet-stream')
    }
    resp = requests.post(url, data=data, files=files)

    return resp.text, resp.status_code


def read_data():
    pass

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

