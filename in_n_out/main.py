from fastapi import FastAPI, UploadFile, File, Depends, Form, Query, Response

from pydantic import BaseModel, Json
from typing import Union, List
import sqlalchemy as db
import pandas as pd
from in_n_out.manager import Manager
from in_n_out.client import PostgresClient
import io

app = FastAPI()


class QueryParams(BaseModel):
    columns: List[str] = ['*']
    conditions: List[str]


class IngestionParams(BaseModel):
    sql_query: str
    query: QueryParams = None
    username: str
    password: str
    port: int
    host: str
    database_name: str


class InsertionParams(BaseModel):
    username: str
    password: str
    port: int = 5432
    host: str
    database_name: str
    table_name: str
    conflict_resolution_strategy: str = 'replace'


@app.post("/ingest")
def ingest(
        response: Response,
        ingestion_params: IngestionParams, limit: int = -1, ):
    ingestion_params = ingestion_params.dict()

    DB_USER = ingestion_params['username']
    DB_PASSWORD = ingestion_params['password']
    DB_HOST = ingestion_params['host']
    DB_PORT = ingestion_params['port']
    DB_NAME = ingestion_params['database_name']

    client = PostgresClient(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
    try:
        client.initialise_client()
    except db.exc.OperationalError as e:
        response.status_code = 400 # todo need to get a different error tbh
        return "The client does not seem to be fully operational"
    df = client.query(ingestion_params['sql_query'])
    return df.to_json()

# writing data features
# - single file write operation
# - multiple file, single write operation (simple transaction)
# - nested transaction (if working with multiple data sources, transaction within a transaction!)
# - complex transaction (write, then read and refresh data, then write again!)
# - not sure if these complex operations can work /w simple app design. Need to think about this better!
@app.post("/insert")
async def insert(
        insertion_params: Json[InsertionParams],
                 file: UploadFile = File(...),
                 limit: int = -1):
    insertion_params = insertion_params.dict()
    content = await file.read()

    DB_USER = insertion_params['username']
    DB_PASSWORD = insertion_params['password']
    DB_HOST = insertion_params['host']
    DB_PORT = insertion_params['port']
    DB_NAME = insertion_params['database_name']
    table_name = insertion_params['table_name']
    conflict_resolution_strategy = insertion_params['conflict_resolution_strategy']

    with io.BytesIO(content) as data:
        if 'csv' in file.content_type:
            df = pd.read_csv(data)
        if file.content_type == 'text/tab-separated-values':
            df = pd.read_csv(data, delimiter='\t')
        if file.content_type == 'application/octet-stream': # TODO can you have other 'octet-stream'?
            df = pd.read_parquet(data, engine='pyarrow')
    client = PostgresClient(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
    client.initialise_client()
    df.to_sql(table_name, client.con, if_exists=conflict_resolution_strategy, index=False)

    return {"message": "Hello World"}


@app.delete("/delete")
def delete():
    return {"message": "deleted resource"}


@app.post("/ingest_external")
def ingest_external():
    pass