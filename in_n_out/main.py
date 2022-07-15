from fastapi import FastAPI, UploadFile
from pydantic import BaseModel
from typing import Union, List
import sqlalchemy as db
import pandas as pd
from in_n_out.manager import Manager
from in_n_out.client import PostgresClient

app = FastAPI()

class QueryParams(BaseModel):
    columns: List[str] = ['*']
    conditions: List[str]


class IngestionParams(BaseModel):
    sql_query: str
    query: QueryParams
    username: str
    password: str
    port: int
    host: str
    database_name: str

class InsertionParams(BaseModel):
    sql_query: str
    query: QueryParams
    username: str
    password: str
    port: int
    host: str
    database_name: str

@app.post("/ingest")
def ingest(ingestion_params: IngestionParams, limit: int = -1,):
    ingestion_params = ingestion_params.dict()

    DB_USER = ingestion_params['username']
    DB_PASSWORD = ingestion_params['password']
    DB_HOST = ingestion_params['host']
    DB_PORT = ingestion_params['port']
    DB_NAME = ingestion_params['database_name']

    client = PostgresClient(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
    client.initialise_client()
    df = client.query(ingestion_params['sql_query'])

    return df

@app.post("/insert")
def insert(insertion_params: InsertionParams,
           data_files: UploadFile,
           limit: int = -1, ):

    insertion_params = insertion_params.dict()

    DB_USER = insertion_params['username']
    DB_PASSWORD = insertion_params['password']
    DB_HOST = insertion_params['host']
    DB_PORT = insertion_params['port']
    DB_NAME = insertion_params['database_name']

    client = PostgresClient(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)
    client.initialise_client()



    return {"message": "Hello World"}

@app.delete("/delete")
def delete():
    return {"message": "deleted resource"}

@app.post("/ingest_external")
def ingest_external():
    pass