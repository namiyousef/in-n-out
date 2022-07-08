from fastapi import FastAPI
from pydantic import BaseModel
from typing import Union, List
import sqlalchemy as db

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

@app.post("/ingest")
def ingest(ingestion_params: IngestionParams, limit: int = -1,):
    ingestion_params = ingestion_params.dict()
    DB_USER = ingestion_params['username']
    DB_PASSWORD = ingestion_params['password']
    DB_HOST = ingestion_params['host']
    DB_PORT = ingestion_params['port']
    DB_NAME = ingestion_params['database_name']

    DATABASE_URI = f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    engine = db.create_engine(DATABASE_URI)
    connection = engine.connect()
    ResultProxy = connection.execute(ingestion_params['sql_query'])
    return ResultProxy.fetchall()


    return {"message": limit}
@app.post("/insert")
def insert():
    return {"message": "Hello World"}