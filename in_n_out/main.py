import io
from typing import List

import pandas as pd
import sqlalchemy as db
from fastapi import FastAPI, File, Response, UploadFile
from fastapi.responses import StreamingResponse
from pandas.api.types import is_datetime64tz_dtype
from pydantic import BaseModel, Json
from sqlalchemy import BOOLEAN, FLOAT, INTEGER, TIMESTAMP, VARCHAR

from in_n_out.client import GoogleMailClient, PostgresClient

app = FastAPI()


class QueryParams(BaseModel):
    columns: List[str] = ["*"]
    conditions: List[str]


class EmailParams(BaseModel):
    sender_email: str
    recipient_email: list
    password: str
    subject: str = None
    message_id: str = None
    content: str = None


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
    conflict_resolution_strategy: str = "replace"


@app.get("/health_check")
def health_check():
    return "API Healthy"


# TODO at the moment we only ingest parquet. Need to compare with doing
# different types!
# TODO we are using post because the contents don't get cahced in the server
# logs
@app.post("/ingest")
def ingest(
    response: Response,
    ingestion_params: IngestionParams,
    limit: int = -1,
):
    ingestion_params = ingestion_params.dict()

    DB_USER = ingestion_params["username"]
    DB_PASSWORD = ingestion_params["password"]
    DB_HOST = ingestion_params["host"]
    DB_PORT = ingestion_params["port"]
    DB_NAME = ingestion_params["database_name"]

    client = PostgresClient(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)

    # Logic should be: initialise client
    # ingest data
    # apply any processing to the data
    # return the data

    # questions to answer? Will you need to use this as a user, e.g.
    # want to see the results in .json?
    # response on read should always be 200
    try:
        client.initialise_client()
    except db.exc.OperationalError as e:
        response.status_code = 400  # todo need to get a different error tbh
        return f"The client does not seem to be fully operational. Error: {e}"
    df = client.query(ingestion_params["sql_query"])
    # TODO on writing we want to correct to the timezone of the sink
    # TODO on reading, we want to read as UTC

    return Response(
        df.to_parquet(engine="pyarrow", index=False),
        media_type="application/octet-stream",
    )

    def iterfile(df):
        mem = io.BytesIO()
        df.to_parquet(mem, engine="pyarrow", index=False)
        mem.seek(0)
        yield from mem

    return StreamingResponse(
        iterfile(df), media_type="application/octet-stream"
    )


# writing data features
# - single file write operation
# - multiple file, single write operation (simple transaction)
# - nested transaction (if working with multiple data sources,
# transaction within a transaction!)
# - complex transaction (write, then read and refresh data, then write again!)
# - not sure if these complex operations can work /w simple app design.
# Need to think about this better!
@app.post("/insert")
async def insert(
    response: Response,
    insertion_params: Json[InsertionParams],
    file: UploadFile = File(...),
    limit: int = -1,
):
    insertion_params = insertion_params.dict()
    content = await file.read()

    DB_USER = insertion_params["username"]
    DB_PASSWORD = insertion_params["password"]
    DB_HOST = insertion_params["host"]
    DB_PORT = insertion_params["port"]
    DB_NAME = insertion_params["database_name"]
    table_name = insertion_params["table_name"]
    conflict_resolution_strategy = insertion_params[
        "conflict_resolution_strategy"
    ]
    dataset_name = insertion_params.get("dataset_name", None)
    schema = insertion_params.get(
        "schema", {}
    )  # need to decide: will schema do a data transformation, or is it just
    # for creating the asset?
    # TODO need to onboard more usecases, e.g. bigquery, gcs, gdrive
    print(schema)

    # TODO need to clean this up!
    with io.BytesIO(content) as data:
        if "csv" in file.content_type:
            df = pd.read_csv(data)
        if file.content_type == "text/tab-separated-values":
            df = pd.read_csv(data, delimiter="\t")
        if (
            file.content_type == "application/octet-stream"
        ):  # TODO can you have other 'octet-stream'?
            df = pd.read_parquet(data, engine="pyarrow")
    client = PostgresClient(DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)

    DTYPE_MAP = {
        "int64": INTEGER,
        "float64": FLOAT,
        "datetime64[ns]": TIMESTAMP,
        "datetime64[ns, UTC]": TIMESTAMP(timezone=True),
        "bool": BOOLEAN,
        "object": VARCHAR,
    }

    def _get_pg_datatypes(df):
        dtypes = {}
        for col, dtype in df.dtypes.items():
            if is_datetime64tz_dtype(dtype):
                dtypes[col] = DTYPE_MAP["datetime64[ns, UTC]"]
            else:
                dtypes[col] = DTYPE_MAP[str(dtype)]
        return dtypes

    try:
        client.initialise_client()
    except db.exc.OperationalError as e:
        response.status_code = 400  # todo need to get a different error tbh
        return f"The client does not seem to be fully operational. Error: {e}"

    dtypes = _get_pg_datatypes(df)

    # TODO this is in the case of postgres!
    df.to_sql(
        table_name,
        client.con,
        schema=dataset_name,
        if_exists=conflict_resolution_strategy,
        index=False,
        method="multi",
        dtype=dtypes,
    )
    response.status_code = 201  # TODO technically not correct because we don't
    # always created the asset, sometimes it is simply a 200 response!

    return "Created table"


@app.delete("/delete")
def delete():
    return {"message": "deleted resources"}


@app.post("/ingest_external")
def ingest_external():
    pass


@app.post("/email/gmail")
async def send_gmail(
    response: Response,
    email_params: Json[EmailParams],
    files: List[UploadFile],
):
    email_params = email_params.dict()
    client = GoogleMailClient(email_params)

    for file in files:
        content = await file.read()
        filename = file.filename
        client.add_attachment(content, filename)
    client.send_email()

    return client.message_id


if __name__ == "__main__":
    # TODO need to learn how to add custom logging to this, and also how to
    # enable reload
    import uvicorn

    log_config = uvicorn.config.LOGGING_CONFIG
    log_config["formatters"]["access"][
        "fmt"
    ] = "%(asctime)s - %(levelname)s - %(message)s"
    log_config["formatters"]["default"][
        "fmt"
    ] = "%(asctime)s - %(levelname)s - %(message)s"
    uvicorn.run(app, log_config=log_config)

    """def write_log_data(request, response):
        print('just testing')


    @app.middleware("http")
    async def log_request(request: Request, call_next):
        response = await call_next(request)
        response.background = BackgroundTask(write_log_data, request, response)
        return response"""
