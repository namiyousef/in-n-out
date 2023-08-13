import io
import json
import logging
from functools import partial
from typing import List

import pandas as pd
import sqlalchemy as db
from fastapi import FastAPI, File, Response, UploadFile
from fastapi.responses import StreamingResponse
from in_n_out_clients import InNOutClient
from in_n_out_clients.email_client import EmailClient
from in_n_out_clients.in_n_out_types import ConflictResolutionStrategy
from in_n_out_clients.postgres_client import PostgresClient
from pydantic import BaseModel, Json

app = FastAPI()


def get_file_type(file):
    if "json" in file.content_type:
        return "json"
    elif "csv" in file.content_type:
        return "csv"
    elif file.content_type == "text/tab-separated-values":
        return "tsv"
    elif file.content_type == "application/octet-stream":
        return "parquet"


FILE_TYPE_TO_PANDAS_PARSER_MAPPING = {
    "json": pd.read_json,
    "csv": pd.read_csv,
    "tsv": partial(pd.read_csv, delimeter="\t"),
    "parquet": partial(pd.read_parquet, engine="pyarrow"),
}

FILE_TYPE_PARSER_MAPPING = {"json": json.load}


"""with open('../logging_config.yaml') as f:
    logging_config = yaml.safe_load(f.read())

print(logging_config)

logging.config.dictConfig(logging_config)
logger = logging.getLogger('simpleExample')


logger.info('logging please?')"""


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
    table_name: str
    database_type: str
    database_name: str | None = None

    on_data_conflict: ConflictResolutionStrategy = "fail"
    on_asset_conflict: ConflictResolutionStrategy = "append"
    data_conflict_properties: List[str] | None = None
    username: str | None = None
    password: str | None = None
    port: int | None = None
    host: str | None = None
    dataset_name: str | None = None


# for calendar, database_type is to indicate which calendar resource, e.g.
# gmail, outlook...
# the database_name should indicate the specific calendar to write to
# dataset is not used
# table_name: this is the individual calendar to use

logger = logging.getLogger("simple_example")
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)


@app.get("/health_check")
def health_check():
    logger.info("Checking api health")
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

    # questions to answer? Will you need to use this as a user, e.g. want to
    # see the results in .json?
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
# - nested transaction (if working with multiple data sources, transaction
# #within a transaction!)
# - complex transaction (write, then read and refresh data, then write again!)
# - not sure if these complex operations can work /w simple app design.
# #Need to think about this better!


@app.post("/insert")
async def insert(
    response: Response,
    insertion_params: Json[InsertionParams],
    file: UploadFile = File(...),
    limit: int = -1,
    read_data_as_dataframe: bool = True,
):
    # -- read inputs to api
    insertion_params = insertion_params.dict()
    logger.info("Loading input file...")
    content = await file.read()

    DB_USER = insertion_params["username"]
    DB_PASSWORD = insertion_params["password"]
    DB_HOST = insertion_params["host"]
    DB_PORT = insertion_params["port"]
    DB_NAME = insertion_params["database_name"]
    DB_TYPE = insertion_params["database_type"]
    table_name = insertion_params["table_name"]
    on_data_conflict = insertion_params["on_data_conflict"]
    on_asset_conflict = insertion_params["on_asset_conflict"]
    data_conflict_properties = insertion_params["data_conflict_properties"]

    dataset_name = insertion_params.get("dataset_name", None)
    schema = insertion_params.get(
        "schema", {}
    )  # need to decide: will schema do a data transformation, or is it just
    # for creating the asset?
    # TODO need to onboard more usecases, e.g. bigquery, gcs, gdrive
    print(schema, dataset_name)

    # TODO need to clean this up!

    # -- parse input data
    logger.info("Parsing data...")
    with io.BytesIO(content) as data:
        file_type = get_file_type(file)
        logger.info(f"Got file_type `{file_type}`")
        if read_data_as_dataframe:
            file_reader = FILE_TYPE_TO_PANDAS_PARSER_MAPPING.get(file_type)
        else:
            file_reader = FILE_TYPE_PARSER_MAPPING.get(file_type)

        if file_reader is None:
            response.status_code = 400
            _msg = (
                f"There is no parser for file_type={file_type} and "
                f"read_data_as_dataframe={read_data_as_dataframe}"
            )
            return {"status_code": 400, "msg": _msg}

        data = file_reader(data)

    # -- connect to client
    logger.info(f"Connecting to `{DB_TYPE}` client...")
    try:
        client = InNOutClient(
            database_type=DB_TYPE,
            database_name=DB_NAME,
            password=DB_PASSWORD,
            username=DB_USER,
            host=DB_HOST,
            port=DB_PORT,
        )
    except ConnectionError as connection_error:
        _msg = (
            f"There was a problem connecting to the `{DB_TYPE}` client. "
            f"Reason: {connection_error}"
        )
        response.status_code = 400
        logger.error(_msg)
        return {
            "status_code": 400,
            "msg": _msg,
        }
    except NotImplementedError as not_implemented_error:
        _msg = (
            f"The `{DB_TYPE}` client is not implemented. "
            f"Reason: {not_implemented_error}"
        )
        response.status_code = 501
        logger.error(_msg)
        return {
            "status_code": 501,
            "msg": _msg,
        }
    # TODO -- preprocess data, using mix-n-match
    # TODO -- maybe read data, e.g. if ingestion parameteres provided...
    # Not sure if I want to allow this though!

    # -- write data
    try:
        resp = client.write(
            table_name=table_name,
            data=data,
            dataset_name=dataset_name,
            on_data_conflict=on_data_conflict,
            on_asset_conflict=on_asset_conflict,
            data_conflict_properties=data_conflict_properties,
        )
    except NotImplementedError as not_implemented_error:
        _msg = (
            "Got inputs requesting an operation that has not been "
            f"implemented. Details: {not_implemented_error}"
        )
        response.status_code = 501
        logger.error(_msg)
        return {"status_code": 501, "msg": _msg}
    # TODO for each client, need to add a data vlidation errro and return that!
    except Exception as exception:
        _msg = f"There was an error in processing the request. Reason: {exception}"
        response.status_code = 500
        logger.error(_msg)
        return {"status_code": 500, "msg": _msg}

    response.status_code = resp["status_code"]
    return resp


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
    client = EmailClient("gmail", email_params)

    for file in files:
        content = await file.read()
        filename = file.filename
        client.add_attachment(content, filename)
    client.send_email()

    return client.message_id


if __name__ == "__main__":
    # TODO need to learn how to add custom logging to this, and also how to
    # nable reload
    """import uvicorn
    log_config = uvicorn.config.LOGGING_CONFIG
    log_config["formatters"]["access"]["fmt"] = "%(asctime)s - %(levelname)s
    - %(message)s"
    log_config["formatters"]["default"]["fmt"] = "%(asctime)s - %(levelname)s
    - %(message)s"
    uvicorn.run(app, log_config=log_config)"""

    """def write_log_data(request, response):
        print('just testing')


    @app.middleware("http")
    async def log_request(request: Request, call_next):
        response = await call_next(request)
        response.background = BackgroundTask(write_log_data, request, response)
        return response"""
