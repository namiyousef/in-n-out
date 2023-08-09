from in_n_out.client import PostgresClient

# need support for transactions, need to think of a smart way to chain items
# together
# also may need to have multiple transactions
# need to also think about how that works with respect to deletion


class Manager:
    def __init__(self, ingestion_params):
        database_type = ingestion_params["database_type"]
        if database_type == "pg":
            self.client = PostgresClient(
                username=ingestion_params["username"],
                password=ingestion_params["password"],
                host=ingestion_params["host"],
                port=ingestion_params["port"],
                name=ingestion_params["database_name"],
            )
            self.initialise_client()

    def preprocess(self):
        # prepare the query
        pass

    def process(self):
        # execute the query
        pass

    def postprocess(self):
        # fix empty dataframe by adding schema
        # here can maybe do data transformations, e.g. for ingest endpoint.
        # Not sure if insert will have postprocess

        pass
