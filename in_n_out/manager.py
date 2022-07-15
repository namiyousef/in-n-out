from in_n_out.client import PostgresClient


class Manager:
    def __init__(self, ingestion_params):
        database_type = ingestion_params['database_type']
        if database_type == 'pg':
            self.client = PostgresClient(
                username=ingestion_params['username'],
                password=ingestion_params['password'],
                host=ingestion_params['host'],
                port=ingestion_params['port'],
                name=ingestion_params['database_name']
            )

    def preprocess(self):
        pass

    def process(self):
        pass

    def postprocess(self):
        pass

