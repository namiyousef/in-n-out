import sqlalchemy as db
import pandas as pd


class GoogleDriveClient:
    def __init__(self):
        pass

    def initialise_client(self):
        print('testing')
class PostgresClient:
    def __init__(self, username, password, host, port, name):
        self.db_user = username
        self.db_password = password
        self.db_host = host
        self.db_port = port
        self.db_name = name
        self.db_uri = f'postgresql+psycopg2://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}'

    def initialise_client(self):
        self.engine = db.create_engine(self.db_uri)
        self.con = self.engine.connect()

    def query(self, query):
        query_result = self.con.execute(query)
        data = query_result.fetchall()
        columns = query_result.keys()
        df = pd.DataFrame(data, columns=columns)
        return df


DATABASE_CLIENT_MAP = {
    "pg": PostgresClient,
    "gdrive": GoogleDriveClient
}
class UniversalClient(PostgresClient, GoogleDriveClient):

    def __init__(self, database_type, ):
        self.client = DATABASE_CLIENT_MAP[database_type]()

    def initialise_client(self):
        self.client.initialise_client()


    def write_to_sink(self):
        pass

    def read_from_source(self):
        pass


if __name__ == '__main__':
    client = UniversalClient('gdrive')
    client.initialise_client()