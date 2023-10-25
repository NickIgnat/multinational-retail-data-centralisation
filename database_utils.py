import yaml
from sqlalchemy import create_engine, inspect


class DatabaseConnector:
    def __init__(self, creds_path):
        self.engine = self.init_db_engine(creds_path)

    def read_db_creds(self, yaml_creds_path):
        with open(yaml_creds_path) as file:
            return yaml.safe_load(file)

    def init_db_engine(self, creds_path):
        creds = self.read_db_creds(creds_path)
        username = creds["USER"]
        password = creds["PASSWORD"]
        host = creds["HOST"]
        port = creds["PORT"]
        database = creds["DATABASE"]

        connection_string = (
            f"postgresql://{username}:{password}@{host}:{port}/{database}"
        )

        engine = create_engine(connection_string)
        return engine

    def list_db_tables(self):
        inspector = inspect(self.engine)
        return inspector.get_table_names()

    def upload_to_db(self, df, table_name):
        df.to_sql(table_name, self.engine, if_exists="replace", index=True)
