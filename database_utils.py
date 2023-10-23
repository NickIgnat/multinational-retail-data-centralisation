import yaml
import sqlalchemy
class DatabaseConnector:
    def read_db_creds(yaml_creds_path):
        with open(yaml_creds_path) as file:
            return yaml.load(file)


    def init_db_engine():
        engine = None
        pass