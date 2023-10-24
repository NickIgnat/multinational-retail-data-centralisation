import yaml
import sqlalchemy
class DatabaseConnector:
    def read_db_creds(yaml_creds_path):
        with open(yaml_creds_path) as file:
            return yaml.load(file)

    def init_db_engine():
        creds = read_db_creds('db_creds.yaml')
        username = creds['username']
        password = creds['password']
        host = creds['host']
        port = creds['port']
        database = creds['database']

        connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"

        engine = create_engine(connection_string)
        return engine
    
    def list_db_tables(engine):
