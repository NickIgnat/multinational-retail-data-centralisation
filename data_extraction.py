import pandas as pd


class DataExtractor:
    def read_rds_table(table, db_connector):
        return pd.read_sql_table(table, db_connector.engine)
