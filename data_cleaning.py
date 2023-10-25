from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import pandas as pd
import numpy as np


class DataCleaning:
    def clean_user_data():
        remote_db_connector = DatabaseConnector("remote_db_creds.yaml")
        engine = remote_db_connector.engine
        users = DataExtractor.read_rds_table("legacy_users", remote_db_connector)

        users = users.replace("NULL", np.nan)

        def date_parser(s):
            formats = ["%Y-%m-%d", "%Y %B %d", "%Y/%m/%d", "%B %Y %d"]
            for format in formats:
                try:
                    return pd.to_datetime(s, format=format)
                except ValueError:
                    continue
            return pd.NaT

        users.join_date = users.join_date.apply(date_parser)
        users.date_of_birth = users.date_of_birth.apply(date_parser)

        users.email_address[~users.email_address.str.contains("@", na=False)] = np.nan

        def country_code_formater(c):
            if c not in ["GB", "GGB", "US", "DE"]:
                return np.nan
            elif c == "GGB":
                return "GB"
            else:
                return c

        users.country_code = users.country_code.apply(country_code_formater)
        local_db_connector = DatabaseConnector("local_db_creds.yaml")
        local_db_connector.upload_to_db(users, "dim_users")
