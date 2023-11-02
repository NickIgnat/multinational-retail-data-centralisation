from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import pandas as pd
import numpy as np


class DataCleaning:
    def date_parser(s):
        formats = ["%Y-%m-%d", "%Y %B %d", "%Y/%m/%d", "%B %Y %d"]
        for format in formats:
            try:
                return pd.to_datetime(s, format=format)
            except ValueError:
                continue
        return pd.NaT

    def clean_user_data():
        remote_db_connector = DatabaseConnector("remote_db_creds.yaml")
        engine = remote_db_connector.engine
        users_df = DataExtractor.read_rds_table("legacy_users", remote_db_connector)

        users_df = users_df.replace("NULL", np.nan)

        users_df.join_date = users_df.join_date.apply(DataCleaning.date_parser)
        users_df.date_of_birth = users_df.date_of_birth.apply(DataCleaning.date_parser)

        users_df.email_address[
            ~users_df.email_address.str.contains("@", na=False)
        ] = np.nan

        def country_code_formater(c):
            if c not in ["GB", "GGB", "US", "DE"]:
                return np.nan
            elif c == "GGB":
                return "GB"
            else:
                return c

        users_df.country_code = users_df.country_code.apply(country_code_formater)

        users_df = users_df.dropna()

        DatabaseConnector("local_db_creds.yaml").upload_to_db(users_df, "dim_users")

    def clean_card_data():
        # getting pdf as a df
        card_df = DataExtractor.retrieve_pdf_data(
            "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        )

        # dropping null values
        card_df = card_df.drop("Unnamed: 0", axis="columns")
        card_df = card_df.replace("NULL", np.nan)
        card_df = card_df.replace("NULL NULL", np.nan)
        card_df.dropna(axis=0, how="all", inplace=True)

        # fixing 'card_number expiry_date' column and dropping it
        card_df["card_number expiry_date"] = card_df.loc[
            card_df["card_number expiry_date"].notna(), "card_number expiry_date"
        ].str.split(" ")
        card_df.card_number[card_df.card_number.isna()] = card_df.loc[
            card_df.card_number.notna(), "card_number expiry_date"
        ].str[0]
        card_df.expiry_date[card_df.expiry_date.isna()] = card_df.loc[
            card_df.expiry_date.notna(), "card_number expiry_date"
        ].str[1][1:]
        card_df = card_df.drop("card_number expiry_date", axis="columns")
        card_df = card_df.dropna()

        # converting date_payment_confirmed into pandas format and dropping rows with incorrect dates
        card_df.date_payment_confirmed = card_df.date_payment_confirmed.apply(
            DataCleaning.date_parser
        )
        card_df = card_df.dropna()

        # dropping rows with incorrect exp. date
        card_df[~card_df.expiry_date.str.contains("/")] = np.nan
        card_df = card_df.dropna()

        # removing ? from card number and converting to numeric
        card_df.card_number = card_df.card_number.astype("str").str.replace("?", "")
        card_df.card_number = card_df.card_number.apply(pd.to_numeric, errors="coerce")

        # storing data in db
        DatabaseConnector("local_db_creds.yaml").upload_to_db(
            card_df, "dim_card_details"
        )

    def called_clean_store_data():
        with open("store_key.yaml") as file:
            store_api_key = yaml.safe_load(file)

        number_of_stores = DataExtractor.list_number_of_stores(
            "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores",
            store_api_key,
        )
        stores_df = DataExtractor.retrieve_stores_data(
            headers=store_api_key, number_of_stores=number_of_stores
        )

        stores_df = stores_df.drop("index", axis=1)
        stores_df = stores_df.replace("NULL", np.nan)
        stores_df.dropna(axis=0, how="all", inplace=True)

        stores_df = stores_df.loc[stores_df.address.str.contains(",")].reset_index(
            drop=True
        )

        stores_df.drop("lat", axis=1, inplace=True)

        stores_df.longitude = stores_df.longitude.astype("float")

        stores_df.staff_numbers = stores_df.staff_numbers.str.replace(
            "[A-Z]|[a-z]", "", regex=True
        )
        stores_df.staff_numbers = stores_df.staff_numbers.astype("int")

        stores_df.opening_date = stores_df.opening_date.apply(DataCleaning.date_parser)

        stores_df.latitude = stores_df.latitude.astype("float")

        stores_df.continent = stores_df.continent.str.replace("ee", "")

        # storing data in db
        DatabaseConnector("local_db_creds.yaml").upload_to_db(
            stores_df, "dim_store_details"
        )
