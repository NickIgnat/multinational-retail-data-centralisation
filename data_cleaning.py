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

    def clean_user_data(self):
        remote_db_connector = DatabaseConnector("remote_db_creds.yaml")
        engine = remote_db_connector.engine
        users_df = DataExtractor.read_rds_table("legacy_users", remote_db_connector)

        users_df = users_df.replace("NULL", np.nan)

        users_df.join_date = users_df.join_date.apply(self.date_parser)
        users_df.date_of_birth = users_df.date_of_birth.apply(self.date_parser)

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

        local_db_connector = DatabaseConnector("local_db_creds.yaml")
        local_db_connector.upload_to_db(users_df, "dim_users")

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
