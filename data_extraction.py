import pandas as pd
import tabula
import requests


class DataExtractor:
    def read_rds_table(table, db_connector):
        return pd.read_sql_table(table, db_connector.engine)

    def retrieve_pdf_data(link):
        dfs = tabula.read_pdf(link, pages="all", stream=True)
        return pd.concat(dfs, ignore_index=True)

    def list_number_of_stores(endpoint_url, headers):
        return requests.get(url=endpoint_url, headers=headers).json()["number_stores"]

    def retrieve_stores_data(headers, number_of_stores):
        list_of_dicts = []
        for store_idx in range(number_of_stores):
            url = f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_idx}"
            list_of_dicts.append(requests.get(url, headers=headers).json())

        return pd.DataFrame.from_dict(list_of_dicts)
