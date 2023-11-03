import pandas as pd
import tabula
import requests
import boto3
from io import BytesIO
from database_utils import DatabaseConnector


class DataExtractor:
    def read_rds_table(table_name: str, db_connector: DatabaseConnector):
        return pd.read_sql_table(table_name, db_connector.engine)

    def retrieve_pdf_data(link: str):
        dfs = tabula.read_pdf(link, pages="all", stream=True)
        return pd.concat(dfs, ignore_index=True)

    def list_number_of_stores(endpoint_url: str, headers: dict):
        return requests.get(url=endpoint_url, headers=headers).json()["number_stores"]

    def retrieve_stores_data(headers: dict, number_of_stores: int):
        list_of_dicts = []
        for store_idx in range(number_of_stores):
            url = f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_idx}"
            list_of_dicts.append(requests.get(url, headers=headers).json())

        return pd.DataFrame.from_dict(list_of_dicts)

    def extract_from_s3(link: str):
        client, bucket, key = link.replace("://", "/").split("/")
        client = boto3.client(client)
        prod_obj = client.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(BytesIO(prod_obj["Body"].read()), index_col="Unnamed: 0")
        return df

    def retrieve_json(link: str):
        return pd.DataFrame(requests.get(link).json())
