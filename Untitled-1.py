from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import pandas as pd
import numpy as np
from data_cleaning import DataCleaning
import requests
import yaml


with open("store_key.yaml") as file:
    store_api_key = yaml.safe_load(file)


number_of_stores = DataExtractor.list_number_of_stores(
    "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores",
    store_api_key,
)


stores_df = DataExtractor.retrieve_stores_data(
    headers=store_api_key, number_of_stores=number_of_stores
)
