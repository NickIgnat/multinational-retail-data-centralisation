from data_cleaning import DataCleaning
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
import requests


number_of_stores = DataExtractor.list_number_of_stores(
    "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores",
    {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"},
)
