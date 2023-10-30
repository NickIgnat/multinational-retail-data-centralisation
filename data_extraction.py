import pandas as pd
import tabula


class DataExtractor:
    def read_rds_table(table, db_connector):
        return pd.read_sql_table(table, db_connector.engine)

    def retrieve_pdf_data(link):
        dfs = tabula.read_pdf(link, pages="all", stream=True)
        return pd.concat(dfs, ignore_index=True)

    def retrieve_pdf_data(link):
        df = DataExtractor.retrieve_pdf_data(link)
