from sqlalchemy import create_engine
import pandas as pd
import os
import urllib.parse


from dotenv import load_dotenv

load_dotenv()

POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
ENCODED_PASSWORD = urllib.parse.quote(POSTGRES_PASSWORD)


class PostgresConnector:
    def __init__(self):
        self.__clientencoding = "utf8"
        self.DATABASE_URL = f"postgresql://{POSTGRES_USER}:{ENCODED_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"


    def df_to_postgres_table(self, df: pd.DataFrame, table_name: str, db_schema: str = None, if_exists: str = "replace"):
            engine = create_engine(
                self.DATABASE_URL, client_encoding=self.__clientencoding
            )
            df.to_sql(
                name=table_name,
                con=engine,
                schema=db_schema,
                if_exists=if_exists,
                index=False,
            )
            