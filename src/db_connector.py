import os

import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from sqlalchemy import MetaData, Table, create_engine, text


class DbConnector:
    """Object for interacting with the data warehouse"""

    def __init__(
        self,
        client: str = None,
        host: str = None,
        user: str = None,
        password: str = None,
        database: str = None,
        connect_args: dict = {},
        running_locally: bool = True,
    ):
        """
        Constructor for S3BucketConnector

        :param host: host name
        :param user: user name
        :param password: password
        :param database: database name
        :param running_locally: flag indicating if running from a local machine
        """
        self.client = client

        if running_locally:
            load_dotenv()

        if client == "sqlite":
            connect_url = f"sqlite:///{database}"
        else:
            host = os.environ.get(host)
            user = os.environ.get(user)
            pw = os.environ.get(password)
            database = os.environ.get(database)
            connect_url = f"{client}://{user}:{pw}@{host}/{database}"

        self.engine = create_engine(connect_url, connect_args=connect_args)

    def query_to_df(self, query: str) -> pd.DataFrame:
        logger.info("Executing query")
        return pd.read_sql(query, self.engine)

    def table_exists(self, table_name):
        if self.client == "sqlite":
            query = "SELECT * FROM sqlite_master"
            return table_name in list(self.query_to_df(query)["tbl_name"])
        query = f"SHOW TABLES LIKE '{table_name}'"
        return len(self.query_to_df(query)) > 0

    def execute_sql_statement(self, sql):
        with self.engine.connect() as con:
            con.execute(sql)

    def drop_table(self, table_name):
        self.execute_sql_statement(f"DROP TABLE IF EXISTS {table_name};")

    def list_table_columns(self, table_name):
        tbl = Table(table_name, MetaData(), autoload=True, autoload_with=self.engine)
        return tbl.columns.keys()

    def load_df_to_dwh(self, table_name, df, method="append"):
        df.to_sql(name=table_name, con=self.engine, index=False, if_exists="append")

    @staticmethod
    def read_query(query_path: str, query_params: dict = None) -> str:
        with open(query_path) as q:
            query = q.read().format(**query_params)
        return query
