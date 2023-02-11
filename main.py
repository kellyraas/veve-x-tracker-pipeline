import os

import gspread
import pandas as pd
import requests
from loguru import logger

from src.db_connector import DbConnector
from src.transformers import transform_collectibles_data, transform_comics_data
from src.utils import get_date_x_days_ago, load_config


def init_db_connection() -> DbConnector:
    """Initializes database connection"""
    db_config = load_config("connections").db.sqlite
    db = DbConnector(
        client=db_config.get("client"),
        host=db_config.get("host"),
        user=db_config.get("user"),
        password=db_config.get("password"),
        database=db_config.get("database"),
        connect_args=db_config.get("connect_args"),
        running_locally=True,
    )
    return db


def get_transfers_data(db: DbConnector) -> pd.DataFrame:
    """Runs query to extract daily transfer data"""
    logger.info("Fetching transfers data")
    query_params = {"day": get_date_x_days_ago(1)}
    query = db.read_query("queries/transfers_daily.sql", query_params)
    df = db.query_to_df(query)
    return df


def get_mints_data(db: DbConnector) -> pd.DataFrame:
    """Runs query to extract daily mints data"""
    logger.info("Fetching mints data")
    query_params = {"day": get_date_x_days_ago(1)}
    query = db.read_query("queries/mints_daily.sql", query_params)
    df = db.query_to_df(query)
    return df


def get_active_wallets_date(db: DbConnector) -> pd.DataFrame:
    """Runs query to extract daily active wallets and new active wallets"""
    logger.info("Fetching active wallets data")
    query_params = {"day": get_date_x_days_ago(1)}
    query = db.read_query("queries/active_wallets_daily.sql", query_params)
    df = db.query_to_df(query)
    return df


def get_collectibles_metadata() -> dict:
    """API Call to get upadeted metadata for Collectibles"""
    logger.info("Collectibles API Call")
    api_call_url = os.environ.get("API_CALL_URL_SETS")
    res = requests.get(api_call_url).json()
    return res


def get_comics_metadata() -> dict:
    """API Call to get upadeted metadata for Comics"""
    logger.info("Comics API Call")
    api_call_url = os.environ.get("API_CALL_URL_COMICS")
    res = requests.get(api_call_url).json()
    return res


# fmt: off
def create_drop_metadata_file(collectibles_data: dict, comics_data: dict) -> pd.DataFrame:
    """Transforms and merges Collectibles and Comics Metadata"""
    logger.info("Creating Drop Metadata File")
    collectibles_df = transform_collectibles_data(collectibles_data)
    comics_df = transform_comics_data(comics_data)
    df = (
        pd.concat([collectibles_df, comics_df])
        .sort_values(by="dropDate")
        .assign(
            season=lambda df: df["season"].fillna(method="ffill").astype(int),
            dropDate=lambda df: pd.to_datetime(df["dropDate"], utc=True).dt.strftime(
                "%Y-%m-%d"
            ),
        )
    )
    return df


def get_top_wallets(db: DbConnector, limit: int) -> pd.DataFrame:
    """Gets top 100 wallets by collection size"""
    logger.info("Fetching Top Wallets")
    wallets_to_exclude = load_config("special_wallets").exclusions
    query_params = {"limit": limit, "wallets_to_exclude": str(wallets_to_exclude)[1:-1]}
    query = db.read_query("queries/top_x_wallets.sql", query_params)
    df = db.query_to_df(query)
    return df


# fmt: off
def get_wallet_activity(db: DbConnector, wallets: pd.DataFrame, days: int) -> pd.DataFrame:
    """Gets daily wallets activity of the top 100 wallets for the past 60 days"""
    logger.info("Fetching Wallet Activity")
    query_params = {
        "start_date": get_date_x_days_ago(days),
        "end_date": get_date_x_days_ago(1),
        "list": str(wallets["wallet"].to_list())[1:-1],
    }
    query = db.read_query("queries/wallet_activity.sql", query_params)
    df = db.query_to_df(query)
    return df


def create_leaderboard(wallets: pd.DataFrame, wallet_activity: pd.DataFrame) -> pd.DataFrame:
    """Creates dataframe for the top 100 dashboard section"""
    logger.info("Creating Leaderboard File")
    df = (
        # Merge top 100 wallets and wallet activity data (with an outer join, to include the wallets that had no activity)
        pd.merge(wallets, wallet_activity, on=["wallet"], how="outer")
        # Fill missing dates of non-active wallets with last date of the period. Calculate the netTradeBalance.
        .assign(
            date=lambda df: pd.to_datetime(df["date"]).fillna(get_date_x_days_ago(1)),
            netTradeBalance=lambda df: df.purchases + df.mints - df.sales,
        )
        # Create an entry for all non-active dates of the period for all wallets
        .set_index(["date", "wallet"])
        .unstack(fill_value=0)
        .asfreq("D", fill_value=0)
        .stack()
        .sort_index(level=1)
        .reset_index()
        # Fill missing values for token_count with the maximum value
        .assign(
            token_count=lambda df: df.groupby("wallet")["token_count"].transform("max"),
            date=lambda df: df["date"].dt.strftime("%Y-%m-%d")
        )
        # Fill missing values for sales, purchases, mints and netTradeBalance with 0
        .assign(**{
            col: lambda df: df[col].fillna(0).astype(int)
            for col in ["sales", "purchases", "mints", "netTradeBalance"]
        })
    )
    return df
# fmt: on


def append_gsheet_data(file: str, tab: str, df: pd.DataFrame) -> None:
    """Appends a new row of data to a GSheet file"""
    logger.info(f"Appending {tab} to {file} GSheet")
    sa = gspread.service_account("gsheet-creds.json")
    file = sa.open(file)
    tab = file.worksheet(tab)
    tab.append_row(df.values.tolist()[0])


def update_gsheet_data(file: str, tab: str, df: pd.DataFrame) -> None:
    """Updates a GSheet file"""
    logger.info(f"Updating {file} GSheet")
    sa = gspread.service_account("gsheet-creds.json")
    file = sa.open(file)
    tab = file.worksheet(tab)
    tab.update([df.columns.values.tolist()] + df.values.tolist())


def run_pipeline():
    # Initiate DB Connection
    db = init_db_connection()

    # Get Daily Transfers, Mints and Active Wallets Data
    df_transfers = get_transfers_data(db)
    df_mints = get_mints_data(db)
    df_active_wallets = get_active_wallets_date(db)

    # Get Drop Metadata
    collectibles_data = get_collectibles_metadata()
    comics_data = get_comics_metadata()
    df_drops = create_drop_metadata_file(collectibles_data, comics_data)

    # Get Leaderboard (aka Top 100) Data
    wallets = get_top_wallets(db, 100)
    wallet_activity = get_wallet_activity(db, wallets, 61)
    df_leaderboard = create_leaderboard(wallets, wallet_activity)

    # Write data to GSheets
    append_gsheet_data("Veve Tracker Daily", "Transfers", df_transfers)
    append_gsheet_data("Veve Tracker Daily", "Mints", df_mints)
    append_gsheet_data("Active Wallets", "Daily", df_active_wallets)
    update_gsheet_data("Veve Drops", "All", df_drops)
    update_gsheet_data("Leaderboard", "Daily", df_leaderboard)


if __name__ == "__main__":
    run_pipeline()
