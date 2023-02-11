from datetime import date, timedelta

from loguru import logger
from omegaconf import OmegaConf


def load_config(filename: str):
    """Loads a config file from the config folder"""
    return OmegaConf.load(f"config/{filename}.yml")


def get_date_x_days_ago(x):
    """Returns date x days ago"""
    return (date.today() - timedelta(days=x)).strftime("%Y-%m-%d")
