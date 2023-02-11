import numpy as np
import pandas as pd


def transform_collectibles_data(metadata: dict) -> pd.DataFrame:
    df = (
        pd.json_normalize(
            metadata, "collectibles", ["brand", "series", "dropDate", "season"]
        )[["dropDate", "brand", "series", "season", "totalIssued"]]
        .rename(columns={"totalIssued": "editions"})
        .assign(type="Collectible")
        .groupby(["dropDate", "brand", "series", "type"])
        .agg({"season": "max", "editions": "sum"})
        .reset_index()
    )
    return df


def transform_comics_data(metadata: dict) -> pd.DataFrame:
    df = (
        pd.json_normalize(metadata)[
            ["dropDate", "publisher", "series", "issue", "editions"]
        ]
        .rename(columns={"publisher": "brand"})
        .assign(
            series=lambda df: df.series.str.cat(df.issue.map(str), sep=" #"),
            type="Comic",
            season=np.nan,
        )
        .drop(["issue"], axis=1)
    )
    return df
