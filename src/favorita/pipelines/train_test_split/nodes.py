from typing import Tuple

from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def train_val_test_split(
    featured_weekly_sales_filled: DataFrame, calendar_processed: DataFrame
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    time_delta = (
        calendar_processed.sort(F.asc("date")).limit(2).toPandas()["time_percentage"][1]
    )

    train = featured_weekly_sales_filled.filter(
        F.col("time_percentage").between(0, 0.8)
    )
    val = featured_weekly_sales_filled.filter(
        F.col("time_percentage").between(0.8 + time_delta, 0.97)
    )
    test = featured_weekly_sales_filled.filter(
        F.col("time_percentage").between(0.97 + time_delta, 1)
    )

    return train, val, test
