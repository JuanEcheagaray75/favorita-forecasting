from collections.abc import Callable
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import WindowSpec, Window




def add_product_rank(
    df: DataFrame, rolled_sales_col: str, output_col: str = "product_rank"
) -> DataFrame:
    return df.withColumn(
        output_col,
        F.percent_rank().over(
            Window.partitionBy("store_id", "date").orderBy(F.asc(rolled_sales_col))
        ),
    )


def add_lags(
    df: DataFrame, lag_col: str, window: WindowSpec, lags: list[int]
) -> DataFrame:
    lag_cols = {
        f"lag_{lag_col}_{i}": F.lag(col=lag_col, offset=i).over(window) for i in lags
    }

    return df.withColumns(lag_cols)


def add_agg_over_windows(
    df: DataFrame,
    val_col: str,
    agg_spark_fn: Callable,
    prefix: str,
    window: WindowSpec,
    periods: list[int],
) -> DataFrame:
    mean_cols = {
        f"{prefix}_{i}": agg_spark_fn(val_col).over(window.rowsBetween(-i, -1))
        for i in periods
    }

    return df.withColumns(mean_cols)


def mean_real_purchase(col_name):
    return F.mean(F.when(F.col(col_name) > 0, F.col(col_name)))


def add_smooth_rbf(
    df: DataFrame,
    peak_at: int,
    alpha: float,
    date_col: str = "date",
    period_length: int = 52,
    start_date: str = "2013-01-01",
) -> DataFrame:
    t = F.ceil(F.date_diff(F.col(date_col), F.to_date(F.lit(start_date))) / 7)
    smooth_rbf = F.exp(
        -(F.lit(period_length) ** 2)
        / F.lit(alpha)
        * F.sin(F.pi() * (t - F.lit(peak_at)) / F.lit(period_length)) ** 2
    )

    return df.withColumn(f"smooth_rbf_{peak_at}_{int(alpha)}", smooth_rbf)


def add_one_side_rbf(
    df: DataFrame, peak_at: int, alpha: float, date_col: str = "date"
) -> DataFrame:
    shrinkage = 52 / (peak_at * alpha**2)
    peak_rbf = F.exp(-shrinkage * (F.lit(peak_at) - F.weekofyear(date_col)) ** 2)

    return df.withColumn(f"one_side_rbf_{peak_at}_{int(alpha)}", peak_rbf)

