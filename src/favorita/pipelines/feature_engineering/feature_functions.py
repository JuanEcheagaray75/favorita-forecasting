from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame, Window, WindowSpec


@dataclass
class SparkAggFn:
    prefix: str
    spark_function: Callable[[str, ...], Column]
    params: Dict[str, Any] = field(default_factory=dict)


def define_grouping_alias(grouping_cols: List[str]) -> str:
    return f"{"_".join(grouping_cols)}_sale".replace("_id", "")


def add_lags(
    df: DataFrame, lag_col: str, wspec: WindowSpec, lags: List[int] | None
) -> DataFrame:
    lag_cols = {
        f"lag_{lag_col}_{i}": F.lag(col=lag_col, offset=i).over(wspec).cast("float")
        for i in lags
    }

    return df.withColumns(lag_cols)


def add_agg_over_windows(
    df: DataFrame,
    val_col: str,
    spark_agg_fn: SparkAggFn,
    wspec: WindowSpec,
    periods: list[int],
) -> DataFrame:
    agg_cols = {
        f"{spark_agg_fn.prefix}_{val_col}_{i}": spark_agg_fn.spark_function(
            val_col, **spark_agg_fn.params
        )
        .over(wspec.rowsBetween(-i, -1))
        .cast("float")
        for i in periods
    }

    return df.withColumns(agg_cols)


def add_product_rank(
    df: DataFrame, rolled_sales_col: str, output_col: str = "product_rank"
) -> DataFrame:
    return df.withColumn(
        output_col,
        F.percent_rank().over(
            Window.partitionBy("store_id", "date").orderBy(F.asc(rolled_sales_col))
        ),
    )


def resample_per_group(
    df: DataFrame,
    grouping_cols: List[str],
    grouping_alias: str,
    date_col: str = "date",
    label_col: str = "unit_sale",
) -> DataFrame:
    if not grouping_alias:
        grouping_alias = define_grouping_alias(grouping_cols)

    base_groupíng_alias = define_grouping_alias(grouping_cols)
    return df.groupby(*grouping_cols, date_col).agg(
        F.sum(label_col).alias(base_groupíng_alias),
        F.count_if(F.col(label_col) == 0).alias(f"num_zero_{base_groupíng_alias}"),
    )


def add_features_per_grouping(
    df: DataFrame,
    grouping_cols: List[str],
    lags: List[int] | None,
    group_periods: List[int],
    agg_spark_fns: List[SparkAggFn],
) -> DataFrame:
    grouping_alias = define_grouping_alias(grouping_cols)
    group_wspec = Window.partitionBy(*grouping_cols).orderBy("date")

    if lags is None:
        lags = []

    group_week_sales = df.transform(
        resample_per_group,
        grouping_cols=grouping_cols,
        grouping_alias=grouping_alias,
    ).transform(
        add_lags,
        lag_col=grouping_alias,
        wspec=group_wspec,
        lags=lags,
    )

    for agg_fn in agg_spark_fns:
        group_week_sales = group_week_sales.transform(
            add_agg_over_windows,
            val_col=grouping_alias,
            spark_agg_fn=agg_fn,
            wspec=group_wspec,
            periods=group_periods,
        ).transform(
            add_agg_over_windows,
            val_col=f"num_zero_{grouping_alias}",
            spark_agg_fn=agg_fn,
            wspec=group_wspec,
            periods=group_periods,
        )

    return df.join(
        other=group_week_sales, on=grouping_cols + ["date"], how="left"
    ).drop(grouping_alias, f"num_zero_{grouping_alias}")


def divide_col_over(df: DataFrame, val_col: str, divisor_cols: List[str]) -> DataFrame:
    divided_cols = {
        f"{val_col}_div_{agg_col}": F.try_divide(val_col, agg_col).cast("float")
        for agg_col in divisor_cols
    }
    return df.withColumns(divided_cols)


# def add_smooth_rbf(
#     df: DataFrame,
#     peak_at: int,
#     alpha: float,
#     date_col: str = "date",
#     period_length: int = 52,
#     start_date: str = "2013-01-01",
# ) -> DataFrame:
#     t = F.ceil(F.date_diff(F.col(date_col), F.to_date(F.lit(start_date))) / 7)
#     smooth_rbf = F.exp(
#         -(F.lit(period_length) ** 2)
#         / F.lit(alpha)
#         * F.sin(F.pi() * (t - F.lit(peak_at)) / F.lit(period_length)) ** 2
#     )

#     return df.withColumn(f"smooth_rbf_{peak_at}_{int(alpha)}", smooth_rbf)


# def add_one_side_rbf(
#     df: DataFrame, peak_at: int, alpha: float, date_col: str = "date"
# ) -> DataFrame:
#     shrinkage = 52 / (peak_at * alpha**2)
#     peak_rbf = F.exp(-shrinkage * (F.lit(peak_at) - F.weekofyear(date_col)) ** 2)

#     return df.withColumn(f"one_side_rbf_{peak_at}_{int(alpha)}", peak_rbf)


# def add_temporal_features(df: DataFrame) -> DataFrame:
#     return (
#         df
#         # Periodic Exponential Peaks
#         .transform(add_one_side_rbf, peak_at=52, alpha=10)
#         .transform(add_one_side_rbf, peak_at=18, alpha=10)
#         # Smooth week effects
#         .transform(add_smooth_rbf, peak_at=52, alpha=15)
#         .transform(add_smooth_rbf, peak_at=18, alpha=15)
#         # Smooth month effects
#         .transform(add_smooth_rbf, peak_at=50, alpha=75)
#         .transform(add_smooth_rbf, peak_at=36, alpha=75)
#     )
