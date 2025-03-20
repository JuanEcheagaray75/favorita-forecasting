import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from .feature_functions import (
    SparkAggFn,
    add_features_per_grouping,
    define_grouping_alias,
    divide_col_over,
)


def featurize_df(weekly_sales_filled: DataFrame) -> DataFrame:
    default_agg_fns = [
        SparkAggFn("mean", F.mean),
        SparkAggFn("std", F.std),
        SparkAggFn("min", F.min),
        SparkAggFn("max", F.max),
        SparkAggFn("median", F.percentile, {"percentage": 0.5}),
    ]

    featured_df = (
        weekly_sales_filled.transform(
            add_features_per_grouping,
            grouping_cols=["store_id", "item_id"],
            group_periods=[4, 8, 12, 24],
            agg_spark_fns=default_agg_fns,
            lags=list(range(1, 5)),
        )
        .transform(
            add_features_per_grouping,
            grouping_cols=["store_id", "class"],
            group_periods=[4, 8, 12, 24],
            agg_spark_fns=default_agg_fns,
            lags=None,
        )
        .transform(
            add_features_per_grouping,
            grouping_cols=["store_id"],
            group_periods=[4, 8, 12, 24],
            agg_spark_fns=default_agg_fns,
            lags=None,
        )
        .transform(
            divide_col_over,
            val_col="lag_store_item_sale_1",
            divisor_cols=[
                f"mean_{define_grouping_alias(["store_id", "item_id"])}_4",
                f"median_{define_grouping_alias(["store_id", "item_id"])}_4",
                f"max_{define_grouping_alias(["store_id", "item_id"])}_4",
                f"mean_{define_grouping_alias(["store_id"])}_4",
                f"median_{define_grouping_alias(["store_id"])}_4",
                f"max_{define_grouping_alias(["store_id"])}_4",
                f"mean_{define_grouping_alias(["store_id", "class"])}_4",
                f"median_{define_grouping_alias(["store_id", "class"])}_4",
                f"max_{define_grouping_alias(["store_id", "class"])}_4",
            ],
        )
        .transform(
            divide_col_over,
            val_col=f"mean_{define_grouping_alias(["store_id", "item_id"])}_4",
            divisor_cols=[
                f"std_{define_grouping_alias(["store_id", "item_id"])}_4",
                f"std_{define_grouping_alias(["store_id", "item_id"])}_8",
                f"std_{define_grouping_alias(["store_id", "item_id"])}_12",
            ],
        )
        .withColumn(
            "week_sale_diff", F.col("unit_sale") - F.col("lag_store_item_sale_1")
        )
    )
    return featured_df
