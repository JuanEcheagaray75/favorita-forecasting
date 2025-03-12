from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window as W


from .feature_functions import (
    add_lags,
    add_agg_over_windows,
    mean_real_purchase,
    add_one_side_rbf,
    add_smooth_rbf,
)


def featurize_df(weekly_sales_filled: DataFrame) -> DataFrame:
    MAX_SALE_LAG = 6
    lags = list(range(1, MAX_SALE_LAG + 1))
    lag_cols = [f"lag_unit_sale_{i}" for i in lags]

    store_item_window = W.partitionBy("store_id", "item_id").orderBy("date")
    cluster_window = W.partitionBy("cluster").orderBy("date")
    class_window = W.partitionBy("class").orderBy("date")
    family_window = W.partitionBy("family").orderBy("date")

    # Window sizes
    period_sizes = [4, 8, 12]

    df = (
        weekly_sales_filled.transform(
            add_lags, lag_col="unit_sale", window=store_item_window, lags=lags
        )
        .withColumn(
            "sales_diff_week", F.col("lag_unit_sale_1") - F.col("lag_unit_sale_2")
        )
        # Periodic Exponential Peaks
        .transform(add_one_side_rbf, peak_at=52, alpha=10)
        .transform(add_one_side_rbf, peak_at=18, alpha=10)
        .transform(add_one_side_rbf, peak_at=40, alpha=10)
        # Smooth week effects
        .transform(add_smooth_rbf, peak_at=52, alpha=15)
        .transform(add_smooth_rbf, peak_at=40, alpha=15)
        .transform(add_smooth_rbf, peak_at=18, alpha=15)
        # Smooth month effects
        .transform(add_smooth_rbf, peak_at=50, alpha=75)
        .transform(add_smooth_rbf, peak_at=36, alpha=75)
        .transform(add_smooth_rbf, peak_at=10, alpha=75)
        .transform(
            add_agg_over_windows,
            val_col="sales_diff_week",
            prefix="sales_diff_mean",
            agg_spark_fn=F.mean,
            window=store_item_window,
            periods=period_sizes,
        )
        .transform(
            add_agg_over_windows,
            val_col="sales_diff_week",
            prefix="sales_diff_std",
            agg_spark_fn=F.std,
            window=store_item_window,
            periods=period_sizes,
        )
        .transform(
            add_agg_over_windows,
            val_col="unit_sale",
            prefix="store_item_real_mean",
            agg_spark_fn=mean_real_purchase,
            window=store_item_window,
            periods=period_sizes,
        )
        .transform(
            add_agg_over_windows,
            val_col="unit_sale",
            prefix="store_item_max",
            agg_spark_fn=F.max,
            window=store_item_window,
            periods=[4],
        )
        .transform(
            add_agg_over_windows,
            val_col="unit_sale",
            prefix="store_item_std",
            agg_spark_fn=F.stddev_samp,
            window=store_item_window,
            periods=period_sizes,
        )
        .transform(
            add_agg_over_windows,
            val_col="unit_sale",
            prefix="family_mean",
            agg_spark_fn=F.mean,
            window=family_window,
            periods=period_sizes,
        )
        .transform(
            add_agg_over_windows,
            val_col="unit_sale",
            prefix="class_mean",
            agg_spark_fn=F.mean,
            window=class_window,
            periods=period_sizes,
        )
        .transform(
            add_agg_over_windows,
            val_col="unit_sale",
            prefix="class_real_mean",
            agg_spark_fn=mean_real_purchase,
            window=class_window,
            periods=period_sizes,
        )
        .transform(
            add_agg_over_windows,
            val_col="unit_sale",
            prefix="class_std",
            agg_spark_fn=F.stddev_samp,
            window=class_window,
            periods=[4, 12],
        )
        .transform(
            add_agg_over_windows,
            val_col="unit_sale",
            prefix="cluster_mean",
            agg_spark_fn=F.mean,
            window=cluster_window,
            periods=period_sizes,
        )
        .dropna(subset=lag_cols)
    )

    return df
