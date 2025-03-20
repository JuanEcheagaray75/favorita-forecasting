from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window as W


def create_processed_calendar(calendar: DataFrame) -> DataFrame:
    SATURDAY_ID = 7
    calendar_processed = (
        calendar.withColumn("year", F.year("date"))
        .withColumn("week", F.weekofyear("date"))
        .filter(F.dayofweek("date") == SATURDAY_ID)
        .withColumn("time_percentage", F.percent_rank().over(W.orderBy("date")))
    )
    return calendar_processed


def create_weekly_sales(train_processed: DataFrame) -> DataFrame:
    weekly_sales = (
        train_processed.withColumn("year", F.year("date"))
        .withColumn("week", F.weekofyear("date"))
        .groupby("year", "week", "store_id", "item_id")
        .agg(F.sum("unit_sales").alias("unit_sale"))
    )
    return weekly_sales


def create_weekly_sales_filled(
    weekly_sales: DataFrame,
    calendar_processed: DataFrame,
    stores_processed: DataFrame,
    items_processed: DataFrame,
) -> DataFrame:
    store_item_window = W.partitionBy("store_id", "item_id")
    city_item_window = W.partitionBy("date", "city", "item_id")
    real_purchase = F.when(F.col("unit_sale").isNotNull(), F.col("date"))

    is_product_available = F.when(
        F.first("unit_sale", ignorenulls=True).over(city_item_window).isNotNull(), True
    ).otherwise(False)

    all_sales = (
        calendar_processed.crossJoin(items_processed)
        .crossJoin(stores_processed)
        .join(
            other=weekly_sales, on=["year", "week", "store_id", "item_id"], how="left"
        )
        .withColumn(
            "first_product_store_purchase", F.min(real_purchase).over(store_item_window)
        )
        .withColumn("is_product_available", is_product_available)
        .filter(F.col("date") >= F.col("first_product_store_purchase"))
        .fillna(value=0, subset=["unit_sale"])
        .withColumn("unit_sale", F.greatest(F.col("unit_sale"), F.lit(0.0)))
        .drop("year", "week")
    )

    return all_sales
