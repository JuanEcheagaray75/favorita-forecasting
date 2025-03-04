import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession

from .utils import generate_dates

spark = SparkSession.getActiveSession()


def clean_items(items: DataFrame) -> DataFrame:
    perishable_cast = F.col("perishable").cast(T.ShortType()).cast(T.BooleanType())
    item_cast = F.col("item_nbr").cast(T.IntegerType())
    class_cast = F.col("class").cast(T.IntegerType())
    item_weight = F.when(F.col("perishable"), F.lit(1.25)).otherwise(F.lit(1.0))

    items = (
        items.withColumns(
            {
                "perishable": perishable_cast,
                "item_id": item_cast,
                "class": class_cast,
            }
        )
        .withColumn("item_weight", item_weight)
        .drop("item_nbr")
    )

    return items


def clean_stores(stores: DataFrame) -> DataFrame:
    stores = stores.withColumns(
        {
            "store_id": F.col("store_nbr").cast(T.IntegerType()),
            "cluster": F.col("cluster").cast(T.IntegerType()),
        }
    ).drop("store_nbr")

    return stores


def clean_train(train: DataFrame) -> DataFrame:
    on_promotion_check = (
        F.when(F.col("onpromotion") == "True", F.lit(True))
        .when(F.col("onpromotion") == "False", F.lit(False))
        .otherwise(F.lit(None))
    )
    train = train.withColumns(
        {
            "date": F.to_date("date"),
            "store_id": F.col("store_nbr").cast(T.IntegerType()),
            "item_id": F.col("item_nbr").cast(T.IntegerType()),
            "unit_sales": F.col("unit_sales").cast(T.DoubleType()),
            "on_promotion": on_promotion_check,
        }
    ).drop("store_nbr", "item_nbr", "onpromotion")
    return train


def create_calendar(train_processed: DataFrame) -> DataFrame:
    earliest, latest = (
        train_processed.select(F.min("date"), F.max("date")).toPandas().values[0]
    )

    all_dates = generate_dates(earliest, latest)

    calendar = spark.createDataFrame(data=all_dates, schema=["date"])

    return calendar
