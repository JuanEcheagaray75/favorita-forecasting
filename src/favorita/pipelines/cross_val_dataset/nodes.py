from functools import reduce

import numpy as np
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from sklearn.model_selection import TimeSeriesSplit


def split_dataset(
    featured_train: DataFrame,
    calendar: DataFrame,
    earliest_train_date: str,
    num_cv_splits: int,
) -> DataFrame:
    valid_calendar = (
        calendar.filter(F.col("date") >= earliest_train_date)
        .select("time_percentage")
        .toPandas()
        .values
    )

    min_train_cutoff = np.min(valid_calendar)

    tscv = TimeSeriesSplit(n_splits=num_cv_splits)
    fold_datasets = []

    for fold, (train_idx, val_idx) in enumerate(tscv.split(valid_calendar)):
        # Get the max train idx and val idx, they define the fold's boundaries
        fold_train_cutoff = np.max(valid_calendar[train_idx])
        fold_val_cutoff = np.max(valid_calendar[val_idx])

        # Creating a single dataset with helper column to differentiate between the
        # 2 datasets. Will enable to use Early Stopping during training
        fold_dataset = featured_train.filter(
            F.col("time_percentage").between(min_train_cutoff, fold_val_cutoff)
        )
        fold_train_val = fold_dataset.withColumn(
            "is_validation",
            F.when(F.col("time_percentage") < fold_train_cutoff, False).otherwise(True),
        ).withColumn("fold_id", F.lit(fold))

        fold_datasets.append(fold_train_val)

    return reduce(lambda x, y: x.unionByName(y), fold_datasets)
