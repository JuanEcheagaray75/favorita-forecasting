from typing import Optional

import pyspark.sql.functions as F
from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, Param, Params, TypeConverters
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql import DataFrame


class WindowSummarizer(
    Transformer, HasInputCol, DefaultParamsReadable, DefaultParamsWritable
):
    partition_cols = Param(
        parent=Params._dummy(),
        name="partition_cols",
        doc="list of column to create window partitions",
        typeConverter=TypeConverters.toListString,
    )
    order_by_col = Param(
        parent=Params._dummy(),
        name="order_by_col",
        doc="order by column",
        typeConverter=TypeConverters.toString,
    )
    window_sizes = Param(
        parent=Params._dummy(),
        name="window_sizes",
        doc="list of window sizes to apply rolling stats",
        typeConverter=TypeConverters.toListFloat,
    )
    agg_fns = Param(
        parent=Params._dummy(),
        name="agg_fns",
        doc="list of aggregating functions to be applied over the window sizes",
        typeConverter=TypeConverters.toListString,
    )
    percentiles = Param(
        parent=Params._dummy(),
        name="percentiles",
        doc="list of percentiles to calculate",
        typeConverter=TypeConverters.toListFloat,
    )
    accuracy = Param(
        parent=Params._dummy(),
        name="accuracy",
        doc="relative error in approximate calculation, as 1 / accuracy",
        typeConverter=TypeConverters.toFloat,
    )

    AGGREGATION_MAP = {
        "mean": F.mean,
        "std": F.stddev_samp,
        "skew": F.skewness,
        "kurt": F.kurtosis,
        "min": F.min,
        "max": F.max,
    }

    @keyword_only
    def __init__(
        self,
        inputCol: str,
        partition_cols: list[str],
        window_sizes: list[int],
        agg_fns: list[str],
        percentiles: Optional[list[float]] = None,
        accuracy: Optional[float] = None,
    ):
        super().__init__()
        self._setDefault(
            inputCol=None,
            partition_cols=None,
            window_sizes=None,
            agg_fns=None,
            percentiles=None,
            accuracy=None,
        )
        self.setParams(**self._input_kwargs)

    @keyword_only
    def setParams(
        self,
        inputCol: str,
        partition_cols: list[str],
        window_sizes: list[int],
        agg_fns: list[str],
        percentiles: list[float],
        accuracy: float,
    ):
        return self._set(**self._input_kwargs)

    def set_partition_cols(self, partition_cols: list[str]):
        return self.setParams(partition_cols=partition_cols)

    def set_window_sizes(self, window_sizes: list[int]):
        if any(ws < 0 for ws in window_sizes):
            raise ValueError("Window sizes must be greater than 0")

    def set_agg_fns(self, agg_fns: list[str]):
        if any(fn not in self.AGGREGATION_MAP.keys() for fn in agg_fns):
            raise ValueError(
                f"Agg function passed not in {self.AGGREGATION_MAP.keys()}"
            )
        return self.setParams(agg_fns=agg_fns)

    def set_percentiles(self, percentiles: list[float]):
        if any((p < 0) or (p > 1) for p in percentiles):
            raise ValueError("Percentiles must be on the interval (0, 1)")

        return self.setParams(percentiles=percentiles)

    def set_accuracy(self, accuracy: float):
        if accuracy < 0:
            raise ValueError("Accuracy must be positive")

        return self.setParams(accuracy=accuracy)

    def get_partition_cols(self) -> list[str]:
        return self.getOrDefault("partition_cols")

    def get_window_sizes(self) -> list[int]:
        return self.getOrDefault("window_sizes")

    def get_agg_fns(self) -> list[str]:
        return self.getOrDefault("agg_fns")

    def get_percentiles(self) -> list[float]:
        return self.getOrDefault("percentiles")

    def get_accuracy(self) -> float:
        return self.getOrDefault("accuracy")

    def _transform(self, df: DataFrame) -> DataFrame:
        inputCol = self.getInputCol()
        partition_cols = self.get_partition_cols()
        window_sizes = self.get_window_sizes()
        percentiles = self.get_percentiles()
        accuracy = self.get_accuracy()
