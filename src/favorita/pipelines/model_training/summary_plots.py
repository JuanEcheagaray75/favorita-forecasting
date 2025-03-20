from math import floor
from typing import List

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
from matplotlib.figure import Figure
from pyspark.ml.feature import Bucketizer
from pyspark.sql import DataFrame


def _get_bin_edges(spark_hist_df: DataFrame) -> List[float]:
    edges = (
        spark_hist_df.select(F.explode("hist").alias("hist_rows"))
        .select("hist_rows.x")
        .toPandas()["x"]
        .to_list()
    )
    edges.insert(0, -float("inf"))
    edges.insert(len(edges), float("inf"))
    return edges


def get_residuals_plot(
    df: DataFrame,
    label_col: str = "unit_sale",
    pred_col: str = "prediction",
    nbins: int = 30,
) -> Figure:
    df = df.withColumn("residual", F.col(label_col) - F.col(pred_col))
    probas = [i / 50 for i in range(50)]
    quants = df.approxQuantile("residual", probas, 0.001)
    hist_residuals = np.asarray(
        [
            (row.x, row.y)
            for row in df.agg(
                F.histogram_numeric(col="residual", nBins=F.lit(nbins))
            ).collect()[0][0]
        ]
    )
    bins = hist_residuals[:, 0]
    counts = hist_residuals[:, 1]

    fig = plt.figure(figsize=(7, 5))
    plt.title("Distribution of residuals")
    plt.stairs(counts[:-1] / counts[:-1].sum(), bins, label="Distribution")
    plt.plot(quants, probas, label="ECDF")
    plt.legend()
    plt.xlabel("Residual")
    plt.ylabel("Density")
    plt.tight_layout()
    plt.close(fig)
    return fig


def get_2d_hist_plot(
    df: DataFrame,
    label_col: str = "unit_sale",
    pred_col: str = "prediction",
    nbins: int = 100,
) -> Figure:
    pred_hist = df.agg(F.histogram_numeric(pred_col, F.lit(nbins)).alias("hist"))
    label_hist = df.agg(F.histogram_numeric(label_col, F.lit(nbins)).alias("hist"))

    pred_edges = _get_bin_edges(pred_hist)
    label_edges = _get_bin_edges(label_hist)
    bucketizer = Bucketizer(
        splitsArray=[pred_edges, label_edges],
        inputCols=[pred_col, label_col],
        outputCols=["bucket_pred", "bucket_label"],
    )

    edges_df = pd.DataFrame(
        {"pred_edge": pred_edges[1:-1], "label_edge": label_edges[1:-1]}
    )
    edges_df["bucket_pred"] = np.arange(0, nbins)
    edges_df["bucket_label"] = np.arange(0, nbins)

    heatmap = (
        bucketizer.transform(df)
        # Remove the inf edges
        .filter(F.col("bucket_pred") != len(pred_edges) - 2)
        .filter(F.col("bucket_label") != len(pred_edges) - 2)
        .groupby("bucket_label")
        .pivot("bucket_pred")
        .count()
        .fillna(value=0)
        .sort(F.desc("bucket_label"))
        .drop("bucket_label")
        .toPandas()
        .values
    )

    step = floor(0.05 * nbins)
    fig = plt.figure(figsize=(7, 7))
    plt.imshow(heatmap / heatmap.sum(), cmap="viridis")
    plt.gca().set_aspect("equal", adjustable="box")
    plt.xticks(
        ticks=np.arange(0, nbins, step),
        labels=np.round(edges_df["pred_edge"].values[::step], 2),
        rotation=90,
    )
    plt.yticks(
        ticks=np.arange(0, nbins, step),
        labels=np.round(edges_df["label_edge"].values[::step][::-1], 2),
    )
    plt.colorbar(label="Density")
    plt.xlabel("Prediction")
    plt.ylabel("Label")
    plt.title("Prediction vs Label")
    plt.tight_layout()
    plt.close(fig)

    return fig
