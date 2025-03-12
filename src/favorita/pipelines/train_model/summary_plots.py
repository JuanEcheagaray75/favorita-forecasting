import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.figure import Figure
from pyspark.sql import DataFrame
from xgboost.spark import SparkXGBRegressorModel


def xgboost_feature_importance_plot(
    xgb_model: SparkXGBRegressorModel,
    input_cols: list[str],
    model_name: str,
    importance_type: str = "gain",
) -> Figure:
    ori_feature_names_idx = pd.Series(input_cols).reset_index()
    ori_feature_names_idx.columns = ["feature_idx", "feature"]

    raw_feature_importance = pd.DataFrame(
        xgb_model.get_feature_importances(
            importance_type,
        ).items()
    )
    raw_feature_importance.columns = ["feature_idx", importance_type]
    raw_feature_importance["feature_idx"] = (
        raw_feature_importance["feature_idx"].str.replace("f", "").astype(int)
    )

    feature_importance = (
        pd.merge(
            left=ori_feature_names_idx,
            right=raw_feature_importance,
            on="feature_idx",
            how="left",
        )
        .drop(columns="feature_idx")
        .fillna(0)
    )

    fig, ax = plt.subplots(figsize=(10, 10))
    ax.barh(
        feature_importance["feature"],
        feature_importance[importance_type],
    )
    ax.set_xlabel(importance_type)
    ax.set_ylabel("feature")
    fig.suptitle(f"Model: {model_name}")
    ax.set_title(f"Feature Importance: {importance_type}")
    plt.tight_layout()
    plt.close(fig)

    return fig


def residual_plot(
    df: DataFrame, label_col: str, residual_col: str = "residual", sample: float = 0.1
) -> Figure:
    sampled_residuals = (
        df.select(label_col, residual_col).sample(sample, seed=42).toPandas()
    )
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.scatter(sampled_residuals[label_col], sampled_residuals[residual_col])
    ax.axhline(y=0, color="r", linestyle="-")
    ax.set_xlabel(label_col)
    ax.set_ylabel("Residual")
    ax.set_title("Residuals vs True Values")
    ax.grid(axis="y")
    plt.tight_layout()
    plt.close(fig)

    return fig


def residual_distribution_plot(
    df: DataFrame, residual_col: str = "residual", sample: float = 0.01
) -> Figure:
    sampled_df = df.select(residual_col).sample(sample).toPandas()
    fig, ax = plt.subplots(figsize=(8, 5))
    _ = plt.hist(sampled_df[residual_col], bins=30)
    ax.set_xlabel("Residual")
    ax.set_ylabel("Count")
    ax.set_title("Residual Plot")
    plt.tight_layout()
    plt.close(fig)

    return fig
