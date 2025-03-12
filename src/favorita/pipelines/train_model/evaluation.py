import pyspark.sql.functions as F
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import DataFrame


def calculate_rmsle(df: DataFrame, label_col: str, pred_col: str) -> DataFrame:
    return df.agg(
        F.sqrt(F.mean(F.pow(F.log1p(label_col) - F.log1p(pred_col), 2)))
    ).collect()[0][0]


def score_model(
    df: DataFrame, label_col: str, pred_col: str, baseline_pred_col: str, context: str
) -> dict[str, float]:
    evaluator = RegressionEvaluator(labelCol=label_col, predictionCol=pred_col)
    baseline_evaluator = RegressionEvaluator(
        labelCol=label_col, predictionCol=baseline_pred_col
    )

    # Performance Metrics
    mae = evaluator.evaluate(df, {evaluator.metricName: "mae"})
    rmse = evaluator.evaluate(df, {evaluator.metricName: "rmse"})
    rmsle = calculate_rmsle(df=df, label_col=label_col, pred_col=pred_col)

    # Scaled Performance Metrics
    mae_baseline = baseline_evaluator.evaluate(
        df, {baseline_evaluator.metricName: "mae"}
    )
    rmse_baseline = baseline_evaluator.evaluate(
        df, {baseline_evaluator.metricName, "rmse"}
    )
    rmsle_baseline = calculate_rmsle(
        df=df, label_col=label_col, pred_col=baseline_pred_col
    )

    metrics = {
        "rmsle": rmsle,
        "rmse": rmse,
        "mae": mae,
        "scaled_rmsle": 1 - rmsle / rmsle_baseline,
        "scaled_rmse": 1 - rmse / rmse_baseline,
        "scaled_mae": 1 - mae / mae_baseline,
    }
    context_model_metrics = {f"{context}_{k}": v for k, v in metrics.items()}

    return context_model_metrics


# Dataset Stats
def summarize_df(
    df: DataFrame, label_col: str, pred_col: str, context: str
) -> dict[str, float]:
    dataset_metrics = (
        df.agg(
            F.count("*").alias("num_rows"),
            F.mean(label_col).alias(f"mean_{label_col}"),
            F.mean(pred_col).alias(f"mean_{pred_col}"),
            F.min(label_col).alias(f"min_{label_col}"),
            F.min(pred_col).alias(f"min_{pred_col}"),
            F.max(label_col).alias(f"max_{label_col}"),
            F.max(pred_col).alias(f"max_{pred_col}"),
            F.std(label_col).alias(f"std_{label_col}"),
            F.std(pred_col).alias(f"std_{pred_col}"),
        )
        .toPandas()
        .T[0]
        .to_dict()
    )
    context_data_metrics = {f"{context}_{k}": v for k, v in dataset_metrics.items()}
    return context_data_metrics
