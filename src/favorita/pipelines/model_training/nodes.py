from functools import partial

import mlflow
import numpy as np
import optuna
import pyspark.sql.functions as F
from optuna import Trial
from pyspark.sql import DataFrame
from pyspark.storagelevel import StorageLevel

from .evaluation import calculate_rmsle, get_model_report
from .model import build_forecast_model, get_model_input_columns, xgb_hyperparam_suggest
from .summary_plots import get_2d_hist_plot, get_residuals_plot


def get_or_create_experiment(experiment_name: str) -> str:
    if experiment := mlflow.get_experiment_by_name(experiment_name):
        return experiment.experiment_id
    else:
        return mlflow.create_experiment(experiment_name)


def get_available_fold_ids(cross_val_datasets: DataFrame) -> np.ndarray:
    available_fold_ids = (
        cross_val_datasets.groupby("fold_id")
        .agg(F.first("date"))
        .select("fold_id")
        .toPandas()["fold_id"]
        .values
    )

    return available_fold_ids


def objective(
    trial: Trial,
    experiment_id: str,
    parent_run_id: str,
    datasets: DataFrame,
    fold_ids: np.ndarray,
) -> float:
    with mlflow.start_run(
        experiment_id=experiment_id, parent_run_id=parent_run_id, nested=True
    ):
        model_param_suggestions = xgb_hyperparam_suggest(trial)
        mlflow.log_params(model_param_suggestions)

        scores = []
        numerical_columns = get_model_input_columns(datasets)

        for fold_id in fold_ids:
            fold_dataset = datasets.filter(F.col("fold_id") == fold_id)
            fold_val = fold_dataset.filter(F.col("is_validation"))

            model = build_forecast_model(model_param_suggestions, numerical_columns)
            forecast_model = model.fit(
                fold_dataset.sampleBy(
                    "is_validation", fractions={True: 1.0, False: 0.1}, seed=42
                )
            )

            val_preds = forecast_model.transform(fold_val).select(
                "unit_sale", "prediction", "lag_store_item_sale_1"
            )
            val_rmsle = calculate_rmsle(val_preds)
            scores.append(val_rmsle)
            mlflow.log_metric("val_rmsle", val_rmsle, step=fold_id)

        mean_rmsle = np.mean(scores)
        mlflow.log_metric("cv_rmsle", mean_rmsle)
        return mean_rmsle


def optimize_model(
    cross_val_datasets: DataFrame, experiment_name: str, n_trials: int
) -> None:
    fold_ids = get_available_fold_ids(cross_val_datasets)

    experiment_id = get_or_create_experiment(experiment_name)
    mlflow.set_experiment(experiment_id)

    with mlflow.start_run(experiment_id=experiment_id, nested=True) as parent_run:
        # Results of the HPO
        study = optuna.create_study(direction="minimize")
        study.optimize(
            partial(
                objective,
                experiment_id=experiment_id,
                parent_run_id=parent_run.info.run_id,
                datasets=cross_val_datasets,
                fold_ids=fold_ids,
            ),
            n_trials=n_trials,
        )
        mlflow.log_metric("best_cv_rmsle", study.best_value)

        # Train a final model
        numerical_columns = get_model_input_columns(cross_val_datasets)
        latest_train_val_ds = cross_val_datasets.filter(
            F.col("fold_id") == len(fold_ids) - 1
        )
        train = latest_train_val_ds.filter(~F.col("is_validation"))
        val = latest_train_val_ds.filter(F.col("is_validation"))

        model = build_forecast_model(study.best_params, numerical_columns)
        forecast_model = model.fit(
            latest_train_val_ds.sampleBy(
                "is_validation", fractions={True: 1.0, False: 0.5}, seed=42
            )
        )

        # Model Reports
        train_preds = (
            forecast_model.transform(train)
            .select("unit_sale", "prediction", "lag_store_item_sale_1")
            .persist(StorageLevel.DISK_ONLY)
        )
        val_preds = (
            forecast_model.transform(val)
            .select("unit_sale", "prediction", "lag_store_item_sale_1")
            .persist(StorageLevel.DISK_ONLY)
        )

        train_report = get_model_report(train_preds)
        val_report = get_model_report(val_preds, context="val")

        train_residuals_plot = get_residuals_plot(train_preds)
        val_residuals_plot = get_residuals_plot(val_preds)
        train_pred_v_label_plot = get_2d_hist_plot(train_preds)
        val_pred_v_label_plot = get_2d_hist_plot(val_preds)

        # Cleanup
        val_preds.unpersist()
        train_preds.unpersist()

        # Model Logging
        mlflow.spark.log_model(
            forecast_model,
            "model",
            input_example=train.select(numerical_columns).limit(5).toPandas(),
        )
        mlflow.log_params(study.best_params)
        mlflow.log_metrics(train_report | val_report)
        mlflow.log_figure(train_residuals_plot, "figures/train_residuals.png")
        mlflow.log_figure(val_residuals_plot, "figures/val_residuals.png")
        mlflow.log_figure(train_pred_v_label_plot, "figures/train_pred_v_label.png")
        mlflow.log_figure(val_pred_v_label_plot, "figures/val_pred_v_label.png")
