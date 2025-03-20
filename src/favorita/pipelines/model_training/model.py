from typing import Dict, List

from optuna import Trial
from pyspark.ml.feature import SQLTransformer, VectorAssembler
from pyspark.ml.pipeline import Pipeline
from pyspark.sql import DataFrame
from xgboost.spark import SparkXGBRegressor


def get_model_input_columns(df: DataFrame) -> List[str]:
    exclude_cols = [
        "store_id",
        "item_id",
        "item_weight",
        "perishable",
        "date",
        "time_percentage",
        "first_product_store_purchase",
        "is_product_available",
        # Internal data wrangling columns
        "fold_id",
        "is_validation",
    ]
    categorical_columns = ["state", "city", "class", "type", "family", "cluster"]
    sale_target_col = "unit_sale"
    week_sale_diff_col = "week_sale_diff"

    numerical_columns = [
        c
        for c in df.columns
        if (c not in exclude_cols)
        and (c not in categorical_columns)
        and (c != sale_target_col)
        and (c != week_sale_diff_col)
    ]
    return numerical_columns


def build_forecast_model(
    model_hyperparams: Dict[str, float], numerical_columns: List[str]
) -> Pipeline:
    model_pipe = Pipeline(
        stages=[
            VectorAssembler(
                inputCols=numerical_columns,
                outputCol="features",
                handleInvalid="skip",
            ),
            SparkXGBRegressor(
                objective="reg:squarederror",
                features_col="features",
                label_col="week_sale_diff",
                prediction_col="raw_diff_prediction",
                tree_method="hist",
                grow_policy="lossguide",
                validation_indicator_col="is_validation",
                verbose=0,
                verbose_eval=False,
                **model_hyperparams,
            ),
            SQLTransformer(
                statement="""
            SELECT
                *,
                raw_diff_prediction + lag_store_item_sale_1 AS raw_prediction
            FROM
                __THIS__
            """
            ),
            SQLTransformer(
                statement="""
            SELECT
                *,
                GREATEST(raw_prediction, 0) AS prediction
            FROM
                __THIS__
            """
            ),
        ]
    )

    return model_pipe


def xgb_hyperparam_suggest(trial: Trial) -> Dict[str, float]:
    return {
        "subsample": trial.suggest_float("subsample", 0.5, 0.9),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 0.9),
        "n_estimators": trial.suggest_int("n_estimators", 20, 300),
        "max_depth": trial.suggest_int("max_depth", 6, 10),
        "learning_rate": trial.suggest_float("learning_rate", 0.05, 0.5, log=True),
        "min_child_weight": trial.suggest_float(
            "min_child_weight", low=0.1, high=100, log=True
        ),
        "reg_alpha": trial.suggest_float("reg_alpha", 1e-8, 10.0, log=True),
        "reg_lambda": trial.suggest_float("reg_lambda", 1e-8, 10.0, log=True),
        "gamma": trial.suggest_float("gamma", 1e-10, 10.0, log=True),
        "early_stopping_rounds": 10,
        "random_state": 42,
    }
