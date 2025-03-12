from typing import Any

from pyspark.ml.feature import VectorAssembler, SQLTransformer
from pyspark.ml.pipeline import Pipeline
from xgboost.spark import SparkXGBRegressor


def create_base_model(
    input_cols: list, target_col: str, xgb_params: dict[str, Any]
) -> Pipeline:
    numerical_feature_assembler = VectorAssembler(
        inputCols=input_cols, outputCol="features", handleInvalid="keep"
    )
    base_model = SparkXGBRegressor(
        features_col="features",
        label_col=target_col,
        prediction_col="base_prediction",
        tree_method="hist",
        objective="reg:squaredlogerror",
        validate_input_parameters=True,
        random_state=42,
        **xgb_params,
    )
    fallback_prediction = SQLTransformer(
        statement="""
        SELECT
            *,
            lag_unit_sale_1 AS fallback_prediction
        FROM
            __THIS__
        """
    )
    negativity_base_check = SQLTransformer(
        statement="""
        SELECT
            *,
            GREATEST(0, base_prediction) AS corrected_base_prediction
        FROM
            __THIS__
        """
    )
    base_model_pipe = Pipeline(
        stages=[
            numerical_feature_assembler,
            base_model,
            fallback_prediction,
            negativity_base_check,
        ]
    )

    return base_model_pipe


def create_dle_target_pipe(target_col: str) -> Pipeline:
    dle_target_pipe = Pipeline(
        stages=[
            SQLTransformer(
                statement=f"""
            SELECT
                *,
                POW(LOG1P(corrected_base_prediction) - LOG1P({target_col}), 2) AS base_sle
            FROM
                __THIS__
            """
            ),
            SQLTransformer(
                statement=f"""
            SELECT
                *,
                POW(LOG1P(fallback_prediction) - LOG1P({target_col}), 2) AS fallback_sle
            FROM
                __THIS__
            """
            ),
        ]
    )

    return dle_target_pipe


def create_nanny_models_pipe(
    input_cols: list[str],
    xgb_base_nanny_params: dict[str, Any],
    xgb_fallback_nanny_params: dict[str, Any],
) -> Pipeline:
    nanny_features_base = VectorAssembler(
        inputCols=[*input_cols, "corrected_base_prediction"],
        outputCol="nanny_base_features",
        handleInvalid="keep",
    )
    nanny_features_fallback = VectorAssembler(
        inputCols=[*input_cols, "fallback_prediction"],
        outputCol="nanny_fallback_features",
        handleInvalid="keep",
    )
    nanny_base_model = SparkXGBRegressor(
        features_col="nanny_base_features",
        label_col="base_sle",
        prediction_col="nanny_base_loss_estimate",
        objective="reg:squarederror",
        tree_method="hist",
        random_state=42,
        **xgb_base_nanny_params,
    )
    nanny_fallback_model = SparkXGBRegressor(
        features_col="nanny_fallback_features",
        label_col="fallback_sle",
        prediction_col="nanny_fallback_loss_estimate",
        objective="reg:squarederror",
        tree_method="hist",
        random_state=42,
        **xgb_fallback_nanny_params,
    )
    nanny_models_pipe = Pipeline(
        stages=[
            nanny_features_base,
            nanny_features_fallback,
            nanny_base_model,
            nanny_fallback_model,
        ]
    )

    return nanny_models_pipe


def create_prediction_adjustment_pipe():
    naive_inference_selector = SQLTransformer(
        statement="""
        SELECT
            *,
            CASE
                WHEN nanny_base_loss_estimate < nanny_fallback_loss_estimate THEN corrected_base_prediction
                ELSE fallback_prediction
            END AS final_prediction
        FROM
            __THIS__
        """
    )
    # Final Check
    negativity_check = SQLTransformer(
        statement="""
        SELECT
            *,
            GREATEST(final_prediction, 0) AS prediction
        FROM
            __THIS__
        """
    )
    final_sanity_check_pipe = Pipeline(
        stages=[naive_inference_selector, negativity_check]
    )

    return final_sanity_check_pipe
