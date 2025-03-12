import mlflow
from pyspark.sql import DataFrame
from pyspark.ml.pipeline import Pipeline, PipelineModel

from .evaluation import score_model, summarize_df
from .model_pipes import (
    create_base_model,
    create_dle_target_pipe,
    create_nanny_models_pipe,
    create_prediction_adjustment_pipe,
)


def train_model(train: DataFrame, val: DataFrame, test: DataFrame) -> PipelineModel:
    sampled_train = train.sample(0.025)
    exclude_cols = [
        "store_id",
        "item_id",
        "date",
        "time_percentage",
        "family",
        "class",
        "perishable",
        "city",
        "state",
        "type",
        "cluster",
        "item_weight",
        "first_product_store_purchase",
        "is_product_available",
    ]
    target_col = "unit_sale"
    input_columns = train.drop(*exclude_cols, target_col).columns

    base_model_pipe = create_base_model(
        input_cols=input_columns,
        target_col=target_col,
        xgb_params={"n_estimators": 100, "grow_policy": "lossguide"},
    )
    dle_target_pipe = create_dle_target_pipe(target_col=target_col)
    nanny_models_pipe = create_nanny_models_pipe(
        input_cols=input_columns,
        xgb_base_nanny_params={"n_estimators": 100, "grow_policy": "lossguide"},
        xgb_fallback_nanny_params={"n_estimators": 100, "grow_policy": "lossguide"},
    )
    final_adjustment_pipe = create_prediction_adjustment_pipe()

    base_model = base_model_pipe.fit(sampled_train)
    base_train_preds = base_model.transform(sampled_train)
    dle_model = dle_target_pipe.fit(base_train_preds)
    train_with_dle = dle_model.transform(base_train_preds)
    nanny_models = nanny_models_pipe.fit(train_with_dle)

    final_pipe = Pipeline(stages=[base_model, nanny_models, final_adjustment_pipe])
    final_model = final_pipe.fit(sampled_train)

    train_preds = final_model.transform(sampled_train)
    val_preds = final_model.transform(val)
    test_preds = final_model.transform(test)

    train_metrics = score_model(
        df=train_preds, label_col=target_col, pred_col="prediction", context="train"
    )
    val_metrics = score_model(
        df=val_preds, label_col=target_col, pred_col="prediction", context="val"
    )
    test_metrics = score_model(
        df=test_preds, label_col=target_col, pred_col="prediction", context="test"
    )
    print(train_metrics)
    print(val_metrics)
    print(test_metrics)
