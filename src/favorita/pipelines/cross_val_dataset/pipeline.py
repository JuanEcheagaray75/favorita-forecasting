from kedro.pipeline import Pipeline, node

from .nodes import split_dataset


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        nodes=[
            node(
                func=split_dataset,
                inputs=[
                    "featured_weekly_sales_filled",
                    "calendar_processed",
                    "params:earliest_train_date",
                    "params:num_cv_splits",
                ],
                outputs="cross_val_datasets",
            )
        ]
    )
