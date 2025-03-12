from kedro.pipeline import Pipeline, node

from .nodes import train_val_test_split


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        nodes=[
            node(
                func=train_val_test_split,
                inputs=["featured_weekly_sales_filled", "calendar_processed"],
                outputs=["train_featurized", "val_featurized", "test_featurized"],
            )
        ]
    )
