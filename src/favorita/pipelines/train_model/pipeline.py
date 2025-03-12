from kedro.pipeline import Pipeline, node

from .nodes import train_model


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        nodes=[
            node(
                func=train_model,
                inputs=["train_featurized", "val_featurized", "test_featurized"],
                outputs=None
            )
        ]
    )
