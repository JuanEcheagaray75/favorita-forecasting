from kedro.pipeline import Pipeline, node

from .nodes import clean_items, clean_stores, clean_train, create_calendar


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        nodes=[
            node(func=clean_items, inputs="items", outputs="items_processed"),
            node(
                func=clean_stores, inputs="stores", outputs="stores_processed"
            ),
            node(func=clean_train, inputs="train", outputs="train_processed"),
            node(
                func=create_calendar,
                inputs="train_processed",
                outputs="calendar",
            ),
        ]
    )
