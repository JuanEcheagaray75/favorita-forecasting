from kedro.pipeline import Pipeline, node

from .nodes import (
    create_processed_calendar,
    create_weekly_sales,
    create_weekly_sales_filled,
)


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        nodes=[
            node(
                func=create_processed_calendar,
                inputs="calendar",
                outputs="calendar_processed",
            ),
            node(
                func=create_weekly_sales,
                inputs="train_processed",
                outputs="weekly_sales",
            ),
            node(
                func=create_weekly_sales_filled,
                inputs=[
                    "weekly_sales",
                    "calendar_processed",
                    "stores_processed",
                    "items_processed",
                ],
                outputs="weekly_sales_filled"
            ),
        ]
    )
