from kedro.pipeline import Pipeline, node

from .nodes import featurize_df


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        nodes=[
            node(
                func=featurize_df,
                inputs="weekly_sales_filled",
                outputs="featured_weekly_sales_filled",
            )
        ]
    )
