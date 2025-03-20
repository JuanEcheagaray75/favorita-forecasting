from kedro.pipeline import Pipeline, node

from .nodes import optimize_model


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        nodes=[
            node(
                func=optimize_model,
                inputs=[
                    "cross_val_datasets",
                    "params:experiment_name",
                    "params:n_trials",
                ],
                outputs=None,
            )
        ]
    )
