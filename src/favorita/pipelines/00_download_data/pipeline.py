"""
This is a boilerplate pipeline 'download_data'
generated using Kedro 0.19.6
"""

from kedro.pipeline import Pipeline, node

from .nodes import (
    download_dataset,
    extract_7z_files,
    extract_competition_files,
    load_and_process_extracted_csvs,
)


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        nodes=[
            node(
                func=download_dataset,
                inputs=["params:dataset_competition", "params:output_path"],
                outputs="zip_file_path",
            ),
            node(
                func=extract_competition_files,
                inputs=["zip_file_path", "params:output_path"],
                outputs="output_path",
            ),
            node(
                func=extract_7z_files,
                inputs="output_path",
                outputs="raw_csv_directory",
            ),
            node(
                func=load_and_process_extracted_csvs,
                inputs="raw_csv_directory",
                outputs={
                    "holidays_events": "holidays_events",
                    "stores": "stores",
                    "items": "items",
                    "oil": "oil",
                    "sample_submission": "sample_submission",
                    "test": "test",
                    "train": "train",
                    "transactions": "transactions",
                },
            ),
        ]
    )
