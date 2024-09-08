"""
This is a boilerplate pipeline 'download_data'
generated using Kedro 0.19.6
"""

from pathlib import Path
from typing import Dict
from zipfile import ZipFile

import kaggle
import py7zr
from pyspark.sql import DataFrame, SparkSession

spark: SparkSession = SparkSession.builder.getOrCreate()


def download_dataset(competition: str, dataset_dir: str) -> str:
    """Download compressed Kaggle competition file

    Args:
        competition (str): Competition Name (as in the CLI tool)
        dataset_dir (str): Directory in which to store the uncompressed dataset
    """
    dataset_file = Path(f"{dataset_dir}/{competition}.zip")

    if not dataset_file.is_file():
        kaggle.api.authenticate()
        kaggle.api.competition_download_files(
            competition=competition, path=dataset_dir
        )

    return str(dataset_file)


def extract_competition_files(dataset_zip_path: str, output_path: str) -> str:
    """Extract all files in downloaded zip file from a Kaggle Competition

    Args:
        dataset_zip_path (str): Path to the .zip file
        output_path (str): Directory in which to store the extracted files
    """
    dataset_zip_dir = Path(dataset_zip_path)
    if not any(f.suffix == ".7z" for f in dataset_zip_dir.glob("*.7z")):
        with ZipFile(file=dataset_zip_path, mode="r") as f:
            f.extractall(path=output_path)

    return output_path


def extract_7z_files(general_data_dir: str) -> str:
    """Extract .7z files found in .zip competition dataset

    Args:
        general_data_dir (str): Directory in which the 7z files are found
    """
    for comp_file in Path(general_data_dir).glob(pattern="*.7z"):
        # If the csv file isn't already there

        comp_file_csv = comp_file.name.replace(".7z", "")
        if not Path(f"{general_data_dir}/{comp_file_csv}").is_file():
            with py7zr.SevenZipFile(file=comp_file, mode="r") as f:
                f.extractall(path=general_data_dir)

    return general_data_dir


def load_and_process_extracted_csvs(
    extract_dir: str,
) -> Dict[str, DataFrame]:
    file_mapping = {
        "holidays_events.csv": "holidays_events",
        "items.csv": "items",
        "oil.csv": "oil",
        "sample_submission.csv": "sample_submission",
        "stores.csv": "stores",
        "test.csv": "test",
        "train.csv": "train",
        "transactions.csv": "transactions",
    }

    processed_datasets = {}

    extract_dir_path = Path(extract_dir)
    for csv_file in extract_dir_path.glob("*.csv"):
        if csv_file.name in file_mapping:
            df = spark.read.csv(path=str(csv_file), header=True)

            dataset_name = file_mapping[csv_file.name]

            processed_datasets[dataset_name] = df

    return processed_datasets
