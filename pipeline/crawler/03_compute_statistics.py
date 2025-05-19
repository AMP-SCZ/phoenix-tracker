#!/usr/bin/env python
"""
Compute Statistics

Computes the volume and number of files for each subject, by modality and protection status.
"""

import sys
from pathlib import Path

file = Path(__file__).resolve()
parent = file.parent
ROOT = None
for parent in file.parents:
    if parent.name == "phoenix-tracker":
        ROOT = parent
sys.path.append(str(ROOT))

# remove current directory from path
try:
    sys.path.remove(str(parent))
except ValueError:
    pass

import logging
import multiprocessing
from datetime import datetime
from typing import Any, Dict, List, Tuple

from rich.logging import RichHandler

from pipeline import data
from pipeline.helpers import db, utils
from pipeline.models.volume_statistics import VolumeStatistics

MODULE_NAME = "compute_statistics"
INSTANCE_NAME = MODULE_NAME

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def process_subject(subject_id: str, config_file: str) -> List[Dict[str, Any]]:
    """
    Get statistics for a specific subject by modality and protection status.

    Args:
        subject_id (str): The subject ID.
        config_file (str): The path to the configuration file.

    Returns:
        List[Dict[str, Any]]: The statistics for the subject.
    """
    study_id = data.get_subject_study_id(subject_id, config_file=config_file)
    modalities = data.get_subject_modalities(subject_id, config_file=config_file)

    results = []

    for modality in modalities:
        modality_files_df = data.get_subject_modality_files(
            subject_id=subject_id, modality=modality, config_file=config_file
        )

        for is_protected in [True, False]:
            for is_raw in [True, False]:
                filtered_df = modality_files_df[
                    (modality_files_df["is_protected"] == is_protected)
                    & (modality_files_df["is_raw"] == is_raw)
                ]

                if filtered_df.empty:
                    continue

                files_count = len(filtered_df)
                size_sum = filtered_df["file_size_mb"].sum()

                result = {
                    "subject_id": subject_id,
                    "study_id": study_id,
                    "modality": modality,
                    "is_protected": is_protected,
                    "is_raw": is_raw,
                    "files_count": files_count,
                    "files_size_mb": size_sum,
                }

                results.append(result)

                if is_protected and is_raw:
                    sub_types: List[Tuple[str, str]] = []
                    if modality == "phone":
                        sub_types = [
                            ("mindlamp_type", "sensor"),
                            ("mindlamp_type", "activity"),
                        ]
                    elif modality == "surveys":
                        sub_types = [
                            ("redcap_instance", "UPENN"),
                            ("redcap_instance", "MGB"),
                        ]

                    for sub_type in sub_types:
                        sub_type_name, sub_type_value = sub_type

                        filtered_df = data.get_subject_modality_files_by_metadata(
                            subject_id=subject_id,
                            modality=modality,
                            metadata_key=sub_type_name,
                            metadata_value=sub_type_value,
                            config_file=config_file,
                        )

                        if filtered_df.empty:
                            continue

                        files_count = len(filtered_df)
                        size_sum = filtered_df["file_size_mb"].sum()

                        result = {
                            "subject_id": subject_id,
                            "study_id": study_id,
                            "modality": f"{modality}_{sub_type_value}",
                            "is_protected": is_protected,
                            "is_raw": is_raw,
                            "files_count": files_count,
                            "files_size_mb": size_sum,
                        }
                        results.append(result)

    return results


def process_subject_wrapper(args: Tuple[str, str]) -> List[Dict[str, Any]]:
    """
    Wrapper function for the process_subject function.

    Args:
        args (Tuple[str, str]): The arguments to pass to the process_subject function.

    Returns:
        List[Dict[str, Any]]: The results of the process_subject
    """
    return process_subject(*args)


def compute_statistics(
    config_file: Path,
) -> None:
    """
    Compute the statistics for each subject and insert them into the database.

    Args:
        config_file (Path): The path to the configuration file.

    Returns:
        None
    """
    subjects = data.get_all_subjects(config_file=config_file)
    params = [(subject, config_file) for subject in subjects]

    results: List[Dict[str, Any]] = []

    with multiprocessing.Pool() as pool:
        with utils.get_progress_bar() as progress:
            task = progress.add_task("Parsing subject files", total=len(params))
            for result in pool.imap_unordered(
                process_subject_wrapper,
                params,
            ):
                results.extend(result)
                progress.update(task, advance=1)

    logger.info(f"Found {len(results)} results.")
    logger.info("Constructing SQL queries...")

    timestamp = datetime.now()
    logger.info(f"Timestamp: {timestamp}")

    queries: List[str] = []

    for result in results:
        subject_id = result["subject_id"]
        study_id = result["study_id"]
        modality = result["modality"]
        is_protected = result["is_protected"]
        is_raw = result["is_raw"]
        files_count = result["files_count"]
        files_size_mb = result["files_size_mb"]

        statistics = VolumeStatistics(
            subject_id=subject_id,
            study_id=study_id,
            modality=modality,
            is_protected=is_protected,
            is_raw=is_raw,
            files_count=files_count,
            files_size_mb=files_size_mb,
            statistics_timestamp=timestamp,
        )

        insert_query = statistics.to_sql()

        queries.append(insert_query)

    db.execute_queries(
        config_file=config_file,
        queries=queries,
        show_commands=False,
        show_progress=True,
    )


if __name__ == "__main__":
    config_file = utils.get_config_file_path()
    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    console.rule(f"[bold red]{MODULE_NAME}")
    logger.info(f"Using config file: {config_file}")

    compute_statistics(config_file=config_file)

    logger.info("Done.")
