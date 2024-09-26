#!/usr/bin/env python
"""
Import Files Metadata

This script imports the metadata of the files in the PHOENIX directory.
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
from typing import List, Tuple

from rich.logging import RichHandler

from pipeline import data
from pipeline.helpers import db, utils
from pipeline.models.files import File
from pipeline.models.phoenix_file import PhoenixFile

MODULE_NAME = "import_files"
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


def parse_subject_files_by_modality_root(
    subject_id: str,
    study_id: str,
    modality_root: Path,
    is_protected: bool,
    is_raw: bool,
) -> List[Tuple[File, PhoenixFile]]:
    """
    Parse the files for a specific subject by modality root.

    Args:
        subject_id (str): The subject ID.
        study_id (str): The study ID.
        modality_root (Path): The path to the modality root.
        is_protected (bool): Whether the data is protected.
        is_raw (bool): Whether the data is raw.

    Returns:
        List[Tuple[File, PhoenixFile]]: A list of tuples containing the file and the PhoenixFile
    """

    subject_files = []

    for path in modality_root.rglob("*"):
        if path.is_file():
            file = File(
                file_path=path,
                with_hash=False,
            )
            phoenix_file = PhoenixFile(
                subject_id=subject_id,
                study_id=study_id,
                file_path=path,
                is_protected=is_protected,
                is_raw=is_raw,
                modality=modality_root.name,
                extracted_timestamp=datetime.now(),
                metadata={},
            )
            subject_files.append((file, phoenix_file))

    return subject_files


def parse_subject_files_by_type(
    config_file: Path,
    subject_id: str,
    is_protected: bool,
    is_raw: bool,
) -> List[Tuple[File, PhoenixFile]]:
    """
    Parse the files for a specific subject by type
        Type: protected/raw, raw/processed

    Args:
        config_file (Path): The path to the configuration file.
        subject_id (str): The subject ID.
        is_protected (bool): Whether the data is protected.
        is_raw (bool): Whether the data is raw.

    Returns:
        List[Tuple[File, PhoenixFile]]: A list of tuples containing the file and the PhoenixFile
    """
    study_id = data.get_subject_study_id(config_file=config_file, subject_id=subject_id)

    if study_id is None:
        console.print(f"Subject {subject_id} not found in any study.")
        raise FileNotFoundError

    subject_data_root = data.get_study_roots(
        config_file=config_file, study_id=study_id, protected=is_protected
    )

    if is_raw:
        subject_data_root = subject_data_root / "raw" / subject_id
    else:
        subject_data_root = subject_data_root / "processed" / subject_id

    if not subject_data_root.exists():
        if not (not is_protected and is_raw):
            console.print(f"Subject {subject_id} has no data - {subject_data_root}")
        raise FileNotFoundError

    # list directories in subject data root to get modalities
    modalities = [x.name for x in subject_data_root.iterdir() if x.is_dir()]

    subject_files = []
    for modality in modalities:
        modality_root = subject_data_root / modality
        subject_files += parse_subject_files_by_modality_root(
            subject_id=subject_id,
            study_id=study_id,
            modality_root=modality_root,
            is_protected=is_protected,
            is_raw=is_raw,
        )

    return subject_files


def parse_subject_files(
    config_file: Path,
    subject_id: str,
) -> List[Tuple[File, PhoenixFile]]:
    """
    Parse the files for a specific subject.

    Args:
        config_file (Path): The path to the configuration file.
        subject_id (str): The subject ID.

    Returns:
        List[Tuple[File, PhoenixFile]]: A list of tuples containing the file and the PhoenixFile
    """
    subject_files = []

    for is_protected in [True, False]:
        for is_raw in [True, False]:
            try:
                subject_files += parse_subject_files_by_type(
                    config_file=config_file,
                    subject_id=subject_id,
                    is_protected=is_protected,
                    is_raw=is_raw,
                )
            except FileNotFoundError:
                pass

    return subject_files


def parse_subject_files_wrapper(
    params: Tuple[Path, str],
):
    """
    Wrapper function to parse subject files.

    Args:
        params (Tuple[Path, str]): A tuple containing the configuration file and the subject ID.
    """
    config_file, subject_id = params
    return parse_subject_files(
        config_file=config_file,
        subject_id=subject_id,
    )


def import_phoenix_metadata(
    config_file: Path,
):
    """
    Import the metadata of the files in the PHOENIX directory.

    Args:
        config_file (Path): The path to the configuration

    Returns:
        None
    """
    subjects = data.get_all_subjects(config_file=config_file)
    params = [(config_file, subject_id) for subject_id in subjects]

    subject_files: List[Tuple[File, PhoenixFile]] = []

    with multiprocessing.Pool() as pool:
        with utils.get_progress_bar() as progress:
            task = progress.add_task("Parsing subject files", total=len(params))
            for result in pool.imap_unordered(
                parse_subject_files_wrapper,
                params,
            ):
                subject_files.extend(result)
                progress.update(task, advance=1)

    logger.info(f"Found {len(subject_files)} files to import.")
    logger.info("Constructing SQL queries...")
    queries: List[str] = []

    for file, phoenix_file in subject_files:
        file_query = file.to_sql()
        phoenix_file_query = phoenix_file.to_sql()
        queries.append(file_query)
        queries.append(phoenix_file_query)

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

    import_phoenix_metadata(config_file=config_file)

    logger.info("Done.")
