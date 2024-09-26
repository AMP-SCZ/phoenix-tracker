#!/usr/bin/env python
"""
Loads the study metadata into the database.

Populates the 'subject' table with the study metadata CSV file.
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
from datetime import datetime
from typing import List

import pandas as pd
from rich.logging import RichHandler

from pipeline.helpers import db, utils
from pipeline.models.subjects import Subject
from pipeline import data

MODULE_NAME = "import_study_metadata"
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


def get_study_metadata(config_file: Path, study_id: str) -> pd.DataFrame:
    """
    Gets the study metadata from the PHOENIX structure

    Args:
        config_file (str): The path to the configuration file.
        study_id (str): The ID of the study.

    Returns:
        pd.DataFrame: The study metadata.

    Raises:
        FileNotFoundError: If the study metadata file is not found.
    """

    # Construct path to study metadata
    params = utils.config(path=config_file, section="general")
    data_roots = params["data_root"]
    data_roots = data_roots.split(",")
    data_roots = [Path(root) for root in data_roots]

    network_id = data.get_study_network(study_id=study_id, config_file=config_file)
    if network_id is None:
        raise ValueError(f"Network ID not found for study: {study_id}")

    # sentence case
    network_id = network_id.lower().capitalize()
    metadata_filename = f"{network_id}{study_id}_metadata.csv"

    for data_root in data_roots:
        study_metadata = Path(data_root, "PROTECTED", f"{network_id}{study_id}", metadata_filename)

        # Check if study_metadata exists
        if not study_metadata.exists():
            pass
        else:
            break

    if not study_metadata.exists():
        raise FileNotFoundError(f"Study metadata file not found: {study_metadata}")

    # Read study metadata
    study_metadata = pd.read_csv(study_metadata)

    return study_metadata


def fetch_subjects(config_file: Path, study_id: str) -> List[Subject]:
    """
    Fetches the subjects from the study metadata.

    Args:
        config_file (Path): The path to the configuration file.
        study_id (str): The ID of the study.
    """

    # Get study metadata
    study_metadata = get_study_metadata(config_file=config_file, study_id=study_id)

    subjects: List[Subject] = []
    for _, row in study_metadata.iterrows():
        # Get required fields
        required_fields = ["Subject ID", "Active", "Consent", "Study"]

        # Drop rows with NaN in required fields
        if pd.isna(row[required_fields]).any():
            continue

        subject_id = row["Subject ID"]
        active = row["Active"]  # 1 if active, 0 if inactive
        consent = row["Consent"]

        # Cast to bool
        active = bool(active)

        # Cast to DateTime
        consent = datetime.strptime(consent, "%Y-%m-%d")

        # construct optional_notes field as a JSON object
        # with all other fields as key-value pairs
        optional_notes = {}
        for field in study_metadata.columns:
            if field in required_fields:
                continue
            # Skip if NaN
            if pd.isna(row[field]):
                continue
            optional_notes[field] = row[field]

        subject = Subject(
            study_id=study_id,
            subject_id=subject_id,
            is_active=active,
            consent_date=consent,
            optional_notes=optional_notes,
        )

        subjects.append(subject)

    logger.info(f"Found {len(subjects)} subjects.")
    return subjects


def insert_subjects(config_file: Path, subjects: List[Subject]):
    """
    Inserts the subjects into the database.

    Args:
        config_file (Path): The path to the configuration file.
        subjects (List[Subject]): The list of subjects to insert.
    """

    queries = [subject.to_sql() for subject in subjects]

    db.execute_queries(config_file=config_file, queries=queries, show_commands=False)


if __name__ == "__main__":
    config_file = utils.get_config_file_path()
    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    console.rule(f"[bold red]{MODULE_NAME}")
    logger.info(f"Using config file: {config_file}")

    studies = data.get_all_studies(config_file=config_file)

    logger.info(f"Found {len(studies)} studies.")
    with utils.get_progress_bar() as progress:
        task = progress.add_task("Importing studies...", total=len(studies))
        for study_id in studies:
            logger.info(f"Importing study: {study_id}")
            progress.update(task, description=f"Importing study: {study_id}")

            try:
                subjects = fetch_subjects(config_file=config_file, study_id=study_id)
                insert_subjects(config_file=config_file, subjects=subjects)
            except FileNotFoundError as e:
                logger.error(e)
                continue

            progress.update(task, advance=1)

    logger.info("Done.")
