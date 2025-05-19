"""
Contains helper functions for data specific to the pipeline.
"""

import logging
from functools import lru_cache
from pathlib import Path
from typing import List, Optional

import pandas as pd

from pipeline.helpers import db, utils

logger = logging.getLogger(__name__)


def get_all_studies(config_file: Path) -> List[str]:
    """
    Gets all the studies from the database.

    Args:
        config_file (Path): The path to the configuration file.

    Returns:
        List[str]: A list of study IDs.
    """

    query = """
        SELECT study_id
        FROM study
        ORDER BY study_id;
    """

    db_df = db.execute_sql(config_file=config_file, query=query)
    if db_df.empty:
        logger.warning("No studies found in the database.")
        return []

    studies = db_df["study_id"].tolist()

    return studies


@lru_cache
def get_study_network(study_id: str, config_file: Path) -> Optional[str]:
    """
    Gets the network ID for a given study.

    Args:
        study_id (str): The study ID.
        config_file (Path): The path to the configuration file.

    Returns:
        Optional[str]: The network ID.
    """

    query = f"""
        SELECT network_id
        FROM study
        WHERE study_id = '{study_id}';
    """

    result = db.fetch_record(config_file=config_file, query=query)

    return result


@lru_cache
def get_study_roots(
    study_id: str, config_file: Path, protected: bool = False
) -> Optional[Path]:
    """
    Gets the root directories for a given study.

    Args:
        study_id (str): The study ID.
        config_file (Path): The path to the configuration file.

    Returns:
        List[Path]: A list of root directories for the study.
    """
    general_params = utils.config(config_file, "general")
    data_roots = general_params["data_root"]
    data_roots = data_roots.split(",")
    data_roots = [Path(root) for root in data_roots]

    network_id = get_study_network(study_id=study_id, config_file=config_file)
    if network_id is None:
        raise ValueError(f"Network ID not found for study: {study_id}")

    # sentence case
    network_id = network_id.lower().capitalize()

    correct_root = None
    for data_root in data_roots:
        study_metadata_csv = Path(data_root, "PROTECTED", f"{network_id}{study_id}")
        if study_metadata_csv.exists():
            correct_root = data_root
            break

    if correct_root is None:
        logger.warning(f"Study root not found for study: {study_id}")
        return None

    if protected:
        study_root = Path(correct_root, "PROTECTED", f"{network_id}{study_id}")
    else:
        study_root = Path(correct_root, "GENERAL", f"{network_id}{study_id}")

    return study_root


def get_all_subjects(config_file: Path, study_id: Optional[str] = None) -> List[str]:
    """
    Gets all the subjects from the database.

    Args:
        config_file (Path): The path to the configuration file.
        study_id (str, optional): The study ID. Defaults to None.

    Returns:
        List[str]: A list of subject IDs.
    """

    if study_id is not None:
        query = f"""
            SELECT subject_id
            FROM subjects
            WHERE study_id = '{study_id}'
            ORDER BY subject_id;
        """
    else:
        query = """
            SELECT subject_id
            FROM subjects
            ORDER BY subject_id;
        """

    db_df = db.execute_sql(config_file=config_file, query=query)
    if db_df.empty:
        logger.warning("No subjects found in the database.")
        return []

    subjects = db_df["subject_id"].tolist()

    return subjects


def get_subject_study_id(subject_id: str, config_file: Path) -> Optional[str]:
    """
    Gets the study ID for a given subject.

    Args:
        subject_id (str): The subject ID.
        config_file (Path): The path to the configuration file.

    Returns:
        Optional[str]: The study ID.
    """

    query = f"""
        SELECT study_id
        FROM subjects
        WHERE subject_id = '{subject_id}';
    """

    result = db.fetch_record(config_file=config_file, query=query)

    return result


def get_all_modalities(config_file: Path) -> List[str]:
    """
    Gets all available modalities accross all subjects.

    Args:
        config_file (Path): The path to the configuration file.

    Returns:
        List[str]: A list of modalities.
    """

    query = """
        SELECT DISTINCT modality
        FROM phoenix_file
    """

    db_df = db.execute_sql(config_file=config_file, query=query)
    if db_df.empty:
        logger.warning("No modalities found!")
        return []

    modalities = db_df["modality"].tolist()

    return modalities


def get_subject_modalities(subject_id: str, config_file: Path) -> List[str]:
    """
    Gets the modalities for a given subject.

    Args:
        subject_id (str): The subject ID.
        config_file (Path): The path to the configuration file.

    Returns:
        List[str]: A list of modalities.
    """

    study_id = get_subject_study_id(subject_id=subject_id, config_file=config_file)

    query = f"""
        SELECT DISTINCT modality
        FROM phoenix_file
        WHERE subject_id = '{subject_id}'
            AND study_id = '{study_id}';
    """

    db_df = db.execute_sql(config_file=config_file, query=query)
    if db_df.empty:
        logger.warning(f"No modalities found for subject: {subject_id}")
        return []

    modalities = db_df["modality"].tolist()

    return modalities


def get_subject_modality_files(
    subject_id: str, modality: str, config_file: Path
) -> pd.DataFrame:
    """
    Gets the files for a given subject and modality.

    Args:
        subject_id (str): The subject ID.
        modality (str): The modality.
        config_file (Path): The path to the configuration file.

    Returns:
        pd.DataFrame: A DataFrame containing the files.
    """

    study_id = get_subject_study_id(subject_id=subject_id, config_file=config_file)

    query = f"""
        SELECT *
        FROM phoenix_file
        LEFT JOIN files USING (file_path)
        WHERE subject_id = '{subject_id}'
            AND study_id = '{study_id}'
            AND modality = '{modality}';
    """

    db_df = db.execute_sql(config_file=config_file, query=query)

    return db_df


def get_subject_modality_files_by_metadata(
    subject_id: str,
    modality: str,
    metadata_key: str,
    metadata_value: str,
    config_file: Path,
) -> pd.DataFrame:
    """
    Gets the files for a given subject and modality.

    Args:
        subject_id (str): The subject ID.
        modality (str): The modality.
        metadata (str): The metadata.
        config_file (Path): The path to the configuration file.

    Returns:
        pd.DataFrame: A DataFrame containing the files.
    """

    study_id = get_subject_study_id(subject_id=subject_id, config_file=config_file)

    query = f"""
        SELECT *
        FROM phoenix_file
        LEFT JOIN files USING (file_path)
        WHERE subject_id = '{subject_id}'
            AND study_id = '{study_id}'
            AND modality = '{modality}'
            AND metadata->>'{metadata_key}' LIKE '%%{metadata_value}%%';
    """

    db_df = db.execute_sql(config_file=config_file, query=query)

    return db_df
