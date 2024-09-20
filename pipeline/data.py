"""
Contains helper functions for data specific to the pipeline.
"""

import logging
from pathlib import Path
from typing import List, Optional

from pipeline.helpers import db

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
