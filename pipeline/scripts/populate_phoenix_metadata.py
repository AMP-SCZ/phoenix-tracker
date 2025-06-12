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
from typing import List, Dict, Any

from rich.logging import RichHandler

from pipeline.helpers import db, utils
from pipeline.models.phoenix_metadata import PhoenixFileMetadata

MODULE_NAME = "populate_phoenix_metadata"
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


def get_scan_ids_to_process(config_file: Path) -> List[int]:
    """
    Get scan IDs that need to be processed.
    Returns:
        List[int]: List of scan IDs to process.
    """

    query = """
    SELECT scan_id
    FROM filesystem.scan_runs
    WHERE scan_id NOT IN (
        SELECT scan_id
        FROM phoenix.file_metadata
    )
    AND scan_id NOT IN (
        -- exclude the first scan of each scan_root
        SELECT MIN(scan_id)
        FROM filesystem.scan_runs
        GROUP BY scan_root
    )
ORDER BY scan_id;
    """
    df = db.execute_sql(
        config_file=config_file,
        query=query,
    )

    if df.empty:
        logging.info("No scan IDs to process.")
        return []

    results: List[int] = []
    for index, row in df.iterrows():
        scan_id = row["scan_id"]
        results.append(int(float(scan_id)))

    return results


def get_file_changes_by_scan_id(scan_id: int, config_file: Path) -> List[Path]:
    """
    Get all file changes for a given scan ID.

    Args:
        scan_id (int): The scan ID to query.
        config_file (Path): The path to the configuration file.

    Returns:
        List[Path]: A list of file paths that were changed in the scan.
    """
    sql_query = f"""
        SELECT 
            file_path
        FROM filesystem.file_changes
        WHERE scan_id = {scan_id}
        ORDER BY file_path;
    """

    df = db.execute_sql(
        query=sql_query,
        config_file=config_file,
    )

    if df.empty:
        return []

    results: List[Path] = []
    for file_path in df["file_path"].tolist():
        results.append(Path(file_path))

    return results


def process_changes(
    changes: List[Path],
    scan_id: int,
) -> List[PhoenixFileMetadata]:
    """
    Process the file changes and extract PHOENIX metadata.

    Args:
        changes (List[Path]): List of file paths that were changed.
        scan_id (int): The scan ID associated with the changes.

    Returns:
        List[PhoenixFileMetadata]: A list of PhoenixFileMetadata objects
    """
    results: List[PhoenixFileMetadata] = []

    with utils.get_progress_bar() as progress:
        task = progress.add_task(
            "Processing file changes",
            total=len(changes),
        )
        for file_path in changes:
            progress.update(task, advance=1)
            try:
                # Remove any leading directories that are not part of the Phoenix path
                root_path = str(file_path).split("PHOENIX", maxsplit=1)[0]
                phoenix_path = file_path.relative_to(root_path)

                network_id = root_path.rstrip("/").split("/")[-1]
                access_level = phoenix_path.parts[1]
                study_id = phoenix_path.parts[2]
                data_stage = phoenix_path.parts[3]
                subject_id = phoenix_path.parts[4]
                modality = phoenix_path.parts[5]

                if modality.endswith(".log"):
                    # Skip log files
                    continue

                file_name = phoenix_path.name
                file_metadata: Dict[str, Any] = {}
                if modality == "surveys":
                    if file_name.endswith("UPENN_nda.json"):
                        file_metadata["redcap_instance"] = "UPENN"
                    elif file_name.endswith("Pronet.json"):
                        file_metadata["redcap_instance"] = "MGB-Pronet"
                    elif file_name.endswith("Prescient.json"):
                        file_metadata["redcap_instance"] = "MGB-Prescient"
                elif modality == "phone":
                    if "_sensor_" in file_name:
                        file_metadata["mindlamp_type"] = "sensor"
                    elif "_activity_" in file_name:
                        file_metadata["mindlamp_type"] = "activity"

                phoenix_metadata = PhoenixFileMetadata(
                    scan_id=scan_id,
                    file_path=str(file_path),
                    network_id=network_id,
                    access_level=access_level,
                    study_id=study_id,
                    data_stage=data_stage,
                    subject_id=subject_id,
                    modality=modality,
                    extra_meta=file_metadata,
                )
                results.append(phoenix_metadata)
            except Exception as e:
                print(f"Error processing file {file_path}: {e}")
        return results


if __name__ == "__main__":
    config_file = utils.get_config_file_path()
    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    console.rule(f"[bold red]{MODULE_NAME}")
    logger.info(f"Using config file: {config_file}")

    scans_ids = get_scan_ids_to_process(config_file=config_file)

    for scan_id in scans_ids:
        logger.info(f"Processing scan ID: {scan_id}")

        logger.info(f"Fetching file changes for scan ID: {scan_id}")
        changes = get_file_changes_by_scan_id(scan_id=scan_id, config_file=config_file)
        if not changes:
            logger.warning(f"No file changes found for scan ID: {scan_id}")
            continue

        phoenix_metadata_list = process_changes(changes=changes, scan_id=scan_id)

        queries = [f.to_sql() for f in phoenix_metadata_list]

        delete_query = f"""
            DELETE FROM phoenix.file_metadata
            WHERE scan_id = {scan_id}
        """

        queries.insert(0, delete_query)

        post_queries = [
            "VACUUM ANALYZE phoenix.file_metadata",
        ]

        queries.extend(post_queries)

        logger.info(f"Executing {len(queries)} queries for scan ID: {scan_id}")
        db.execute_queries(
            config_file=config_file,
            queries=queries,
            show_commands=False,
            show_progress=True,
        )

    logger.info("All scan IDs processed successfully.")
