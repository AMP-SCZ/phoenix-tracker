#!/usr/bin/env python
"""
Send Slack notification.

Sends a Slack notification via. Webhook.
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

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Literal, Tuple

import humanize
import requests
from rich.logging import RichHandler
import pandas as pd

from pipeline.helpers import db, utils

MODULE_NAME = "slack_send_notification"
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


def get_slack_webhook_url(config_file: Path) -> str:
    """
    Retrieves the Slack Webhook URL from the configuration file.

    Args:
        config_file (Path): The path to the configuration file.

    Returns:
        str: The Slack Webhook URL.
    """
    slack_config_params = utils.config(path=config_file, section="slack")
    slack_webhook_url = slack_config_params["slack_webhook_url"]
    return slack_webhook_url


def get_most_recent_scan_id(
    config_file: Path,
    data_root: str,
) -> Optional[int]:
    """
    Returns the most recent scan ID for a given network, access level, modality, and data stage.

    Args:
        config_file (Path): The path to the configuration file.
        data_root (str): The data root path.

    Returns:
        int: The most recent scan ID.
    """

    query = f"""
        SELECT MAX(scan_id) AS max_scan_id
        FROM filesystem.scan_runs
        WHERE scan_root = '{data_root}';
    """

    result = db.fetch_record(
        query=query,
        config_file=config_file,
    )

    if result is None or result == "None":
        return None

    return int(result)


def get_scan_dates(
    config_file: Path,
    data_root: str,
) -> Optional[Dict[str, datetime]]:
    """
    Returns the start dates of the most recent and previous scans for a given data root.

    Args:
        config_file (Path): The path to the configuration file.
        data_root (str): The root directory of the scan.

    Returns:
        Optional[Dict[str, datetime]]: A dictionary containing the start dates of the
            most recent and previous scans.
        If no scans are found, returns None.
    """

    query = f"""
        SELECT scan_id, started_at
        FROM filesystem.scan_runs
        WHERE scan_root = '{data_root}'
        ORDER BY started_at DESC
        LIMIT 2;
    """

    db_df = db.execute_sql(
        config_file=config_file,
        query=query,
    )

    previous_scan_datetime: Optional[pd.Timestamp] = (
        db_df["started_at"].iloc[1] if len(db_df) > 1 else None
    )
    most_recent_scan_datetime: Optional[pd.Timestamp] = (
        db_df["started_at"].iloc[0] if len(db_df) > 0 else None
    )

    # Cast to datetime if not None
    if previous_scan_datetime is not None:
        previous_scan_datetime = pd.to_datetime(previous_scan_datetime)
    if most_recent_scan_datetime is not None:
        most_recent_scan_datetime = pd.to_datetime(most_recent_scan_datetime)

    if previous_scan_datetime is None and most_recent_scan_datetime is None:
        return None

    result: Dict[str, datetime] = {}
    if previous_scan_datetime is not None:
        result["previous_scan_datetime"] = previous_scan_datetime.to_pydatetime()
    if most_recent_scan_datetime is not None:
        result["most_recent_scan_datetime"] = most_recent_scan_datetime.to_pydatetime()

    return result


def get_delta_files_count(
    config_file: Path,
    scan_id: int,
    network: str,
    access_level: Literal["PROTECTED", "GENERAL"],
    modality: str,
    data_stage: Literal["raw", "processed"],
    change_type: Literal["added", "deleted", "modified"],
    extra_metadata: Optional[Dict[str, Any]] = None,
) -> Optional[int]:
    """
    Returns the number if new files added to the database in the last scan.

    Args:
        config_file (Path): The path to the configuration file.
        scan_id (int): The scan ID.
        network (str): The network ID.
        access_level (str): The access level.
        modality (str): The modality.
        data_stage (str): The data stage.
        change_type (str): The type of change (e.g., 'added', 'removed').

    Returns:
        int: The delta files count.
    """

    files_sum_query_current = f"""
    SELECT COUNT(*) AS file_count
    FROM phoenix.file_metadata
    LEFT JOIN filesystem.file_changes USING (scan_id, file_path)
    WHERE scan_id = {scan_id}
    AND network_id = '{network}'
    AND access_level = '{access_level}'
    AND modality = '{modality}'
    AND data_stage = '{data_stage}'
    AND change_type = '{change_type}'
    """

    if extra_metadata:
        extra_metadata_json = json.dumps(extra_metadata)
        files_sum_query_current += f"AND extra_meta @> '{extra_metadata_json}'::jsonb"

    files_sum_query_current += ";"

    result = db.fetch_record(
        query=files_sum_query_current,
        config_file=config_file,
    )

    if result is None:
        return None

    files_count = int(result)
    return files_count


def get_delta_files_size(
    config_file: Path,
    scan_id: int,
    network: str,
    access_level: Literal["PROTECTED", "GENERAL"],
    modality: str,
    data_stage: Literal["raw", "processed"],
    change_type: Literal["added", "deleted", "modified"],
    extra_metadata: Optional[Dict[str, Any]] = None,
) -> Optional[int]:
    """
    Returns the total size of files that have changed in the last scan.

    Args:
        config_file (Path): The path to the configuration file.
        scan_id (int): The scan ID.
        network (str): The network ID.
        access_level (str): The access level.
        modality (str): The modality.
        data_stage (str): The data stage.
        change_type (str): The type of change (e.g., 'added', 'removed').

    Returns:
        int: The total size of changed files in bytes.
    """

    # Use abs of the difference between new and old size
    # cnsider null values in new_size_bytes and old_size_bytes as 0
    files_sum_query_current = f"""
    SELECT
        SUM(ABS(COALESCE(new_size_bytes, 0) - COALESCE(old_size_bytes, 0))) AS total_size_bytes
    FROM phoenix.file_metadata
    LEFT JOIN filesystem.file_changes USING (scan_id, file_path)
    WHERE scan_id = {scan_id}
    AND network_id = '{network}'
    AND access_level = '{access_level}'
    AND modality = '{modality}'
    AND data_stage = '{data_stage}'
    AND change_type = '{change_type}'
    """

    if extra_metadata:
        extra_metadata_json = json.dumps(extra_metadata)
        files_sum_query_current += f"AND extra_meta @> '{extra_metadata_json}'::jsonb"

    files_sum_query_current += ";"

    result = db.fetch_record(
        query=files_sum_query_current,
        config_file=config_file,
    )

    if result is None or result == "None":
        return None

    files_size_bytes = int(float(result))
    return files_size_bytes


def get_slack_formatted_date(date: datetime) -> str:
    """
    Formats the date for Slack:

    <!date^timestamp^token_string^optional_link|fallback_text>

    Referece: https://api.slack.com/reference/surfaces/formatting

    Args:
        date (datetime): The date to format.

    Returns:
        str: The formatted date string
    """
    timestamp = int(date.timestamp())
    token_string = "{date_short} {time}"

    # Check if TZ info is present
    if date.tzinfo:
        fallback_text = date.strftime("%Y-%m-%d %H:%M:%S %Z")
    else:
        current_tz = datetime.now().astimezone().tzinfo
        date = date.replace(tzinfo=current_tz)
        fallback_text = date.strftime("%Y-%m-%d %H:%M:%S %Z")

    slack_date_str = f"<!date^{timestamp}^{token_string}|{fallback_text}>"
    return slack_date_str


def construct_slack_blockkit_json(
    config_file: Path, data_root: Path
) -> Optional[Dict[str, Any]]:
    """
    Returns the Slack BlockKit JSON.

    Args:
        config_file (Path): The path to the configuration file.
        latest_timestamp (datetime): The latest timestamp.
        previous_timestamp (datetime): The previous timestamp.

    Returns:
        Dict[str, Any]: The Slack BlockKit JSON.
    """

    modalities = ["actigraphy", "eeg", "interviews", "mri", "phone", "surveys"]
    blocks = []

    scan_dates = get_scan_dates(
        config_file=config_file,
        data_root=str(data_root),
    )

    if (
        scan_dates is None
        or scan_dates.get("most_recent_scan_datetime") is None
        or scan_dates.get("previous_scan_datetime") is None
    ):
        logger.error("No scan dates found. Cannot construct Slack BlockKit JSON.")
        return None

    scan_id = get_most_recent_scan_id(
        config_file=config_file,
        data_root=str(data_root),
    )

    if scan_id is None:
        logger.error("No scan ID found. Cannot construct Slack BlockKit JSON.")
        return None

    logger.debug(f"Scan ID: {scan_id}")

    latest_timestamp: datetime = scan_dates.get("most_recent_scan_datetime")  # type: ignore
    previous_timestamp: datetime = scan_dates.get("previous_scan_datetime")  # type: ignore

    # Check if latest_timestamp is < 24H ago
    now = datetime.now()
    if latest_timestamp.tzinfo is not None:
        now = now.replace(tzinfo=latest_timestamp.tzinfo)
    elif now.tzinfo is not None:
        latest_timestamp = latest_timestamp.replace(tzinfo=now.tzinfo)
    
    if (now - latest_timestamp).total_seconds() > 24 * 3600:
        logger.error(
            f"Latest timestamp {latest_timestamp} is more than 24 hours old. "
            "Skipping Slack notification."
        )
        return None

    header = {
        "type": "section",
        "text": {
            "text": f"Comparing {get_slack_formatted_date(latest_timestamp)} to \
{get_slack_formatted_date(previous_timestamp)}.",
            "type": "mrkdwn",
        },
    }

    divider = {"type": "divider"}

    network: str = data_root.name
    network_sections = []
    issue_detected_modalities: List[str] = []

    network_section_elements = [
        {
            "type": "rich_text_section",
            "elements": [
                {
                    "type": "text",
                    "text": f"{network}",
                    "style": {"bold": True},
                },
            ],
        },
    ]

    for modality in modalities:
        bullet_list_block = {
            "type": "rich_text_list",
            "indent": 0,
            "style": "bullet",
        }
        bullet_list_elements: List[Dict[str, Any]] = []

        access_level = "PROTECTED"
        data_stage = "raw"
        qualifier: str = ""

        if modality == "interviews":
            access_level = "GENERAL"
            data_stage = "processed"
            qualifier = "- (GENERAL, processed)"

        data_dict: Dict[str, Any] = {}

        for change_type in ["added", "deleted", "modified"]:
            delta_files = get_delta_files_count(
                config_file=config_file,
                scan_id=scan_id,
                network=network,
                modality=modality,
                access_level=access_level,
                data_stage=data_stage,
                change_type=change_type,  # type: ignore
            )

            if delta_files is None:
                logger.debug(f"delta_files is None for {modality}, {change_type}")
                delta_files = 0

            delta_size = get_delta_files_size(
                config_file=config_file,
                scan_id=scan_id,
                network=network,
                modality=modality,
                access_level=access_level,
                data_stage=data_stage,
                change_type=change_type,  # type: ignore
            )

            if delta_size is None:
                logger.debug(f"delta_size is None for {modality}, {change_type}")
                delta_size = 0

            data_dict[change_type] = {
                "files": delta_files,
                "size": delta_size,
            }

        delta_files = data_dict["added"]["files"] - data_dict["deleted"]["files"]
        delta_size = data_dict["added"]["size"] - data_dict["deleted"]["size"]

        delta_files_str = humanize.intcomma(delta_files)
        delta_size_str = humanize.naturalsize(delta_size, binary=True)

        modified_files = data_dict["modified"]["files"]
        modified_size = data_dict["modified"]["size"]
        modified_files_str = humanize.intcomma(modified_files)
        modified_size_str = humanize.naturalsize(modified_size, binary=True)

        if delta_files == 0 and delta_size == 0:
            change_str = "0 files"
        else:
            change_str = f"{delta_files_str} files ({delta_size_str})"

        if modified_files == 0 and modified_size == 0:
            modified_str = ""
        else:
            modified_str = (
                f", {modified_files_str} modified files ({modified_size_str})"
            )

        bullet_list_elements.extend(
            [
                {
                    "type": "rich_text_section",
                    "elements": [
                        {
                            "type": "text",
                            "text": f"{modality} - {change_str}{modified_str} {qualifier}",
                        }
                    ],
                }
            ]
        )

        bullet_list_block["elements"] = bullet_list_elements
        # Add bullet list to network_section_elements's elements
        network_section_elements.append(bullet_list_block)

        if delta_files == 0 and modified_files == 0:
            issue_detected_modalities.append(modality)

        modality_sub_types: List[Tuple[str, Tuple[str, str]]] = []
        if modality == "phone":
            modality_sub_types = [
                ("sensor", ("mindlamp_type", "sensor")),
                ("activity", ("mindlamp_type", "activity")),
            ]
        elif modality == "surveys":
            modality_sub_types = [
                ("UPENN", ("redcap_instance", "UPENN")),
                ("MGB", ("redcap_instance", "MGB-Prescient")),
            ]

        for sub_type in modality_sub_types:
            sub_type_name, sub_type_info = sub_type
            sub_type_key, sub_type_value = sub_type_info
            extra_metadata = {sub_type_key: sub_type_value}
            bullet_list_block = {
                "type": "rich_text_list",
                "indent": 1,
                "style": "bullet",
            }
            bullet_list_elements: List[Dict[str, Any]] = []

            data_dict: Dict[str, Any] = {}
            for change_type in ["added", "deleted", "modified"]:
                delta_files = get_delta_files_count(
                    config_file=config_file,
                    scan_id=scan_id,
                    network=network,
                    modality=modality,
                    access_level=access_level,
                    data_stage=data_stage,
                    change_type=change_type,  # type: ignore
                    extra_metadata=extra_metadata,
                )

                if delta_files is None:
                    logger.debug(
                        f"delta_files is None for {modality}, {change_type}, {extra_metadata}"
                    )
                    delta_files = 0

                delta_size = get_delta_files_size(
                    config_file=config_file,
                    scan_id=scan_id,
                    network=network,
                    modality=modality,
                    access_level=access_level,
                    data_stage=data_stage,
                    change_type=change_type,  # type: ignore
                    extra_metadata=extra_metadata,
                )

                if delta_size is None:
                    logger.debug(
                        f"delta_size is None for {modality}, {change_type}, {extra_metadata}"
                    )
                    delta_size = 0

                data_dict[change_type] = {
                    "files": delta_files,
                    "size": delta_size,
                }

            delta_files = data_dict["added"]["files"] - data_dict["deleted"]["files"]
            delta_size = data_dict["added"]["size"] - data_dict["deleted"]["size"]

            delta_files_str = humanize.intcomma(delta_files)
            delta_size_str = humanize.naturalsize(delta_size, binary=True)

            modified_files = data_dict["modified"]["files"]
            modified_size = data_dict["modified"]["size"]

            modified_files_str = humanize.intcomma(modified_files)
            modified_size_str = humanize.naturalsize(modified_size, binary=True)

            if delta_files == 0 and delta_size == 0:
                change_str = "0 files"
            else:
                change_str = f"{delta_files_str} files ({delta_size_str})"

            if modified_files == 0 and modified_size == 0:
                modified_str = ""
            else:
                modified_str = (
                    f", {modified_files_str} modified files ({modified_size_str})"
                )

            bullet_list_elements.extend(
                [
                    {
                        "type": "rich_text_section",
                        "elements": [
                            {
                                "type": "text",
                                "text": f"{sub_type_name} - {change_str}{modified_str} {qualifier}",
                            }
                        ],
                    }
                ]
            )

            bullet_list_block["elements"] = bullet_list_elements
            # Add bullet list to network_section_elements's elements
            network_section_elements.append(bullet_list_block)

    network_section = {
        "type": "rich_text",
        "elements": network_section_elements,
    }
    network_sections.append(network_section)

    blocks.append(header)
    blocks.append(divider)
    blocks.extend(network_sections)
    blocks.append(divider)

    if issue_detected_modalities:
        logger.warning(
            f"Potential data-flow issue detected: {issue_detected_modalities}"
        )
        issue_warning = {
            "type": "context",
            "elements": [
                {
                    "type": "image",
                    "image_url": "https://cdn-icons-png.flaticon.com/128/4539/4539472.png",
                    "alt_text": "notifications warning icon",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Potential Data-Flow issue detected* {', '.join(issue_detected_modalities)}",
                },
            ],
        }
        blocks.append(issue_warning)

    info_block = {
        "type": "context",
        "elements": [
            {
                "type": "image",
                "image_url": "https://cdn-icons-png.flaticon.com/128/8692/8692942.png",
                "alt_text": "information icon",
            },
            {"type": "mrkdwn", "text": "Only includes *PROTECTED* and *raw* files"},
        ],
    }

    blocks.append(info_block)

    payload_body = {
        "blocks": blocks,
    }

    return payload_body


def send_slack_notification(config_file: Path, dry_run: bool = False) -> None:
    """
    Sends a Slack notification about the daily volume of data transferred.

    Args:
        config_file (Path): The path to the configuration file.
        dry_run (bool): Whether to run in dry-run mode.
            Does not send the notification if True.

    Returns:
        None
    """

    slack_webhook_url = get_slack_webhook_url(config_file=config_file)

    data_roots = [
        "/data/predict1/data_from_nda/Prescient",
        "/data/predict1/data_from_nda/Pronet",
    ]

    for data_root in data_roots:
        slack_payload = construct_slack_blockkit_json(
            config_file=config_file,
            data_root=Path(data_root),
        )

        if dry_run:
            logger.info("Dry-run mode enabled. Skipping Slack notification.")
            logger.debug(f"Payload: {json.dumps(slack_payload, indent=4)}")
            continue

        if slack_payload is None:
            logger.error("Failed to construct Slack payload. Skipping notification.")
            continue

        response = requests.post(
            slack_webhook_url,
            json=slack_payload,
            headers={"Content-type": "application/json"},
            timeout=30,
        )

        if response.status_code == 200:
            logger.info("Slack notification sent successfully.")
        else:
            logger.error(f"Failed to send Slack notification: [{response.status_code}]")
            logger.error(response.text)
            logger.debug(f"Payload: {json.dumps(slack_payload, indent=4)}")

    return


if __name__ == "__main__":
    config_file = utils.get_config_file_path()
    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    console.rule(f"[bold red]{MODULE_NAME}")
    logger.info(f"Using config file: {config_file}")

    send_slack_notification(config_file=config_file, dry_run=False)

    logger.info("Done.")
