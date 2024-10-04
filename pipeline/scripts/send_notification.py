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
from typing import Any, Dict, List

import humanize
import requests
from rich.logging import RichHandler

from pipeline import data
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


def get_most_recent_statistics_timestamp(config_file: Path) -> datetime:
    """
    Retrieves the most recent timestamp from the statistics table.

    Args:
        config_file (Path): The path to the configuration file.

    Returns:
        datetime: The most recent timestamp.
    """

    query = """
        SELECT MAX(statistics_timestamp)
        FROM volume_statistics
    """

    last_update = db.fetch_record(
        config_file=config_file,
        query=query,
    )

    last_update = datetime.fromisoformat(last_update)
    return last_update


def get_earlier_statistics_timestamp(
    config_file: Path, threshold_date: datetime, offset: int = 0
) -> datetime:
    """
    Returns the last timestamp before the threshold date.

    Args:
        config_file (Path): The path to the configuration file.
        threshold_date (datetime): The threshold date.

    Returns:
        datetime: The last timestamp before the threshold date.
    """

    previous_update_query = f"""
        SELECT DISTINCT statistics_timestamp
        FROM volume_statistics
        WHERE statistics_timestamp < '{threshold_date}'
        ORDER BY statistics_timestamp DESC
        LIMIT 1 OFFSET {offset}
    """

    previous_update = db.fetch_record(
        config_file=config_file,
        query=previous_update_query,
    )

    previous_update = datetime.fromisoformat(previous_update)
    return previous_update


def construct_slack_blockkit_json(
    config_file: Path, latest_timestamp: datetime, previous_timestamp: datetime
) -> Dict[str, Any]:
    """
    Returns the Slack BlockKit JSON.

    Args:
        config_file (Path): The path to the configuration file.
        latest_timestamp (datetime): The latest timestamp.
        previous_timestamp (datetime): The previous timestamp.

    Returns:
        Dict[str, Any]: The Slack BlockKit JSON.
    """

    modalities = data.get_all_modalities(config_file=config_file)
    blocks = []

    header = {
        "type": "section",
        "text": {
            "text": f"Comparing {get_slack_formatted_date(latest_timestamp)} to \
{get_slack_formatted_date(previous_timestamp)}.",
            "type": "mrkdwn",
        },
    }

    divider = {"type": "divider"}

    networks = ["ProNET", "PRESCIENT"]
    network_sections = []
    issue_detected = False

    for network in networks:
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

        bullet_list_elements: List[Dict[str, Any]] = []
        for modality in modalities:
            is_protected: bool = True
            is_raw: bool = True
            qualifier: str = ""

            if modality == "interviews":
                is_protected = False
                is_raw = False
                qualifier = "- (GENERAL, processed)"

            files_sum_query_current = f"""
            SELECT SUM(files_count) as number_of_files
            FROM volume_statistics
            LEFT JOIN subjects USING (subject_id, study_id)
            LEFT JOIN study USING (study_id)
            WHERE statistics_timestamp = '{latest_timestamp}' AND
                modality = '{modality}' AND
                study.network_id = '{network}' AND
                is_raw is {is_raw} AND
                is_protected is {is_protected}
            """

            files_sum_current = db.fetch_record(
                config_file=config_file,
                query=files_sum_query_current,
            )

            size_sum_query_current = f"""
            SELECT SUM(files_size_mb) as files_size_mb
            FROM volume_statistics
            LEFT JOIN subjects USING (subject_id, study_id)
            LEFT JOIN study USING (study_id)
            WHERE statistics_timestamp = '{latest_timestamp}' AND
                modality = '{modality}' AND
                study.network_id = '{network}' AND
                is_raw is {is_raw} AND
                is_protected is {is_protected}
            """

            size_sum_current = db.fetch_record(
                config_file=config_file, query=size_sum_query_current
            )

            files_sum_query_previous = f"""
            SELECT SUM(files_count) as number_of_files
            FROM volume_statistics
            LEFT JOIN subjects USING (subject_id, study_id)
            LEFT JOIN study USING (study_id)
            WHERE statistics_timestamp = '{previous_timestamp}' AND
                modality = '{modality}' AND
                study.network_id = '{network}' AND
                is_raw is {is_raw} AND
                is_protected is {is_protected}
            """

            files_sum_previous = db.fetch_record(
                config_file=config_file,
                query=files_sum_query_previous,
            )

            size_sum_query_previous = f"""
            SELECT SUM(files_size_mb) as files_size_mb
            FROM volume_statistics
            LEFT JOIN subjects USING (subject_id, study_id)
            LEFT JOIN study USING (study_id)
            WHERE statistics_timestamp = '{previous_timestamp}' AND
                modality = '{modality}' AND
                study.network_id = '{network}' AND
                is_raw is {is_raw} AND
                is_protected is {is_protected}
            """

            size_sum_previous = db.fetch_record(
                config_file=config_file, query=size_sum_query_previous
            )

            delta_files = int(files_sum_current) - int(files_sum_previous)
            delta_size = float(size_sum_current) - float(size_sum_previous)

            delta_files_str = humanize.intcomma(delta_files)
            delta_size_str = humanize.naturalsize(delta_size * 1024 * 1024, binary=True)

            bullet_list_elements.extend(
                [
                    {
                        "type": "rich_text_section",
                        "elements": [
                            {
                                "type": "text",
                                "text": f"{modality} - {delta_files_str} files ({delta_size_str}) {qualifier}",
                            }
                        ],
                    }
                ]
            )

            if delta_size == 0 or delta_files == 0:
                issue_detected = True

        bullet_list_block = {
            "type": "rich_text_list",
            "style": "bullet",
            "elements": bullet_list_elements,
        }

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

    if issue_detected:
        logger.warning("Potential data-flow issue detected.")
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
                    "text": "*Potential Data-Flow issue detected!*",
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

    latest_timestamp = get_most_recent_statistics_timestamp(config_file=config_file)
    logger.info(f"Latest timestamp: {latest_timestamp}")

    previous_timestamp = get_earlier_statistics_timestamp(
        config_file=config_file, threshold_date=latest_timestamp
    )
    logger.info(f"Previous timestamp: {previous_timestamp}")

    slack_webhook_url = get_slack_webhook_url(config_file=config_file)

    slack_payload = construct_slack_blockkit_json(
        config_file=config_file,
        latest_timestamp=latest_timestamp,
        previous_timestamp=previous_timestamp,
    )

    if dry_run:
        logger.info("Dry-run mode enabled. Skipping Slack notification.")
        logger.debug(f"Payload: {json.dumps(slack_payload, indent=4)}")
        return

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

    send_slack_notification(config_file=config_file)

    logger.info("Done.")
