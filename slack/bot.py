#!/usr/bin/env python
"""
A simple Slack bot that listens for commands and responds with an image plot.

Usage:
- /ping: Responds with "pong"
- /plot [modality] [count/size] [network] [num_days]: Responds with a plot
    of the data flow for the given modality

Example:
- /plot actigraphy count ProNET 14
- /plot actigraphy size ProNET 14

The bot listens for app mentions and logs them.
"""

import sys
from pathlib import Path

file = Path(__file__)
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
from typing import Optional
import socket

import matplotlib
import pandas as pd
from rich.logging import RichHandler

matplotlib.use("agg")
from matplotlib import pyplot as plt
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

from pipeline.helpers import db, utils
from pipeline.helpers.config import config

MODULE_NAME = "slack_bot"

logger = logging.getLogger(MODULE_NAME)
logargs = {
    "level": logging.DEBUG,
    # "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s",
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)

console = utils.get_console()


def get_bot_token(config_file: Path) -> str:
    """
    Returns the bot token from the config file.

    Args:
    - config_file (Path): Path to the config file

    Returns:
    - bot_token (str): Bot token
    """
    config_params = config(path=config_file, section="slack")
    bot_token = config_params["bot_token"]

    return bot_token


def get_app_token(config_file: Path) -> str:
    """
    Returns the app token from the config file.

    Args:
    - config_file (Path): Path to the config file

    Returns:
    - app_token (str): App token
    """
    config_params = config(path=config_file, section="slack")
    app_token = config_params["app_token"]

    return app_token


config_file = utils.get_config_file_path()
console.print(f"Using config file: {config_file}")
app_token = get_app_token(config_file)
bot_token = get_bot_token(config_file)

app = App(token=bot_token)


@app.command("/ping")
def ping_function(ack, respond, command) -> None:
    """
    Echoes "pong" with the text from the command.

    Args:
    - body: Request body
    - ack: Acknowledge the request
    - respond: Respond to the request
    - command: Command details

    Returns:
        None
    """
    ack()
    response = f"pong[@{socket.gethostname()}]: {command['text']}"
    logger.debug(f'{command["user_id"]} - {command["text"]} - {response}')
    respond(response)


modality_colors_map = {
    "actigraphy": "skyblue",
    "surveys": "lightcoral",
    "mri": "lightblue",
    "eeg": "lightgreen",
    "phone": "lightpink",
    "interviews": "red",
}


def plot_modality_count(
    modality: str,
    config_file: Path,
    network_id: Optional[str] = None,
    num_days: int = 14,
) -> Path:
    """
    Creates a plot of the # of files data flow for the given modality, network, and number of days.

    Args:
    - modality (str): Modality
    - config_file (Path): Path to the config file
    - network_id (Optional[str]): Network ID
    - num_days (int): Number of days

    Returns:
    - plot_file (Path): Path to the saved plot
    """
    current_date = datetime.now().date()
    start_date = current_date - pd.Timedelta(days=num_days)

    is_raw = True
    is_protected = True

    if modality == "interviews":
        is_raw = False
        is_protected = False

    if network_id is None:
        data_query = f"""
        SELECT modality, statistics_timestamp,
            SUM(files_count) as files_count,
            SUM(files_size_mb) as files_size_mb
        FROM volume_statistics
        left join study using (study_id)
        WHERE modality = '{modality}' AND
            is_raw is {is_raw} AND
            is_protected is {is_protected} AND
            statistics_timestamp >= '{start_date}'
        group by statistics_timestamp, modality
        order by statistics_timestamp ASC
        """
    else:
        data_query = f"""
        SELECT network_id, modality, statistics_timestamp,
            SUM(files_count) as files_count,
            SUM(files_size_mb) as files_size_mb
        FROM volume_statistics
        left join study using (study_id)
        WHERE modality = '{modality}' AND
            is_raw is {is_raw} AND
            is_protected is {is_protected} AND
            statistics_timestamp >= '{start_date}'
        group by statistics_timestamp, modality, network_id
        order by statistics_timestamp ASC
        """

    data_df = db.execute_sql(
        config_file=config_file,
        query=data_query,
        db="postgresql",
    )

    if network_id is not None:
        network_df = data_df[data_df["network_id"] == network_id]
    else:
        network_id = "AllNetworks"
        network_df = data_df
    # truncate to the start of the day
    network_df["statistics_timestamp"] = pd.to_datetime(
        network_df["statistics_timestamp"]
    ).dt.floor("d")
    network_df["files_count_diff"] = network_df["files_count"].diff()

    try:
        modality_color = modality_colors_map[modality]
    except KeyError:
        raise ValueError(  # pylint: disable=raise-missing-from
            f"Invalid modality: {modality}. Available modalities: {modality_colors_map.keys()}"
        )

    plt.figure(figsize=(10, 7))

    plt.bar(
        network_df["statistics_timestamp"],
        network_df["files_count_diff"],
        color=modality_color,
        label=modality.upper(),
    )

    # draw rectangles over weekends
    for _, row in network_df.iterrows():
        if row["statistics_timestamp"].weekday() == 5:
            weekend_start = row["statistics_timestamp"]
            weekend_end = row["statistics_timestamp"] + pd.Timedelta(days=2)

            # truncate to the start of the day
            weekend_start = pd.Timestamp(weekend_start.date()) - pd.Timedelta(hours=12)
            weekend_end = pd.Timestamp(weekend_end.date()) - pd.Timedelta(hours=12)

            plt.axvspan(
                weekend_start,
                weekend_end,
                color="lightgrey",
                alpha=0.5,
            )

            # write weekend start day
            plt.text(
                weekend_start + pd.Timedelta(days=1),
                0.05 * network_df["files_count_diff"].max(),
                "Weekend",
                rotation=90,
                verticalalignment="bottom",
                horizontalalignment="center",
                color="black",
            )

    plt.title(f"{modality.upper()} Data flow for {network_id}")
    plt.xlabel("Time")
    plt.ylabel("Files count")

    # Remove year from x-axis
    plt.gca().xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter("%b %d"))

    # save the plot
    plot_file = Path(f"/tmp/{modality}_data_flow_{network_id}_count.png")
    plt.savefig(plot_file)

    logger.debug(f"count: Saved plot to {plot_file}")

    return plot_file


def plot_modality_size(
    modality: str,
    config_file: Path,
    network_id: Optional[str] = None,
    num_days: int = 14,
) -> Path:
    """
    Plots the size of the files data flow for the given modality, network, and number of days.

    Args:
    - modality (str): Modality
    - config_file (Path): Path to the config file
    - network_id (Optional[str]): Network ID

    Returns:
    - plot_file (Path): Path to the saved plot
    """
    current_date = datetime.now().date()
    start_date = current_date - pd.Timedelta(days=num_days)

    is_raw = True
    is_protected = True

    if modality == "interviews":
        is_raw = False
        is_protected = False

    if network_id is None:
        data_query = f"""
        SELECT modality, statistics_timestamp,
            SUM(files_count) as files_count,
            SUM(files_size_mb) as files_size_mb
        FROM volume_statistics
        left join study using (study_id)
        WHERE modality = '{modality}' AND
            is_raw is {is_raw} AND
            is_protected is {is_protected} AND
            statistics_timestamp >= '{start_date}'
        group by statistics_timestamp, modality
        order by statistics_timestamp ASC
        """
    else:
        data_query = f"""
        SELECT network_id, modality, statistics_timestamp,
            SUM(files_count) as files_count,
            SUM(files_size_mb) as files_size_mb
        FROM volume_statistics
        left join study using (study_id)
        WHERE modality = '{modality}' AND
            is_raw is {is_raw} AND
            is_protected is {is_protected} AND
            statistics_timestamp >= '{start_date}'
        group by statistics_timestamp, modality, network_id
        order by statistics_timestamp ASC
        """

    data_df = db.execute_sql(config_file=config_file, query=data_query, db="postgresql")

    if network_id is not None:
        network_df = data_df[data_df["network_id"] == network_id]
    else:
        network_id = "AllNetworks"
        network_df = data_df
    # truncate to the start of the day
    network_df["statistics_timestamp"] = pd.to_datetime(
        network_df["statistics_timestamp"]
    ).dt.floor("d")
    network_df["files_size_mb_diff"] = network_df["files_size_mb"].diff()

    plt.figure(figsize=(10, 7))

    try:
        modality_color = modality_colors_map[modality]
    except KeyError:
        raise ValueError(  # pylint: disable=raise-missing-from
            f"Invalid modality: {modality}. Available modalities: {modality_colors_map.keys()}"
        )

    plt.bar(
        network_df["statistics_timestamp"],
        network_df["files_size_mb_diff"],
        color=modality_color,
        label=modality.upper(),
    )

    # draw rectangles over weekends
    for _, row in network_df.iterrows():
        if row["statistics_timestamp"].weekday() == 5:
            weekend_start = row["statistics_timestamp"]
            weekend_end = row["statistics_timestamp"] + pd.Timedelta(days=2)

            # truncate to the start of the day
            weekend_start = pd.Timestamp(weekend_start.date()) - pd.Timedelta(hours=12)
            weekend_end = pd.Timestamp(weekend_end.date()) - pd.Timedelta(hours=12)

            plt.axvspan(
                weekend_start,
                weekend_end,
                color="lightgrey",
                alpha=0.5,
            )

            # write weekend start day
            plt.text(
                weekend_start + pd.Timedelta(days=1),
                0.05 * network_df["files_size_mb_diff"].max(),
                "Weekend",
                rotation=90,
                verticalalignment="bottom",
                horizontalalignment="center",
                color="black",
            )

    plt.title(f"{modality.upper()} Data flow for {network_id}")
    plt.xlabel("Time")
    plt.ylabel("Files size (MB)")
    plt.ylim(0, 1.1 * network_df["files_size_mb_diff"].max())

    # Remove year from x-axis
    plt.gca().xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter("%b %d"))

    # save the plot
    plot_file = Path(f"/tmp/{modality}_data_flow_{network_id}_size.png")
    plt.savefig(plot_file)

    logger.debug(f"size: Saved plot to {plot_file}")

    return plot_file


@app.command("/plot")
def plot_modality_wrapper(ack, respond, command):
    """
    Slash command wrapper for plotting the data flow for the given modality.

    Usage:
    - /plot [modality] [count/size] [network] [num_days]
      - modality: Modality (actigraphy, surveys, mri, eeg, phone, interviews)
        - count/size: Count or size of files
        - network: Network ID (ProNET, PRESCIENT) [Optional] - Default: AllNetworks
        - num_days: Number of days to plot [Optional] - Default: 14

    Args:
    - ack: Acknowledge the request
    - respond: Respond to the request
    - command: Command details

    Returns:
    - None
    """
    ack()

    user_id = command["user_id"]

    # parse the command:
    # /plot [modality] [count/size]
    parts = command["text"].split()
    try:
        modality = parts[0]
        count_size = parts[1]
        network = parts[2]
        num_days = int(parts[3])
    except IndexError:
        if len(parts) < 2:
            logger.error(f"{user_id} - {command['text']} - Invalid command")
            respond("Usage: /plot [modality] [count/size] [network] [num_days]")
            return
        if len(parts) == 2:
            network = None
            num_days = 14
        elif len(parts) == 3:
            num_days = 14

    logger.info(f"{user_id} - {command['text']}")

    # Check if network in [ProNET. PRESCIENT]
    if network == "pronet":
        network = "ProNET"
    elif network == "prescient":
        network = "PRESCIENT"

    if count_size not in ["count", "size"]:
        respond("Usage: /plot [modality] [count/size] [network] [num_days]")
        return

    try:
        if count_size == "count":
            plot_file = plot_modality_count(
                modality=modality,
                config_file=config_file,
                network_id=network,
                num_days=num_days,
            )
        elif count_size == "size":
            plot_file = plot_modality_size(
                modality=modality,
                config_file=config_file,
                network_id=network,
                num_days=num_days,
            )
        else:
            respond("Usage: /plot [modality] [count/size] [network] [num_days]")
            return
    except ValueError as e:
        logger.error(e)
        respond(f"Error: {e}")
        return

    # Upload the file
    with open(plot_file, "rb") as f:
        _ = app.client.files_upload_v2(
            file=f,
            channel=command["channel_id"],
            user=command["user_id"],
            initial_comment=f"Plot for {modality} data flow {count_size} \
- {network} {num_days} days",
            title=f"{modality}_{count_size}_{network}_{num_days}days.png",
            alt_txt=f"{modality} data flow {count_size} visualization - {network} {num_days} days",
        )

    respond(f"Uploaded plot for {modality} data flow")
    logger.debug(
        f"{user_id} - Uploaded plot for {modality} data flow {count_size} \
- {network} {num_days} days"
    )


@app.event("app_mention")
def handle_app_mention(body):
    """
    Event handler for app mentions.

    Args:
    - body: Request body
    - ack: Acknowledge the request
    - say: Respond to the request

    Returns:
    - None
    """
    user_id = body["event"]["user"]
    channel_id = body["event"]["channel"]
    text = body["event"]["text"]

    logger.debug(f"Received app mention from {user_id} in {channel_id}: {text}")


def get_consented_count(
    config_file: Path,
    cohort: str,
    network_id: str,
) -> int:
    """
    Returns the consented count for the given cohort and network.

    Args:
    - config_file (Path): Path to the config file
    - cohort (str): Cohort
    - network (str): Network

    Returns:
    - recruitment_count (int): Recruitment count
    """
    query = f"""
    SELECT COUNT(*) AS count
    FROM
        (SELECT forms_derived.recruitment_status.*,
                site_id,
                site_name,
                site_country,
                network_id,
                site_country_code,
                cohort
        FROM forms_derived.recruitment_status
        INNER JOIN subjects ON recruitment_status.subject_id = subjects.id
        INNER JOIN site ON subjects.site_id = site.id
        INNER JOIN forms_derived.filters ON forms_derived.recruitment_status.subject_id = forms_derived.filters.subject) AS virtual_table
    WHERE network_id IN ('{network_id}')
        AND cohort IN ('{cohort}');
    """

    consented_count = db.fetch_record(
        config_file=config_file,
        query=query,
        db="formsdb",
    )

    return consented_count


def get_recruitment_count(
    config_file: Path,
    cohort: str,
    network_id: str,
) -> int:
    """
    Returns the recruitment count for the given cohort and network.

    Args:
    - config_file (Path): Path to the config file
    - cohort (str): Cohort
    - network (str): Network

    Returns:
    - recruitment_count (int): Recruitment count
    """
    query = f"""
    SELECT COUNT(*) AS count
    FROM
        (SELECT forms_derived.recruitment_status.*,
                site_id,
                site_name,
                site_country,
                network_id,
                site_country_code,
                cohort
        FROM forms_derived.recruitment_status
        INNER JOIN subjects ON recruitment_status.subject_id = subjects.id
        INNER JOIN site ON subjects.site_id = site.id
        INNER JOIN forms_derived.filters ON forms_derived.recruitment_status.subject_id = forms_derived.filters.subject) AS virtual_table
    WHERE network_id IN ('{network_id}')
        AND cohort IN ('{cohort}')
        AND recruitment_status = 'recruited';
    """

    recruitment_count = db.fetch_record(
        config_file=config_file,
        query=query,
        db="formsdb",
    )

    return recruitment_count


@app.command("/recruitment")
def post_recruitment_numbers(ack, respond, command):
    """
    Slash command wrapper for plotting the data flow for the given modality.

    Usage:
    - /recruitment

    Args:
    - ack: Acknowledge the request
    - respond: Respond to the request
    - command: Command details

    Returns:
    - None
    """
    ack()

    user_id = command["user_id"]

    blocks = []

    # Date Format: August 20, 2024
    header = {
        "type": "section",
        "text": {
            "text": f"AMPSCZ Recruitment Status as of {datetime.now().date().strftime('%B %d, %Y')}",
            "type": "mrkdwn",
        },
    }

    divider = {"type": "divider"}

    blocks.append(header)
    blocks.append(divider)

    networks = ["ProNET", "PRESCIENT"]
    cohorts = ["CHR", "HC"]

    for network in networks:
        network_elemnts = []
        network_header = {
            "type": "rich_text_section",
            "elements": [{"type": "text", "text": network}],
        }
        network_elemnts.append(network_header)

        for cohort in cohorts:
            list_header = {
                "type": "rich_text_list",
                "style": "bullet",
                "indent": 0,
                "border": 0,
                "elements": [
                    {
                        "type": "rich_text_section",
                        "elements": [{"type": "text", "text": cohort}],
                    }
                ],
            }
            consented_count = get_consented_count(
                config_file=config_file, cohort=cohort, network_id=network
            )
            recruitment_count = get_recruitment_count(
                config_file=config_file, cohort=cohort, network_id=network
            )

            info_block = {
                "type": "rich_text_list",
                "style": "bullet",
                "indent": 1,
                "border": 0,
                "elements": [
                    {
                        "type": "rich_text_section",
                        "elements": [
                            {"type": "text", "text": f"Consented: {consented_count}"}
                        ],
                    },
                    {
                        "type": "rich_text_section",
                        "elements": [
                            {"type": "text", "text": f"Recruited: {recruitment_count}"}
                        ],
                    },
                ],
            }

            network_elemnts.append(list_header)
            network_elemnts.append(info_block)

        blocks.append({"type": "rich_text", "elements": network_elemnts})
        blocks.append(divider)

    logger.debug(f"{user_id} - {command['text']}")

    payload = {
        "blocks": blocks,
    }
    respond(payload)


if __name__ == "__main__":
    handler = SocketModeHandler(app, app_token)
    handler.start()
