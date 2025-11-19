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
from typing import List, Optional, Dict, Any, Tuple, Literal
import socket
import requests
import base64

import matplotlib
import pandas as pd
from rich.logging import RichHandler
from adjustText import adjust_text

matplotlib.use("agg")
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import matplotlib.dates as mdates
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

from pipeline.helpers import db, utils
from pipeline import data, constants
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

noisy_moduled = [
    "matplotlib.font_manager",
    "PIL.PngImagePlugin",
]
for noisy_module in noisy_moduled:
    logger.debug(f"Setting logging level for {noisy_module} to WARNING")
    noisy_logger = logging.getLogger(noisy_module)
    noisy_logger.setLevel(logging.WARNING)  # Set to WARNING to reduce noise


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


def upload_file(file_path: Path, config_file: Path) -> str:
    """
    Uploads a file to imgbb.

    Args:
    - file_path (Path): Path to the file to upload
    - config_file (Path): Path to the config file

    Returns:
    - file_url (str): URL of the uploaded file
    """
    config_params = config(path=config_file, section="imgbb")
    imgbb_api_key = config_params["imgbb_api_key"]
    imgbb_api_url = config_params["imgbb_api_url"]
    imgbb_expiration = config_params["imgbb_expiration"]

    if not imgbb_api_key or not imgbb_api_url:
        raise ValueError("imgbb API key or URL is not set in the config file.")

    with open(file_path, "rb") as file:
        file_content = file.read()

    encoded_file = base64.b64encode(file_content).decode("utf-8")

    payload = {
        "key": imgbb_api_key,
        "image": encoded_file,
        "expiration": imgbb_expiration,
    }

    response = requests.post(imgbb_api_url, data=payload)

    if response.status_code != 200:
        logger.error(f"Failed to upload file to imgbb: {response.text}")
        raise Exception("Failed to upload file to imgbb")

    response_data = response.json()

    public_url = response_data.get("data", {}).get("url")

    if not public_url:
        logger.error("Failed to get public URL from imgbb response.")
        raise Exception("Failed to get public URL from imgbb response")

    logger.debug(f"File uploaded to imgbb: {public_url}")
    return public_url


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

change_type_colors_map = {
    "added": "forestgreen",
    "modified": "orange",
    # 'deleted' is filtered out in the query, so it's not needed here
}


def plot_modality_data_flow(
    modality: str,
    metric: Literal["count", "size"],
    config_file: Path,
    network_id: Optional[str] = None,
    num_days: int = 14,
) -> Path:
    """
    Creates a plot of the data flow for a given modality, network, and number of days.
    The plot can show either the count of files or the total size of files.

    Args:
    - modality (str): The data modality (e.g., 'interviews').
    - metric (Literal["count", "size"]): The metric to plot ('count' or 'size').
    - config_file (Path): Path to the database configuration file.
    - network_id (Optional[str]): The network ID to filter by. If None, all networks are included.
    - num_days (int): The number of past days to include in the plot.

    Returns:
    - Path: The path to the saved plot image file.

    Raises:
    - ValueError: If an invalid metric is provided.
    """
    # --- 1. Configure metric-specific variables ---
    if metric == "count":
        metric_sql_agg = "COUNT(fm.file_path) AS files_count"
        metric_df_col = "files_count"
        metric_ylabel = "Daily Files Count"
    elif metric == "size":
        metric_sql_agg = "SUM(fc.new_size_bytes) / (1024 * 1024) AS files_size_mb"
        metric_df_col = "files_size_mb"
        metric_ylabel = "Daily Files Size (MB)"
    else:
        raise ValueError(f"Invalid metric '{metric}'. Choose from 'count' or 'size'.")

    # --- 2. Common Data Fetching and Preparation ---
    current_date = datetime.now().date()
    start_date = current_date - pd.Timedelta(days=num_days)

    data_stage = "raw"
    access_level = "PROTECTED"
    if modality == "interviews":
        data_stage = "processed"
        access_level = "GENERAL"

    # Use an f-string to inject the metric-specific SQL aggregation
    data_query = f"""
    SELECT
        fm.network_id,
        fm.modality,
        sr.started_at AS statistics_timestamp,
        fc.change_type,
        {metric_sql_agg}
    FROM
        phoenix.file_metadata AS fm
    JOIN
        filesystem.scan_runs AS sr ON fm.scan_id = sr.scan_id
    JOIN
        filesystem.file_changes AS fc ON fm.scan_id = fc.scan_id AND fm.file_path = fc.file_path
    WHERE
        fm.data_stage = '{data_stage}' AND
        fm.access_level = '{access_level}' AND
        fm.modality = '{modality}' AND
        sr.started_at >= '{start_date}' AND
        fc.change_type != 'deleted'
    GROUP BY
        fm.network_id,
        sr.started_at,
        fm.modality,
        fc.change_type
    ORDER BY
        sr.started_at ASC;
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

    if network_df.empty:
        logger.warning(
            f"No data found for modality '{modality}', network '{network_id}' in the last {num_days} days."
        )
        # Create an empty plot with a message if there's no data at all
        plt.figure(figsize=(12, 7))
        plt.text(
            0.5,
            0.5,
            "No data to display for the selected period.",
            horizontalalignment="center",
            verticalalignment="center",
            transform=plt.gca().transAxes,
        )
    else:
        # --- 3. Common Data Processing ---
        network_df["statistics_timestamp"] = pd.to_datetime(
            network_df["statistics_timestamp"]
        ).dt.floor("d")

        # Group by the dynamic metric column
        network_df_daily_changes = (
            network_df.groupby(["statistics_timestamp", "change_type"])[metric_df_col]
            .sum()
            .unstack(fill_value=0)
        )

    # Create a full date range to ensure all days are represented
    full_date_range = pd.date_range(
        start=start_date, end=current_date, freq="D", tz="UTC"
    )
    if "network_df_daily_changes" in locals():
        network_df_daily_changes = network_df_daily_changes.reindex(
            full_date_range, fill_value=0
        )
    else:  # Handle case where initial dataframe was empty
        network_df_daily_changes = pd.DataFrame(index=full_date_range)

    network_df_daily_changes.index.name = "statistics_timestamp"

    total_daily_values = network_df_daily_changes.sum(axis=1)
    max_total_daily_value = (
        total_daily_values.max() if not total_daily_values.empty else 1
    )

    # --- 4. Common Plotting Logic ---
    plt.figure(figsize=(12, 7))

    ordered_change_types = ["added", "modified"]
    change_types_to_plot = [
        ct for ct in ordered_change_types if ct in network_df_daily_changes.columns
    ]

    if not change_types_to_plot:
        plt.text(
            0.5,
            0.5,
            "No file change data to display.",
            horizontalalignment="center",
            verticalalignment="center",
            transform=plt.gca().transAxes,
        )
    else:
        bottom_values = pd.Series(0, index=network_df_daily_changes.index)
        for change_type in change_types_to_plot:
            values = network_df_daily_changes[change_type]
            color = change_type_colors_map.get(change_type, "grey")
            plt.bar(
                network_df_daily_changes.index,
                values,
                bottom=bottom_values,
                color=color,
                label=f"{change_type.capitalize()} Files",
                width=0.8,
            )
            bottom_values += values
        plt.legend(title="Change Type")

    # Draw rectangles over weekends
    for single_date in full_date_range:
        if single_date.weekday() >= 5:  # Saturday or Sunday
            plt.axvspan(
                single_date,
                single_date + pd.Timedelta(days=1),
                color="lightgrey",
                alpha=0.5,
                zorder=0,
            )
            if (
                single_date.weekday() == 5 and max_total_daily_value > 0
            ):  # Place text on Saturday
                plt.text(
                    single_date + pd.Timedelta(days=0.5),
                    0.05 * max_total_daily_value,
                    "Weekend",
                    rotation=90,
                    va="bottom",
                    ha="center",
                    color="black",
                    fontsize=8,
                )

    # --- 5. Dynamic Plot Customization and Saving ---
    plt.title(
        f"{modality.upper()} Daily Data Flow ({metric.capitalize()}) for {network_id}"
    )
    plt.xlabel("Date")
    plt.ylabel(metric_ylabel)

    plt.xlim(
        pd.Timestamp(start_date) - pd.Timedelta(days=0.5),
        pd.Timestamp(current_date) + pd.Timedelta(days=0.5),
    )
    plt.ylim(
        bottom=0, top=max_total_daily_value * 1.15 if max_total_daily_value > 0 else 1
    )  # Consistent y-axis scaling

    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator())
    plt.gcf().autofmt_xdate()
    plt.grid(axis="y", linestyle="--", alpha=0.7)

    # Save the plot with a dynamic filename
    plot_file = Path(f"/tmp/{modality}_data_flow_{network_id}_{metric}.png")
    plt.savefig(plot_file, bbox_inches="tight")
    plt.close()  # Free up memory

    logger.debug(f"{metric}: Saved plot to {plot_file}")

    return plot_file


def generate_master_plot(
    modalities: List[str],
    networks: List[str],
    config_file: Path,
    num_days: int = 14,
) -> Path:
    """
    Generates a master plot with subplots for all modalities and networks.

    Args:
    - modalities (List[str]): List of modalities to plot.
    - networks (List[str]): List of networks to plot.
    - config_file (Path): Path to the configuration file.
    - num_days (int): Number of days to include in the plot.

    Returns:
    - Path: Path to the saved master plot image file.
    """
    num_rows = len(modalities)
    num_cols = len(networks)

    # Create a figure and a grid of subplots
    # `squeeze=False` ensures `axes` is always a 2D array, even if rows/cols is 1.
    fig, axes = plt.subplots(
        nrows=num_rows,
        ncols=num_cols,
        figsize=(num_cols * 5, num_rows * 4),
        squeeze=False,
    )

    temp_plot_paths = []
    # Iterate over each modality and network to fill the grid
    for row_idx, modality in enumerate(modalities):
        for col_idx, network in enumerate(networks):
            logger.info(f"Generating plot for: {network} - {modality}")

            # 1. Generate the individual plot file
            plot_file_path = plot_modality_data_flow(
                modality=modality,
                metric="size",
                config_file=config_file,
                network_id=network,
                num_days=num_days,
            )
            temp_plot_paths.append(plot_file_path)

            # 2. Load the generated image
            img = mpimg.imread(plot_file_path)

            # 3. Display the image on the corresponding subplot
            ax = axes[row_idx, col_idx]
            ax.imshow(img)
            ax.axis("off")  # Hide axes ticks and labels for the image subplot

    # Set row titles using y-labels on the first column
    for row_idx, modality in enumerate(modalities):
        axes[row_idx, 0].set_ylabel(
            modality.capitalize(),
            fontsize=16,
            rotation=90,
            labelpad=20,
            va="center",  # Vertically align the label
        )

    # Adjust layout to prevent titles/labels from overlapping
    fig.tight_layout()

    # Save the final master plot
    output_path = Path("master_data_flow_plot.png")
    fig.savefig(output_path, dpi=300)
    logger.debug(f"\nMaster plot saved to: {output_path}")

    plt.close(fig)  # Final cleanup of master figure
    return output_path


def plot_forms_per_timepoint(
    subject_id: str,
    config_file: Path,
) -> Path:
    """
    Plot the number of forms per timepoint for a given subject.

    Args:
    - subject_id (str): Subject ID
    - config_file (Path): Path to the config file

    Returns:
    - plot_file (Path): Path to the saved plot
    """

    def get_subject_withdrawn_date(
        subject_id: str,
        config_file: str,
    ) -> Optional[datetime]:
        withdrawn_date_query = f"""
        SELECT 
            removed_date
        FROM 
            forms_derived.subject_removed
        WHERE 
            subject_id = '{subject_id}' AND
            removed = TRUE;
        """

        withdrawn_date = db.fetch_record(
            config_file=config_file,
            query=withdrawn_date_query,
            db="formsdb",
        )

        if withdrawn_date:
            withdrawn_date_dt = datetime.strptime(withdrawn_date, "%Y-%m-%d %H:%M:%S")
            # Replace the time part with 00:00:00
            withdrawn_date_dt = withdrawn_date_dt.replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            return withdrawn_date_dt

        return None

    def get_subject_converted_date(
        subject_id: str, config_file: str
    ) -> Optional[datetime]:
        converted_date_query = f"""
        SELECT 
            conversion_date
        FROM 
            forms_derived.conversion_status
        WHERE 
            subject_id = '{subject_id}' AND
            converted = TRUE;
        """

        converted_date = db.fetch_record(
            config_file=config_file,
            query=converted_date_query,
            db="formsdb",
        )

        if converted_date:
            converted_date_dt = datetime.strptime(converted_date, "%Y-%m-%d %H:%M:%S")
            # Replace Time part with 00:00:00
            converted_date_dt = converted_date_dt.replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            return converted_date_dt

        return None

    def get_subject_consent_dates(
        subject_id: str, config_file: Path
    ) -> Optional[datetime]:
        query = f"""
        SELECT form_data ->> 'chric_consent_date' as consent_date
        FROM forms.forms
        WHERE subject_id = '{subject_id}' AND
            form_name = 'informed_consent_run_sheet' AND
            form_data ? 'chric_consent_date';
        """

        consented_date = db.fetch_record(
            config_file=config_file,
            query=query,
            db="formsdb",
        )

        if consented_date:
            try:
                date = datetime.strptime(consented_date, "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                date = pd.to_datetime(consented_date, dayfirst=True)
                date = date.to_pydatetime()

            return date

        return None

    get_dates_query = f"""
    SELECT 
        form_name,
        event_name,
        key AS interview_date_key,
        value AS interview_date_value
    FROM 
        forms.forms,
        jsonb_each_text(form_data)
    WHERE 
        subject_id = '{subject_id}'
        AND key LIKE '%%_interview_date';
    """

    dates_df = db.execute_sql(
        config_file=config_file,
        query=get_dates_query,
        db="formsdb",
    )

    df = pd.DataFrame(dates_df)
    visit_order: List[str] = [
        "screening",
        "baseline",
        "month_1",
        "month_2",
        "month_3",
        "month_4",
        "month_5",
        "month_6",
        "month_7",
        "month_8",
        "month_9",
        "month_10",
        "month_11",
        "month_12",
        "month_18",
        "month_24",
    ]

    def get_visit(name):
        for visit in visit_order:
            if f"{visit}_" in name:
                return visit
        return None

    # Map event name to the visit order
    df["visit"] = df["event_name"].apply(get_visit)

    df["interview_date_value"] = pd.to_datetime(
        df["interview_date_value"], errors="coerce"
    )
    df = df.dropna(subset=["interview_date_value"])
    df = df[df["interview_date_value"].dt.year >= 1950]

    # Group by date and visit to count the number of forms
    grouped = (
        df.groupby(["interview_date_value", "visit"]).size().reset_index(name="count")
    )

    # Sort the DataFrame by date and visit order
    grouped = grouped.sort_values(
        by=["interview_date_value", "visit"],
        key=lambda col: col.apply(
            lambda x: (visit_order.index(x) if x in visit_order else len(visit_order))
        ),
    )

    consent_date = get_subject_consent_dates(
        config_file=config_file, subject_id=subject_id
    )
    withdrawal_date = get_subject_withdrawn_date(subject_id, config_file)
    conversion_date = get_subject_converted_date(subject_id, config_file)

    # With consent_date as day 1, Add a day column to the DataFrame
    grouped["day"] = (grouped["interview_date_value"] - consent_date).dt.days

    # Plotting
    plt.figure(figsize=(12, 8))

    if consent_date:
        plt.axvline(consent_date, color="g", linestyle="--", label="Consented")
    if withdrawal_date:
        plt.axvline(withdrawal_date, color="r", linestyle="--", label="Withdrawn")
    if conversion_date:
        plt.axvline(conversion_date, color="blue", linestyle="--", label="Converted")

    # Highlight the dates on the X axis
    highlighted_dates = [consent_date, conversion_date, withdrawal_date]
    for date in highlighted_dates:
        if date:
            days_since_consent = (date - consent_date).days
            plt.text(
                date,
                len(visit_order) + 0.5,
                f"{days_since_consent}",
                horizontalalignment="center",
                verticalalignment="bottom",
                fontsize=10,
                color="black",
                weight="bold",
            )

    # Create a bubble plot
    # The size argument controls the size of the bubbles
    # Create a bubble plot with color based on event_name
    plt.scatter(
        grouped["interview_date_value"],
        grouped["visit"].apply(lambda x: visit_order.index(x)),
        s=grouped["count"]
        * 50,  # Multiply by a factor to adjust bubble sizes for better visibility
        alpha=0.5,
        c=grouped["visit"].apply(
            lambda x: visit_order.index(x)
        ),  # Color based on visit order
        cmap="viridis",  # You can choose any colormap
    )

    texts = []
    for _, row in grouped.iterrows():
        font_size = 9 + (row["count"] - 1) * 0.5  # Scale font size based on count
        text = plt.text(
            row["interview_date_value"],
            visit_order.index(row["visit"]),
            str(row["count"]),
            horizontalalignment="center",
            verticalalignment="center",
            fontsize=font_size,
            color="black",
            weight="bold",
        )
        texts.append(text)

    adjust_text(texts, arrowprops=dict(arrowstyle="-", color="k", lw=0.5))

    # Use days as secondary x-axis
    consent_date_float = consent_date.timestamp() / (
        24 * 3600
    )  # Convert consent_date to float days since epoch
    secondary_ax = plt.gca().secondary_xaxis(
        "top",
        functions=(lambda x: x - consent_date_float, lambda x: x + consent_date_float),
    )
    secondary_ax.set_xlabel("Days since consent")

    # Set y-ticks to correspond to visit_order
    plt.grid(True, which="both", linestyle="--", linewidth=0.2)
    plt.yticks(range(len(visit_order)), visit_order)
    plt.xlabel("Date")
    plt.ylabel("Timepoint")
    plt.title(f"Forms per timepoint for {subject_id}")
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()

    # save the plot
    plot_file = Path(f"/tmp/{subject_id}_forms_per_timepoint_{datetime.now()}.png")
    plt.savefig(plot_file)

    logger.debug(f"Saved plot to {plot_file}")

    return plot_file


def plot_forms_per_timepoint_v2(
    subject_id: str,
    config_file: Path,
) -> Path:
    def get_subject_withdrawn_date(
        subject_id: str,
        config_file: Path,
    ) -> Optional[datetime]:
        withdrawn_date_query = f"""
        SELECT 
            removed_date
        FROM 
            forms_derived.subject_removed
        WHERE 
            subject_id = '{subject_id}' AND
            removed = TRUE;
        """

        withdrawn_date = db.fetch_record(
            config_file=config_file,
            query=withdrawn_date_query,
            db="formsdb",
        )

        if withdrawn_date:
            withdrawn_date_dt = datetime.strptime(withdrawn_date, "%Y-%m-%d %H:%M:%S")
            # Replace the time part with 00:00:00
            withdrawn_date_dt = withdrawn_date_dt.replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            return withdrawn_date_dt

        return None

    def get_subject_converted_date(
        subject_id: str, config_file: Path
    ) -> Optional[datetime]:
        converted_date_query = f"""
        SELECT 
            conversion_date
        FROM 
            forms_derived.conversion_status
        WHERE 
            subject_id = '{subject_id}' AND
            converted = TRUE;
        """

        converted_date = db.fetch_record(
            config_file=config_file,
            query=converted_date_query,
            db="formsdb",
        )

        if converted_date:
            converted_date_dt = datetime.strptime(converted_date, "%Y-%m-%d %H:%M:%S")
            # Replace Time part with 00:00:00
            converted_date_dt = converted_date_dt.replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            return converted_date_dt

        return None

    def get_form_date(
        config_file: Path,
        subject_id: str,
        form_name: str,
        event_name: str,
    ) -> Optional[datetime]:

        form_name_to_date_variable_overrides: Dict[str, str] = {
            "lifetime_ap_exposure_screen": "chrap_date",
            "informed_consent_run_sheet": "chric_consent_date",
            "inclusionexclusion_criteria_review": "chrcrit_date",
            "psychs_p9ac32": None,  # type: ignore[assignment]
            "sofas_followup": "chrsofas_interview_date_fu",
        }

        variable_name = form_name_to_date_variable_overrides.get(form_name)
        if variable_name is None:
            # Check for '%%_interview_date' pattern in form_data JSON
            query = f"""
            SELECT fields.value AS interview_date
            FROM forms.forms f
                JOIN LATERAL jsonb_each_text(f.form_data) AS fields(key, value)
            ON TRUE
            WHERE f.subject_id = '{subject_id}'
                AND f.form_name = '{form_name}'
                AND f.event_name LIKE '%%{event_name}_arm%%'
                AND fields.key ~ '_interview_date$';
            """
        else:
            query = f"""
            SELECT 
                form_data ->> '{variable_name}' AS form_date
            FROM
                forms.forms
            WHERE
                subject_id = '{subject_id}' AND
                form_name = '{form_name}' AND
                event_name LIKE '%%{event_name}_arm%%';
            """

        form_date = db.fetch_record(
            config_file=config_file,
            query=query,
            db="formsdb",
        )

        if form_date:
            form_date_dt = datetime.fromisoformat(form_date)
            return form_date_dt

        return None

    def get_subject_prediction_targets(
        config_file: Path,
        subject_id: str,
    ) -> List[datetime]:
        # Targets are Month 12 and Month 24 Psychs
        prediction_targets = []

        for visit in ["month_12", "month_24"]:
            prediction_target = get_form_date(
                config_file=config_file,
                subject_id=subject_id,
                form_name="psychs_p1p8_fu",
                event_name=visit,
            )
            if prediction_target:
                prediction_targets.append(prediction_target)

        return prediction_targets

    cohort = data.get_subject_cohort(
        config_file=config_file,
        subject_id=subject_id,
    )
    required_forms = data.get_forms_cohort_timepoint_map()[cohort.lower()]
    visits = required_forms.keys()

    results: List[Dict[str, Any]] = []
    skipped_forms: List[str] = [
        "psychs_p9ac32_fu",
        "psychs_p9ac32",  # No date field
        "coenrollment_form",
    ]

    for visit in visits:
        required_visit_forms = required_forms[visit]
        visit_results: Dict[datetime, int] = {}
        skipped_forms_count = 0
        for required_form in required_visit_forms:
            if required_form in skipped_forms:
                continue
            form_date = get_form_date(
                config_file=config_file,
                subject_id=subject_id,
                form_name=required_form,
                event_name=visit,
            )
            if form_date:
                # Drop the time part
                form_date = form_date.replace(hour=0, minute=0, second=0, microsecond=0)
                if form_date in visit_results:
                    visit_results[form_date] += 1
                else:
                    visit_results[form_date] = 1
            # print(f"{visit} - {required_form} - {form_date}")

        visit_required_forms_count = len(required_visit_forms) - skipped_forms_count
        # Sort the visit_results by date
        visit_results = dict(sorted(visit_results.items()))

        cumulative_completed_forms = 0
        for form_date, form_count in visit_results.items():
            start_ratio = cumulative_completed_forms
            cumulative_completed_forms += form_count
            visit_results = {
                "event_name": visit,
                "interview_date": form_date,
                "cumulative_completed_forms": cumulative_completed_forms,
                "cumulative_completed_forms_start_ratio": start_ratio
                / visit_required_forms_count,
                "cumulative_completed_forms_end_ratio": cumulative_completed_forms
                / visit_required_forms_count,
            }
            results.append(visit_results)

    results_df = pd.DataFrame(results)

    # Assuming dates_df and constants are defined somewhere else
    df = pd.DataFrame(results_df)

    # Convert interview_date_value to datetime
    df["interview_date"] = pd.to_datetime(df["interview_date"], errors="coerce")

    # Drop rows with dates before 1950
    df = df[df["interview_date"].dt.year >= 1950]

    consent_date = data.get_subject_consent_dates(
        config_file=config_file, subject_id=subject_id
    )
    withdrawal_date = get_subject_withdrawn_date(subject_id, config_file)
    conversion_date = get_subject_converted_date(subject_id, config_file)
    prediction_targets = get_subject_prediction_targets(
        config_file=config_file, subject_id=subject_id
    )

    # With consent_date as day 1, Add a day column to the DataFrame
    df["day"] = (df["interview_date"] - consent_date).dt.days

    # Plotting
    plt.figure(figsize=(12, 8))

    if consent_date:
        plt.axvline(
            consent_date,
            color="g",
            linestyle="--",
            label="Consented",
            alpha=0.5,
            linewidth=0.7,
        )
    if withdrawal_date:
        plt.axvline(
            withdrawal_date,
            color="r",
            linestyle="--",
            label="Withdrawn",
            alpha=0.5,
            linewidth=0.7,
        )
    if conversion_date:
        plt.axvline(
            conversion_date,
            color="blue",
            linestyle="--",
            label="Converted",
            alpha=0.5,
            linewidth=0.7,
        )
    for i, target in enumerate(prediction_targets):
        if i == 0:
            plt.axvline(
                target,
                color="black",
                linestyle="--",
                label="Prediction Target",
                alpha=0.5,
                linewidth=0.7,
            )
        else:
            plt.axvline(target, color="black", linestyle="--", alpha=0.5, linewidth=0.7)

    # Highlight the dates on the X axis
    # highlighted_dates = [consent_date, conversion_date, withdrawal_date]
    # highlighted_dates.extend(prediction_targets)
    highlighted_dates: List[Tuple[str, datetime, str]] = [
        (f"Consented", consent_date, "g"),
        (f"Converted", conversion_date, "blue"),
        (f"Withdrawn", withdrawal_date, "r"),
    ]
    highlighted_dates.extend(
        [
            (f"Prediction Target {i+1}", target, "black")
            for i, target in enumerate(prediction_targets)
        ]
    )

    for label, date, color in highlighted_dates:
        if date:
            days_since_consent = (date - consent_date).days
            plt.text(
                date,
                len(constants.visit_order) + 0.6,
                f"{days_since_consent}",
                horizontalalignment="center",
                verticalalignment="bottom",
                fontsize=10,
                color=color,
                weight="bold",
            )

    # Create vertical bars to avoid overlaps
    # y axis from cumulative_completed_forms_start_ratio to cumulative_completed_forms_end_ratio
    for i, row in df.iterrows():
        interview_date = row["interview_date"]
        start_ratio = row["cumulative_completed_forms_start_ratio"]
        end_ratio = row["cumulative_completed_forms_end_ratio"]

        plt.plot(
            [interview_date, interview_date],
            [
                constants.visit_order.index(row["event_name"]) + start_ratio,
                constants.visit_order.index(row["event_name"]) + end_ratio,
            ],
            linewidth=1,
            color=plt.cm.inferno(
                constants.visit_order.index(row["event_name"])
                / len(constants.visit_order)
            ),
            alpha=0.7,
        )

    # Use days as secondary x-axis
    consent_date_float = consent_date.timestamp() / (
        24 * 3600
    )  # Convert consent_date to float days since epoch
    secondary_ax = plt.gca().secondary_xaxis(
        "top",
        functions=(lambda x: x - consent_date_float, lambda x: x + consent_date_float),
    )
    secondary_ax.set_xlabel("Days since consent", labelpad=15)

    # Set y-ticks to correspond to visit_order
    plt.grid(True, which="both", linestyle="--", linewidth=0.2)
    plt.yticks(range(len(constants.visit_order)), constants.visit_order)
    plt.xlabel("Date")
    plt.ylabel("Event Name")
    plt.title(f"Forms per Timepoint for {subject_id}")
    plt.xticks(
        pd.date_range(
            start=df["interview_date"].min(), end=df["interview_date"].max(), freq="ME"
        ).to_pydatetime(),
        rotation=45,
    )
    plt.gca().xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter("%Y-%m"))

    # Sek ylim to include all visits
    plt.ylim(-1, len(constants.visit_order))

    plt.legend()
    plt.tight_layout()

    # save the plot
    plot_file = Path(f"/tmp/{subject_id}_forms_per_timepoint_{datetime.now()}_v2.png")
    plt.savefig(plot_file)

    logger.debug(f"Saved plot to {plot_file}")
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
    except IndexError as e:
        if len(parts) < 2:
            logger.error(f"{user_id} - {command['text']} - Invalid command")
            respond("Usage: /plot [modality] [count/size] [network] [num_days]")
            return
        if len(parts) == 2:
            modality = parts[0]
            count_size = parts[1]
            network = None
            num_days = 14
        elif len(parts) == 3:
            modality = parts[0]
            count_size = parts[1]
            network = parts[2]
            num_days = 14
        else:
            raise e

    logger.info(f"{user_id} - {command['text']}")

    # Check if network in [ProNET. PRESCIENT]
    if network == "pronet":
        network = "Pronet"
    elif network == "prescient":
        network = "Prescient"

    if count_size not in ["count", "size"]:
        respond("Usage: /plot [modality] [count/size] [network] [num_days]")
        return

    try:
        plot_file = plot_modality_data_flow(
            modality=modality,
            metric=count_size,
            config_file=config_file,
            network_id=network,
            num_days=num_days,
        )
    except ValueError as e:
        logger.error(e)
        respond(f"Error: {e}")
        return

    try:
        # Upload the file
        with open(plot_file, "rb") as f:
            f.seek(0)
            _ = app.client.files_upload_v2(
                file=f,
                channel=command["channel_id"],
                user=command["user_id"],
                initial_comment=f"Plot for {modality} data flow {count_size} \
    - {network} {num_days} days",
                title=f"{modality}_{count_size}_{network}_{num_days}days.png",
                alt_txt=f"{modality} data flow {count_size} visualization - {network} {num_days} days",
            )
    except Exception as e:
        logger.error(f"Failed to upload file: {e}")
        respond(f"Error uploading plot: {e}")
        return

    respond(f"Uploaded plot for {modality} data flow")
    logger.debug(
        f"{user_id} - Uploaded plot for {modality} data flow {count_size} \
- {network} {num_days} days"
    )


@app.command("/track-dev")
def send_plot_forms_per_timepoint(ack, respond, command):
    """
    Slash command wrapper for plotting the forms per timepoint for a given subject.

    Usage:
    - /track [subject_id]
        - subject_id: Subject ID

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
        subject_id = parts[0]
    except IndexError:
        if len(parts) < 1:
            logger.error(f"{user_id} - {command['text']} - Invalid command")
            respond("Usage: /track [subject_id]")
            return

    logger.info(f"{user_id} - {command['text']}")

    try:
        plot_file = plot_forms_per_timepoint(
            subject_id=subject_id, config_file=config_file
        )
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
            initial_comment=f"Forms per timepoint for {subject_id}",
            title=f"{subject_id}_forms_per_timepoint.png",
            alt_txt=f"Forms per timepoint visualization for {subject_id}",
        )

    respond(f"Uploaded plot for forms per timepoint for {subject_id}")
    logger.debug(f"{user_id} - Uploaded plot for forms per timepoint")


@app.command("/track")
def send_plot_forms_per_timepoint(ack, respond, command):
    """
    Slash command wrapper for plotting the forms per timepoint for a given subject.

    Usage:
    - /track-dev [subject_id]
        - subject_id: Subject ID

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
        subject_id = parts[0]
    except IndexError:
        if len(parts) < 1:
            logger.error(f"{user_id} - {command['text']} - Invalid command")
            respond("Usage: /track [subject_id]")
            return

    logger.info(f"{user_id} - {command['text']}")

    try:
        plot_file = plot_forms_per_timepoint_v2(
            subject_id=subject_id, config_file=config_file
        )
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
            initial_comment=f"Forms per timepoint for {subject_id}",
            title=f"{subject_id}_forms_per_timepoint_v2.png",
            alt_txt=f"Forms per timepoint visualization for {subject_id}",
        )

    respond(f"Uploaded plot for forms per timepoint for {subject_id}")
    logger.debug(f"{user_id} - Uploaded plot for forms per timepoint")


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


def get_recruitment_numbers_raw() -> List[Dict[str, Any]]:
    """
    Returns the raw recruitment numbers from the database.

    Returns:
    - List[Dict[str, Any]]: List of dictionaries containing recruitment numbers in BlockKit format
    """
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

    return blocks


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
    logger.debug(f"{user_id} - {command['text']}")

    recruitment_numbers = get_recruitment_numbers_raw()

    payload = {
        "blocks": recruitment_numbers,
    }
    respond(payload)


from slack_sdk.web import WebClient


@app.event("app_home_opened")
def update_home_tab(client: WebClient, event: dict):
    user_id = event["user"]
    logger.info(f"App home opened by user {user_id}")

    recruitment_numbers = get_recruitment_numbers_raw()

    # Set Welcome message
    welcome_message = {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f":wave: Welcome to the PHOENIX Tracker Bot, <@{user_id}>!",
        },
    }

    modalities = [
        "actigraphy",
        "eeg",
        "interviews",
        "mri",
        "phone",
        "surveys",
    ]

    networks = [
        "Pronet",
        "Prescient",
    ]

    image_blocks: List[Dict[str, Any]] = []

    for modality in modalities:
        plot_file = generate_master_plot(
            modalities=[modality],
            networks=networks,
            config_file=config_file,
            num_days=28,
        )

        public_image_url = upload_file(
            file_path=plot_file,
            config_file=config_file,
        )
        image_block = {
            "type": "image",
            "image_url": public_image_url,
            "alt_text": f"{modality} data flow",
            "title": {
				"type": "plain_text",
				"text": f"{modality} data flow",
			},
        }
        image_blocks.append(image_block)


    # Flatten recruitment_numbers into the blocks list
    view = {
        "type": "home",
        "blocks": [welcome_message, *recruitment_numbers, *image_blocks],
    }

    logger.debug(f"Publishing home tab view for user {user_id} : {view}")

    client.views_publish(user_id=user_id, view=view)


if __name__ == "__main__":
    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )
    handler = SocketModeHandler(app, app_token)
    handler.start()
