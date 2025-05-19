#!/usr/bin/env python
"""
Airflow DAG for PHOENIX Tracker
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

CONDA_ENV_PATH = "/home/pnl/miniforge3/envs/jupyter/bin"
PYTHON_PATH = f"{CONDA_ENV_PATH}/python"
REPO_ROOT = "/home/pnl/dev/phoenix-tracker"

# Define variables
default_args = {
    "owner": "pnl",
    "depends_on_past": False,
    "start_date": datetime(2025, 4, 21),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "catchup": False,
}

dag = DAG(
    "ampscz_phoenix_tracker",
    default_args=default_args,
    description="DAG for AMPSCZ phoenix metadata tracker",
    schedule="@daily",
)

info = BashOperator(
    task_id="print_info",
    bash_command='''echo "$(date) - Hostname: $(hostname)"
echo "$(date) - User: $(whoami)"
echo ""
echo "$(date) - Current directory: $(pwd)"
echo "$(date) - Git branch: $(git rev-parse --abbrev-ref HEAD)"
echo "$(date) - Git commit: $(git rev-parse HEAD)"
echo "$(date) - Git status: "
echo "$(git status --porcelain)"
echo ""
echo "$(date) - Uptime: $(uptime)"''',
    dag=dag,
    cwd=REPO_ROOT,
)

# Import study metadata
import_metadata = BashOperator(
    task_id="import_metadata",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/pipeline/crawler/01_import_study_metadata.py",
    dag=dag,
    cwd=REPO_ROOT,
)

# Import files metadata
import_files = BashOperator(
    task_id="import_files",
    bash_command=PYTHON_PATH + " " + REPO_ROOT + "/pipeline/crawler/02_import_files.py",
    dag=dag,
    cwd=REPO_ROOT,
)

# Compute statistics
compute_statistics = BashOperator(
    task_id="compute_statistics",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/pipeline/crawler/03_compute_statistics.py",
    dag=dag,
    cwd=REPO_ROOT,
)

# Send Notification
send_notification = BashOperator(
    task_id="send_notification",
    bash_command=PYTHON_PATH
    + " "
    + REPO_ROOT
    + "/pipeline/scripts/send_notification.py",
    dag=dag,
    cwd=REPO_ROOT,
)

# Done Task Definitions

# Start DAG construction

info.set_downstream(import_metadata)
import_metadata.set_downstream(import_files)
import_files.set_downstream(compute_statistics)
compute_statistics.set_downstream(send_notification)

# End DAG construction
