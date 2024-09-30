# PHOENIX Tracker

A simple crawler that tracks the rate of data added to the PHOENIX file system.

## Installation

- Create a `config.ini` file based on the `sample.config.ini` file.
- Add the DAG under `crons` directory to the `dags` directory in your Airflow installation after applicable modifications.

## Features

1. Crawls the PHOENIX file system and adds all files in the system to a database. (daily cron)
  - the files are tagged with:
    - modification date
    - size
    - modalities (e.g. MRI, EEG, etc.)
    - Associated subject ID / Study / Reseach Network
    - File type (e.g. CSV, zip, etc.)
2. Summarizes the data available per modality and per subject ID / Study / Research Network. (daily cron)
3. Sends out a daily Slack message with the delta between the previous day's data and the current day's data (based on 2). (daily cron)

## Intended Use

This tool is intended to be used to detect when no new data is being added to the PHOENIX file system, and alert the relevant parties.

## Tech Stack

- Python 3
- Airflow
- PostgreSQL
- Slack





