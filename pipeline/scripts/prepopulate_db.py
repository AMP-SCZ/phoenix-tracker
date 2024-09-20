#!/usr/bin/env python
"""
Initializes the database.
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
import json
from typing import Set

from rich.logging import RichHandler

from pipeline.helpers import db, utils

MODULE_NAME = "init_db"
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


def populate_sites(sites_json: Path, config_file: Path) -> None:
    """
    Populates the site table with data from a JSON file.

    Args:
        sites_json (str): The path to the JSON file containing the site data.
        config_file (str): The path to the configuration file containing the connection parameters.
    """

    class Site:
        """
        Represents a site.
        """

        def __init__(self, site_id, name, country, country_code, network_id):
            self.id = site_id
            self.name = name
            self.country = country
            self.country_code = country_code
            self.network_id = network_id

            if self.country == "":
                self.country = "NULL"

    with open(sites_json, encoding="utf-8") as f:
        sites = json.load(f)

    networks: Set[str] = set()
    commands = []

    for site in sites:
        site_obj = Site(
            site_id=db.santize_string(site["id"]),
            name=db.santize_string(site["name"]),
            country=db.santize_string(site["country"]),
            country_code=db.santize_string(site["country_code"]),
            network_id=db.santize_string(site["network"]),
        )

        networks.add(site_obj.network_id)

        command = f"""
        INSERT INTO study (
            study_id, study_name, study_country,
            study_country_code, network_id
        ) VALUES (
            '{site_obj.id}', '{site_obj.name}', '{site_obj.country}',
            '{site_obj.country}', '{site_obj.network_id}');
        """

        command = db.handle_null(command)

        commands.append(command)

    for network in networks:
        command = f"""
        INSERT INTO networks (network_id)
        VALUES ('{network}');
        """

        command = db.handle_null(command)

        # Add the command to the list's beginning so that it is executed first
        commands.insert(0, command)

    logger.info(f"Found {len(networks)} Networks")

    db.execute_queries(
        config_file=config_file,
        queries=commands,
        show_commands=False,
        silent=True,
    )


if __name__ == "__main__":
    config_file = utils.get_config_file_path()
    utils.configure_logging(
        config_file=config_file, module_name=MODULE_NAME, logger=logger
    )

    console.rule(f"[bold red]{MODULE_NAME}")
    logger.info(f"Using config file: {config_file}")

    params = utils.config(config_file, "general")

    logger.info("Populating sites...")
    sites_json = Path(params["sites_json"])
    logger.info(f"Using sites JSON file: {sites_json}")
    populate_sites(sites_json, config_file)
