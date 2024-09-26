"""
Initializes the database.
"""

from pathlib import Path
from typing import List, Union

from pipeline.helpers import db

from pipeline.models.files import File
from pipeline.models.logs import Log
from pipeline.models.networks import Network
from pipeline.models.study import Study
from pipeline.models.subjects import Subject
from pipeline.models.phoenix_file import PhoenixFile
from pipeline.models.volume_statistics import VolumeStatistics


def flatten_list(coll: list) -> list:
    """
    Flattens a list of lists into a single list.

    Args:
        coll (list): List of lists.

    Returns:
        list: Flattened list.
    """
    flat_list = []
    for i in coll:
        if isinstance(i, list):
            flat_list += flatten_list(i)
        else:
            flat_list.append(i)
    return flat_list


def init_db(config_file: Path):
    """
    Initializes the database.

    WARNING: This will drop all tables and recreate them.
    DO NOT RUN THIS IN PRODUCTION.

    Args:
        config_file (Path): Path to the config file.
    """
    drop_queries_l: List[Union[str, List[str]]] = [
        VolumeStatistics.drop_table_query(),
        PhoenixFile.drop_table_query(),
        File.drop_table_query(),
        Subject.drop_table_query(),
        Study.drop_table_query(),
        Network.drop_table_query(),
        Log.drop_table_query(),
    ]

    create_queries_l: List[Union[str, List[str]]] = [
        Log.init_table_query(),
        Network.init_table_query(),
        Study.init_table_query(),
        Subject.init_table_query(),
        File.init_table_query(),
        PhoenixFile.init_table_query(),
        VolumeStatistics.init_table_query(),
    ]

    drop_queries = flatten_list(drop_queries_l)
    create_queries = flatten_list(create_queries_l)

    sql_queries: List[str] = drop_queries + create_queries

    db.execute_queries(config_file=config_file, queries=sql_queries)
