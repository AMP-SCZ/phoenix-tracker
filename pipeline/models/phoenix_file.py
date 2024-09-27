"""
PhoenixFile Model
"""

from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from pipeline.helpers import db


class PhoenixFile:
    """
    Represents a File in PHOENIX structure.
    """

    def __init__(
        self,
        study_id: str,
        subject_id: str,
        file_path: Path,
        is_raw: bool,
        is_protected: bool,
        modality: str,
        extracted_timestamp: datetime,
        metadata: Dict[str, Any],
    ):
        self.study_id = study_id
        self.subject_id = subject_id
        self.file_path = file_path
        self.is_raw = is_raw
        self.is_protected = is_protected
        self.modality = modality
        self.extracted_timestamp = extracted_timestamp
        self.metadata = metadata

    def __str__(self):
        protected_str = "protected" if self.is_protected else "general"
        raw_str = "raw" if self.is_raw else "processed"
        return f"PhoenixFile( {protected_str} {self.study_id} {raw_str} {self.subject_id} \
{self.file_path})"

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'phoenix_file' table.
        """
        insert_table_query = """
            CREATE TABLE IF NOT EXISTS phoenix_file (
                study_id TEXT NOT NULL,
                subject_id TEXT NOT NULL,
                file_path TEXT NOT NULL PRIMARY KEY,
                is_raw BOOLEAN NOT NULL,
                is_protected BOOLEAN NOT NULL,
                modality TEXT NOT NULL,
                extracted_timestamp TIMESTAMP,
                metadata JSONB,
                FOREIGN KEY (study_id, subject_id) REFERENCES subjects (study_id, subject_id),
                FOREIGN KEY (file_path) REFERENCES files (file_path)
            );
        """
        return insert_table_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'phoenix_file' table.
        """
        drop_table_query = """
            DROP TABLE IF EXISTS phoenix_file;
        """
        return drop_table_query

    @staticmethod
    def truncate_table_query() -> str:
        """
        Return the SQL query to truncate the 'phoenix_file' table.
        """
        truncate_table_query = "TRUNCATE TABLE phoenix_file RESTART IDENTITY CASCADE;"
        return truncate_table_query

    def to_sql(self):
        """
        Return the SQL query to insert the object into the 'phoenix_file' table.
        """

        file_path = db.santize_string(str(self.file_path))

        sql_query = f"""
            INSERT INTO phoenix_file (
                study_id, subject_id, file_path, is_raw,
                is_protected, modality, extracted_timestamp,
                metadata
            )
            VALUES (
                '{self.study_id}', '{self.subject_id}', '{file_path}', {self.is_raw},
                {self.is_protected}, '{self.modality}', '{self.extracted_timestamp}',
                '{db.sanitize_json(self.metadata)}'
            ) ON CONFLICT (file_path) DO UPDATE SET
                study_id = excluded.study_id,
                subject_id = excluded.subject_id,
                is_raw = excluded.is_raw,
                is_protected = excluded.is_protected,
                modality = excluded.modality,
                extracted_timestamp = excluded.extracted_timestamp,
                metadata = excluded.metadata;
        """
        return sql_query
