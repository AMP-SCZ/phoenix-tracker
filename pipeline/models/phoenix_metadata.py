"""
phoenix.file_metadata Model
"""

from datetime import datetime
from typing import List, Dict, Any, Optional

from pipeline.helpers import db


class PhoenixFileMetadata:
    """
    Records the metadata of files associated with a PHOENIX filesystem.
    """

    def __init__(
        self,
        scan_id: int,
        file_path: str,
        network_id: str,
        access_level: str,
        study_id: str,
        data_stage: str,
        subject_id: str,
        modality: str,
        extra_meta: Optional[Dict[str, Any]] = None,
        recorded_at: Optional[datetime] = None,
    ):
        self.scan_id = scan_id
        self.file_path = file_path
        self.network_id = network_id
        self.access_level = access_level
        self.study_id = study_id
        self.data_stage = data_stage
        self.subject_id = subject_id
        self.modality = modality
        self.extra_meta = extra_meta if extra_meta is not None else {}
        self.recorded_at = recorded_at if recorded_at is not None else datetime.now()

    def __str__(self):
        return (
            f"PhoenixFileMetadata(scan_id={self.scan_id}, file_path='{self.file_path}', "
            f"network_id='{self.network_id}', access_level='{self.access_level}', "
            f"study_id='{self.study_id}', data_stage='{self.data_stage}', "
            f"subject_id='{self.subject_id}', modality='{self.modality}', "
            f"extra_meta={self.extra_meta}, recorded_at={self.recorded_at})"
        )

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def init_table_query() -> List[str]:
        """
        Return the SQL query to create the 'phoenix.file_metadata' table.
        """
        init_schema = """
            CREATE SCHEMA IF NOT EXISTS phoenix;
        """
        init_query = """
            CREATE TABLE phoenix.file_metadata (
                scan_id      int         NOT NULL,
                file_path    text        NOT NULL,
                network_id   text        NOT NULL,  -- e.g. Pronet, Prescient, …
                access_level text        NOT NULL,  -- e.g. PROTECTED or GENERAL
                study_id     text        NOT NULL,  -- e.g. PronetNC, PronetNN, …
                data_stage   text        NOT NULL,  -- raw or processed
                subject_id   text        NOT NULL,  -- e.g. NC00000
                modality     text        NOT NULL,  -- e.g. phone, surveys, actigraphy
                extra_meta   jsonb       NULL,
                recorded_at  timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (scan_id, file_path),
                FOREIGN KEY (scan_id, file_path) REFERENCES filesystem.file_changes (scan_id, file_path) ON DELETE CASCADE
            );
        """
        index_query_1 = """
        CREATE INDEX ON phoenix.file_metadata
        ( scan_id, network_id, access_level, modality, data_stage, file_path);
        """
        index_query_2 = """
            CREATE INDEX ON phoenix.file_metadata USING gin (extra_meta);
        """

        return [init_schema, init_query, index_query_1, index_query_2]

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'phoenix.file_metadata' table.
        """
        drop_query = """
            DROP TABLE IF EXISTS phoenix.file_metadata;
        """

        return drop_query

    def to_sql(self):
        """
        Return the SQL query to insert the object into the 'phoenix.file_metadata' table.
        """

        file_path = db.sanitize_string(str(self.file_path))
        if self.extra_meta is None:
            json_str = "NULL"
        else:
            json_str = db.sanitize_json(self.extra_meta)

        sql_query = f"""
            INSERT INTO phoenix.file_metadata (
                scan_id, file_path, network_id, access_level, study_id, data_stage,
                subject_id, modality, extra_meta, recorded_at
            ) VALUES (
                {self.scan_id}, '{file_path}', '{self.network_id}', '{self.access_level}',
                '{self.study_id}', '{self.data_stage}', '{self.subject_id}', '{self.modality}',
                '{json_str}', '{self.recorded_at.isoformat()}'
            )
        """

        sql_query = db.handle_null(sql_query)

        return sql_query
