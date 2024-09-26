"""
VolumeStatistics Model
"""

from datetime import datetime


class VolumeStatistics:
    """
    Records the statistics of all files associated with a subject.
    """

    def __init__(
        self,
        study_id: str,
        subject_id: str,
        is_raw: bool,
        is_protected: bool,
        modality: str,
        files_count: int,
        files_size_mb: float,
        statistics_timestamp: datetime,
    ):
        self.study_id = study_id
        self.subject_id = subject_id
        self.is_raw = is_raw
        self.is_protected = is_protected
        self.modality = modality
        self.files_count = files_count
        self.files_size_mb = files_size_mb
        self.statistics_timestamp = statistics_timestamp

    def __str__(self):
        protected_str = "protected" if self.is_protected else "general"
        raw_str = "raw" if self.is_raw else "processed"
        return f"VolumeStatistics( {self.subject_id} {protected_str} {raw_str}\
Count: {self.files_count} Size: {self.files_size_mb}MB)"

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'volume_statistics' table.
        """

        init_query = """
            CREATE TABLE IF NOT EXISTS volume_statistics (
                study_id TEXT NOT NULL,
                subject_id TEXT NOT NULL,
                is_raw BOOLEAN NOT NULL,
                is_protected BOOLEAN NOT NULL,
                modality TEXT NOT NULL,
                files_count INTEGER NOT NULL,
                files_size_mb REAL NOT NULL,
                statistics_timestamp TIMESTAMP,
                PRIMARY KEY (study_id, subject_id, is_raw, is_protected, modality, statistics_timestamp),
                FOREIGN KEY (study_id, subject_id) REFERENCES subjects (study_id, subject_id)
            );
        """

        return init_query

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'volume_statistics' table.
        """
        drop_query = """
            DROP TABLE IF EXISTS volume_statistics;
        """

        return drop_query

    def to_sql(self):
        """
        Return the SQL query to insert the object into the 'volume_statistics' table.
        """

        sql_query = f"""
            INSERT INTO volume_statistics (
                study_id, subject_id, is_raw, is_protected, modality,
                files_count, files_size_mb, statistics_timestamp
            ) VALUES (
                '{self.study_id}', '{self.subject_id}', {self.is_raw}, {self.is_protected},
                '{self.modality}', {self.files_count}, {self.files_size_mb}, '{self.statistics_timestamp}'
            );
        """

        return sql_query
