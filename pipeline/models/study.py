"""
Study Model
"""

from pipeline.helpers import db


class Study:
    """
    Represents a study.

    Attributes:
        study_id (str): The study ID.
    """

    def __init__(
        self,
        study_id: str,
        study_name: str,
        study_country: str,
        study_country_code: str,
        network_id: str,
    ):
        self.study_id = study_id
        self.study_name = study_name
        self.study_country = study_country
        self.study_country_code = study_country_code
        self.network_id = network_id

    def __str__(self):
        return f"Study({self.study_id})"

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'study' table.
        """
        return """
            CREATE TABLE IF NOT EXISTS study (
                study_id TEXT PRIMARY KEY,
                study_name TEXT NOT NULL,
                study_country TEXT,
                study_country_code TEXT,
                network_id TEXT NOT NULL REFERENCES networks (network_id)
            );
        """

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'study' table.
        """
        return """
            DROP TABLE IF EXISTS study;
        """

    def to_sql(self):
        """
        Return the SQL query to insert the object into the 'study' table.
        """
        study_id = db.santize_string(self.study_id)
        study_name = db.santize_string(self.study_name)
        study_country = db.santize_string(self.study_country)

        return f"""
            INSERT INTO study (study_id, study_name, study_country, study_country_code, network_id)
            VALUES ('{study_id}', '{study_name}', '{study_country}', '{self.study_country_code}', '{self.network_id}')
            ON CONFLICT (study_id) DO UPDATE
            SET study_name = excluded.study_name,
                study_country = excluded.study_country,
                study_country_code = excluded.study_country_code,
                network_id = excluded.network_id;
        """
