"""
Network Model
"""

from pipeline.helpers import db


class Network:
    """
    Represents a research networks.

    Attributes:
        network_id (str): The network ID.
    """

    def __init__(self, network_id: str):
        self.network_id = network_id

    def __str__(self):
        return f"Nework({self.network_id})"

    def __repr__(self):
        return self.__str__()

    @staticmethod
    def init_table_query() -> str:
        """
        Return the SQL query to create the 'networks' table.
        """
        return """
            CREATE TABLE IF NOT EXISTS networks (
                network_id TEXT PRIMARY KEY
            );
        """

    @staticmethod
    def drop_table_query() -> str:
        """
        Return the SQL query to drop the 'networks' table.
        """
        return """
            DROP TABLE IF EXISTS networks;
        """

    def to_sql(self):
        """
        Return the SQL query to insert the object into the 'networks' table.
        """
        network_id = db.santize_string(self.network_id)

        sql_query = f"""
            INSERT INTO networks (network_id)
            VALUES ('{network_id}');
        """

        return sql_query
