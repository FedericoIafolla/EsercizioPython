from impala.dbapi import connect


class ImpalaWriter:

    def __init__(self, host: str, port: int, database: str):
        """
        Repository to write data on Impala.

        :param host: hostname of the Impala server.
        :param port: port of the Impala server.
        :param database: default database to use.
        """
        self.host = host
        self.port = port
        self.database = database

    def insert(self, table: str, data: dict) -> None:
        """
        Insert a row inside the specified table.

        :param table: name of the table where to insert the data.
        :param data: dictionary column -> values
        :raises: exception in case of errors cause by connection or queries.
        """
        columns = ", ".join(data.keys())
        placeholders = ", ".join(["%s"] * len(data))
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        self.execute(query, list(data.values()))

    def execute(self, query: str, params: list | dict) -> None:
        """
        Execute a generic query with parameters.

        :param query: The SQL query to execute.
        :param params: The parameters to bind to the query.
        """
        with connect(host=self.host, port=self.port, database=self.database) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                conn.commit()