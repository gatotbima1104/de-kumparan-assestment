from abc import ABC, abstractmethod

import psycopg2


# abstract classes for variouse database connections
class DatabaseConnection(ABC):
    @abstractmethod
    def get_connection(self):
        """Return a database connection"""
        pass


class PostgresConnection(DatabaseConnection):
    def __init__(self, host, port, dbname, user, password):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password

    # return connection
    def get_connection(self):
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
        )
