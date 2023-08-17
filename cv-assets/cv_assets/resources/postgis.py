from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import psycopg2
from dagster import ConfigurableResource


class PostGISConfig(Protocol):
    postgres_db: str
    postgres_user: str
    postgres_pass: str
    postgres_host: str
    postgres_port: int


class PostGISResource(ConfigurableResource):
    """A Resource to connect to a PostGIS database."""

    postgres_db: str
    postgres_user: str
    postgres_pass: str
    postgres_host: str
    postgres_port: int

    @classmethod
    def from_config(cls, config: PostGISConfig) -> PostGISResource:
        return cls(
            postgres_db=config.postgres_db,
            postgres_user=config.postgres_user,
            postgres_pass=config.postgres_pass,
            postgres_host=config.postgres_host,
            postgres_port=config.postgres_port,
        )

    def get_connection(self) -> psycopg2.connection:
        """Get a connection to the PostGIS database. The caller is responsible for
        closing the connection.

        Example usage:
            ```
            conn = postgis.get_connection()
            try:
                with conn.cursor() as cursor:
                    cursor.execute(...)
            finally:
                conn.close()
            ```
        """
        return psycopg2.connect(self.dsn)

    def execute_and_commit(self, query: str) -> None:
        """Execute a query and commit a query that alters the database.

        Example usage:
            ```
            query = "CREATE TABLE ..."
            postgis.execute_and_commit(query)
            ```
        """
        conn = self.get_connection()
        try:
            with conn:
                with conn.cursor() as curs:
                    curs.execute(query)
        finally:
            conn.close()

    @property
    def dsn(self) -> str:
        """The database connection string."""
        return f"postgresql://{self.postgres_user}:{self.postgres_pass}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"


@dataclass
class PGTable:
    schema: str
    table: str

    @property
    def qualified_name(self) -> str:
        """
        The qualified name of the table, which is the combination of the
        schema and table names.
        """
        return f"{self.schema}.{self.table}"
