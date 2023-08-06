from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

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
