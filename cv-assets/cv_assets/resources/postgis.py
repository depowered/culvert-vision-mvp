from dataclasses import dataclass

from dagster import ConfigurableResource


class PostGISResource(ConfigurableResource):
    """A Resource to connect to a PostGIS database."""

    dbname: str
    username: str
    password: str
    host: str
    port: int

    @property
    def dsn(self) -> str:
        """The database connection string."""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.dbname}"


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
