from dataclasses import dataclass

from dagster import ConfigurableResource


class PostGISResource(ConfigurableResource):
    dbname: str
    username: str
    password: str
    host: str
    port: int

    @property
    def dsn(self) -> str:
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.dbname}"


@dataclass
class PGTable:
    schema: str
    table: str

    @property
    def qualified_name(self) -> str:
        return f"{self.schema}.{self.table}"
