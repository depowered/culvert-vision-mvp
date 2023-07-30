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
