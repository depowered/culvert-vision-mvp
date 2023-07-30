from pydantic import BaseSettings, DirectoryPath


class Settings(BaseSettings):
    file_asset_storage_dir: DirectoryPath

    target_epsg: int

    postgres_db: str
    postgres_user: str
    postgres_pass: str
    postgres_host: str
    postgres_port: int

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


def get_settings() -> Settings:
    return Settings()
