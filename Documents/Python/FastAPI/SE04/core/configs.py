from pydantic_settings import BaseSettings # type: ignore


class Settings(BaseSettings):
    API_V1_STR: str = '/api/v1'
    DB_URL: str = "postgresql+asyncpg://admin:admin123456@localhost:5455/faculdade"

    class Config:
        case_sensitive = True

settings: Settings = Settings()