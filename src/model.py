from typing import Optional 

from pydantic import BaseModel, Field, ValidationError
from pydantic_settings import BaseSettings

class AppConfig(BaseSettings):
    # Required variables (no default)
    API_KEY: str = Field(..., min_length=1, env="API_KEY")
    BASE_URL: str = Field(..., min_length=1, env="BASE_URL")
    FOLDER: str = Field(..., min_length=1, env="FOLDER")
    MODEL_NAME: str = Field(..., min_length=1, env="MODEL_NAME")
    MCP_CONFIG: str = Field(..., min_length=1, env="MCP_CONFIG")
    GITLAB_URL: str = Field(..., min_length=1, env="GITLAB_URL")
    GITLAB_TOKEN: str = Field(..., min_length=1, env="GITLAB_TOKEN")
    PROJECT_PATH: str = Field(..., min_length=1, env="PROJECT_PATH")
    
    # Optional variables with defaults
    LOG_LEVEL: str = Field('INFO', env="DEBUG")
    POSTGRESQL_URL: str = Field(None, env="POSTGRESQL_URL")

    class Config:
        # Extra configuration
        env_file = ".env"  # Optional: load from .env file
        env_file_encoding = "utf-8"
        extra = "ignore"  # Ignore extra environment variables


