import os
from pathlib import Path
from  pydantic_settings import BaseSettings
from typing import Optional
from urllib.parse import quote_plus

class Settings(BaseSettings):
    """
    Configurations setting for the e-commerce ETL pipeline.
    Loads From environment variables and .env file.
    """
    # Database settings
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "ecommerce_retail"
    db_user: str = "postgres"
    db_password: str = "actual_password"

    raw_data_path: str = "data/raw/"
    processed_data_path: str
    log_path: str = "logs/"

    # Optional: Used only if file is already present locally
    local_excel_file: str = "data/raw/Online_Retail.xlsx" 

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    @property
    def db_url(self) -> str:
        user = quote_plus(self.db_user)
        password = quote_plus(self.db_password)
        return f"postgres://{user}:{password}@{self.db_host}:{self.db_port}/{self.db_name}"
    
    def create_directories(self):
        for directory in [self.raw_data_path, self.processed_data_path, self.log_path]:
            Path(directory).mkdir(parents=True, exist_ok=True)
settings = Settings()
    

    

