import pandas as pd
from pathlib import Path
from src.utils.logger import etl_logger
from src.utils.config import settings
from typing import Optional

class ExcelExtractor:
    """
    Handles extractions of the uk online retail datasets from a local excel file.
    """

    def __init__(self):
        self.logger = etl_logger
        self.raw_data_path = Path(settings.raw_data_path)
        self.raw_data_path.mkdir(parents=True,exist_ok=True)
    def extract_from_excel(self,filepath: str) -> pd.DataFrame:
        """
        extract data from local excel file
        args:
            filepath(str): path to excel file

        returns:
            pd.DataFrame: Raw datasets
        """
        try:
            self.logger.info(f"Reading Excel file: {filepath}")
            df = pd.read_excel(filepath,sheet_name=0)
            self.logger.info(f"Successfully extracted {len(df)} records with {len(df.columns)} columns")
            return df
        except Exception as e:
            self.logger.error(f"Failed to extract data from Excel: {str(e)}")
            raise

    
    def save_raw_data(self,df: pd.DataFrame, filename: str = "raw_retail_data.csv"):
        """
        Save exttracted data to a csv file

        Args:
            df ( pd.DataFrame): DataFrame to save
            filename (str): Output filename
        
        Returns:
            str: Path to the saved CSV
        """
        try:
            filepath = self.raw_data_path / filename
            df.to_csv(filepath, index=False)
            self.logger.info(f"Raw data saved to: {filepath}")
            return str(filepath)
        except Exception as e:
            self.logger.error(f"Failed to save raw data: {str(e)}")
            raise

