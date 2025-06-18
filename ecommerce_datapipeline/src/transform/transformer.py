import pandas as pd
from datetime import datetime
from typing import List, Tuple
from pydantic import ValidationError
from src.utils.logger import etl_logger
from src.utils.config import settings
from models.onlineretail import OnlineRetailRecord, ProcessedRetailRecord

class DataTransformer:
    """
    Handles data cleaning, validation, and transformation for the retail dataset.
    """
    
    def __init__(self):
        self.logger = etl_logger
        self.processed_data_path = settings.processed_data_path
    
    def clean_raw_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean the raw dataset by handling missing values and data types.
        
        Args:
            df: Raw DataFrame
            
        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        try:
            self.logger.info("Starting data cleaning process...")
            
            # Create a copy to avoid modifying original data
            df_clean = df.copy()
            
            # Log initial data info
            self.logger.info(f"Initial dataset shape: {df_clean.shape}")
            self.logger.info(f"Missing values per column:\n{df_clean.isnull().sum()}")
            
            # Standardize column names (remove spaces, lowercase)
            df_clean.columns = [col.strip().lower().replace(' ', '_') for col in df_clean.columns]
            
            # Handle missing CustomerID (this is common in the dataset)
            missing_customers = df_clean['customerid'].isnull().sum()
            self.logger.info(f"Records with missing CustomerID: {missing_customers}")
            
            # Remove records with critical missing values
            initial_count = len(df_clean)
            
            # Remove records with missing invoice numbers or stock codes
            df_clean = df_clean.dropna(subset=['invoiceno', 'stockcode'])
            
            # Remove records with missing or invalid descriptions for products (not cancellations)
            df_clean = df_clean[
                (df_clean['description'].notna()) | 
                (df_clean['invoiceno'].str.startswith('C'))  # Keep cancellations even without description
            ]
            
            # Remove records with missing unit price
            df_clean = df_clean.dropna(subset=['unitprice'])
            
            final_count = len(df_clean)
            self.logger.info(f"Removed {initial_count - final_count} records with critical missing values")
            
            # Convert data types
            df_clean['invoicedate'] = pd.to_datetime(df_clean['invoicedate'])
            df_clean['quantity'] = pd.to_numeric(df_clean['quantity'], errors='coerce')
            df_clean['unitprice'] = pd.to_numeric(df_clean['unitprice'], errors='coerce')
            
            # Remove records where quantity or unitprice conversion failed
            df_clean = df_clean.dropna(subset=['quantity', 'unitprice'])
            
            # Clean text fields
            df_clean['description'] = df_clean['description'].str.strip()
            df_clean['country'] = df_clean['country'].str.strip()
            
            # Handle customer ID (convert to string, fill missing with 'UNKNOWN')
            df_clean['customerid'] = df_clean['customerid'].fillna('UNKNOWN').astype(str)
            
            self.logger.info(f"Data cleaning completed. Final shape: {df_clean.shape}")
            return df_clean
            
        except Exception as e:
            self.logger.error(f"Data cleaning failed: {str(e)}")
            raise
    
    def validate_records(self, df: pd.DataFrame) -> Tuple[List[dict], List[dict]]:
        """
        Validate records using Pydantic models.
        
        Args:
            df: Cleaned DataFrame
            
        Returns:
            Tuple[List[dict], List[dict]]: Valid records and error records
        """
        valid_records = []
        error_records = []
        
        self.logger.info(f"Validating {len(df)} records...")
        
        for idx, row in df.iterrows():
            try:
                # Create record dictionary
                record_dict = {
                    'invoice_no': str(row['invoiceno']),
                    'stock_code': str(row['stockcode']),
                    'description': str(row['description']) if pd.notna(row['description']) else None,
                    'quantity': int(row['quantity']),
                    'invoice_date': row['invoicedate'],
                    'unit_price': float(row['unitprice']),
                    'customer_id': str(row['customerid']) if pd.notna(row['customerid']) else None,
                    'country': str(row['country'])
                }
                
                # Validate using Pydantic model
                validated_record = OnlineRetailRecord(**record_dict)
                valid_records.append(validated_record.dict())
                
            except ValidationError as e:
                error_record = {
                    'row_index': idx,
                    'record': row.to_dict(),
                    'errors': e.errors()
                }
                error_records.append(error_record)
            except Exception as e:
                error_record = {
                    'row_index': idx,
                    'record': row.to_dict(),
                    'errors': [{'msg': str(e)}]
                }
                error_records.append(error_record)
        
        self.logger.info(f"Validation completed: {len(valid_records)} valid, {len(error_records)} errors")
        
        if error_records:
            self.logger.warning(f"Found {len(error_records)} validation errors")
            # Log first few errors for debugging
            for i, error in enumerate(error_records[:3]):
                self.logger.warning(f"Error {i+1}: {error['errors']}")
        
        return valid_records, error_records
    
    def enhance_records(self, valid_records: List[dict]) -> List[dict]:
        """
        Enhance valid records with calculated fields.
        
        Args:
            valid_records: List of validated record dictionaries
            
        Returns:
            List[dict]: Enhanced records
        """
        enhanced_records = []
        
        self.logger.info(f"Enhancing {len(valid_records)} records...")
        
        for record in valid_records:
            try:
                # Enhance with new fields
                record['total_amount'] = round(record['quantity'] * record['unit_price'], 2)
                record['is_return'] = record['invoice_no'].startswith('C')
                record['year'] = record['invoice_date'].year
                record['month'] = record['invoice_date'].month
                
                # Create enhanced record using ProcessedRetailRecord model
                enhanced_record = ProcessedRetailRecord(**record)
                enhanced_records.append(enhanced_record.dict())
                
            except Exception as e:
                self.logger.error(f"Failed to enhance record: {str(e)}")
                # Keep original record if enhancement fails
                enhanced_records.append(record)
        
        self.logger.info(f"Enhancement completed for {len(enhanced_records)} records")
        return enhanced_records
    
    def save_processed_data(self, enhanced_records: List[dict], filename: str = "processed_retail_data.csv") -> str:
        """
        Save processed data to CSV file.
        
        Args:
            enhanced_records: List of enhanced record dictionaries
            filename: Output filename
            
        Returns:
            str: Path to saved file
        """
        try:
            df_processed = pd.DataFrame(enhanced_records)
            filepath = f"{self.processed_data_path}/{filename}"
            
            df_processed.to_csv(filepath, index=False)
            
            self.logger.info(f"Processed data saved to: {filepath}")
            self.logger.info(f"Saved {len(df_processed)} records with {len(df_processed.columns)} columns")
            
            return filepath
            
        except Exception as e:
            self.logger.error(f"Failed to save processed data: {str(e)}")
            raise
    
    def generate_data_quality_report(self, df_original: pd.DataFrame, enhanced_records: List[dict]) -> dict:
        """
        Generate a data quality report comparing original and processed data.
        
        Args:
            df_original: Original raw DataFrame
            enhanced_records: List of processed record dictionaries
            
        Returns:
            dict: Data quality report
        """
        df_processed = pd.DataFrame(enhanced_records)
        
        report = {
            'original_records': len(df_original),
            'processed_records': len(df_processed),
            'data_loss_percentage': ((len(df_original) - len(df_processed)) / len(df_original)) * 100,
            'unique_customers': df_processed['customer_id'].nunique(),
            'unique_products': df_processed['stock_code'].nunique(),
            'unique_countries': df_processed['country'].nunique(),
            'date_range': {
                'start': df_processed['invoice_date'].min().strftime('%Y-%m-%d'),
                'end': df_processed['invoice_date'].max().strftime('%Y-%m-%d')
            },
            'returns_percentage': (df_processed['is_return'].sum() / len(df_processed)) * 100,
            'total_revenue': df_processed[df_processed['is_return'] == False]['total_amount'].sum()
        }
        
        self.logger.info(f"Data Quality Report: {report}")
        return report