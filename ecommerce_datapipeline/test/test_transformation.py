import pytest 
import pandas as pd
from src.transform.transformer import DataTransformer

@pytest.fixture
def sample_raw_df():
    return pd.DataFrame({
        "InvoiceNo": ["10001", "C10002", None],
        "StockCode": ["A123", "B456", "C789"],
        "Description": ["Product A", None, "Product C"],
        "Quantity": [1, -2, 3],
        "InvoiceDate": ["2021-01-01 10:00", "2021-01-02 12:00", "2021-01-03 14:00"],
        "UnitPrice": [10.0, 20.0, None],
        "CustomerID": [12345, None, 67890],
        "Country": ["United Kingdom", "France", "Germany"]
    })

def test_clean_raw_data(sample_raw_df):
    transformer = DataTransformer()
    cleaned_df = transformer.clean_raw_data(sample_raw_df)

    # Ensure no missing InvoiceNo or StockCode
    assert cleaned_df['invoiceno'].isnull().sum() == 0
    assert cleaned_df['stockcode'].isnull().sum() == 0

    # Quantity and UnitPrice should not contain NaNs
    assert cleaned_df['quantity'].isnull().sum() == 0
    assert cleaned_df['unitprice'].isnull().sum() == 0

    # Column names should be lowercase and snake_case
    assert 'invoice_no' not in cleaned_df.columns  # snake_case not added manually, just lowercase
    assert all(col == col.lower().replace(" ", "_") for col in cleaned_df.columns)

def test_validate_records(sample_raw_df):
    transformer = DataTransformer()
    cleaned_df = transformer.clean_raw_data(sample_raw_df)
    valid_records, error_records = transformer.validate_records(cleaned_df)

    # Check that valid records are Pydantic-dict format
    assert isinstance(valid_records, list)
    assert isinstance(valid_records[0], dict)

    # Check if each error record has row_index and error fields
    for error in error_records:
        assert 'row_index' in error
        assert 'errors' in error

def test_enhance_records(sample_raw_df):
    transformer = DataTransformer()
    cleaned_df = transformer.clean_raw_data(sample_raw_df)
    valid_records, _ = transformer.validate_records(cleaned_df)
    enhanced_records = transformer.enhance_records(valid_records)

    # Check structure of enhanced record
    assert isinstance(enhanced_records, list)
    if enhanced_records:
        record = enhanced_records[0]
        assert 'total_amount' in record
        assert 'is_return' in record

def test_generate_data_quality_report(sample_raw_df):
    transformer = DataTransformer()
    cleaned_df = transformer.clean_raw_data(sample_raw_df)
    valid_records, _ = transformer.validate_records(cleaned_df)
    enhanced_records = transformer.enhance_records(valid_records)
    report = transformer.generate_data_quality_report(cleaned_df, enhanced_records)

    assert 'original_records' in report
    assert 'processed_records' in report
    assert 'returns_percentage' in report
    assert 'total_revenue' in report
