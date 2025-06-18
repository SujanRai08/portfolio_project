from src.extract.csv_extractor import ExcelExtractor
from src.utils.config import settings

def test_excel_extraction():
    extractor = ExcelExtractor()
    df = extractor.extract_from_excel(settings.local_excel_file)
    print("✅ Extraction successful!")
    print(f"Shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    print(df.head())

if __name__ == "__main__":
    test_excel_extraction()
