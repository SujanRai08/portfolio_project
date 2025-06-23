import unittest
import pandas as pd
from unittest.mock import patch, MagicMock
from src.load.loader import DatabaseLoader

class TestDatabaseLoader(unittest.TestCase):
    @patch('src.load.loader.create_engine')
    def test_connect_todatabase_success(self,mock_create_engine):
        mock_conn = MagicMock()
        mock_conn.execute.return_value = None
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        mock_create_engine.return_value = mock_engine
        loader = DatabaseLoader()
        self.assertIsNotNone(loader.engine)
        self.assertEqual(mock_conn.execute.call_count, 1)

    @patch("src.load.loader.create_engine", side_effect=Exception("DB fail"))
    def test_connect_to_database_fail(self,mock_create_engine):
        loader = DatabaseLoader()
        self.assertIsNone(loader.engine)

    @patch("src.load.loader.open", create=True)
    @patch("src.load.loader.create_engine")
    def test_create_tables(self,mock_create_engine,mock_open):
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_create_engine.return_value = mock_engine
        mock_open.return_value.__enter__.return_value.read.return_value = "CREATE TABLE dummy_table();"

        loader = DatabaseLoader()
        loader.create_tables()

        self.assertEqual(mock_conn.execute.call_count, 2)
        mock_conn.commit.assert_called_once()

    @patch('src.load.loader.create_engine')
    def test_load_data(self,mock_create_engine):
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        loader = DatabaseLoader()
        loader.engine = mock_engine

        records = [{"customer_id": "123", "amount": 10.5}]
        with patch("pandas.DataFrame.to_sql") as mock_to_sql:
            loader.load_data(records, table_name="test_table")
            mock_to_sql.assert_called_once()

    @patch("src.load.loader.create_engine")
    def test_execute_analysis_queries(self, mock_create_engine):
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_create_engine.return_value = mock_engine

        mock_df = pd.DataFrame([{"dummy": "data"}])
        with patch("pandas.read_sql", return_value=mock_df):
            loader = DatabaseLoader()
            loader.engine = mock_engine
            results = loader.execute_analysis_queries()
            self.assertTrue("top_products" in results or len(results) >= 1)

    @patch("src.load.loader.create_engine")
    def test_get_data_summary(self, mock_create_engine):
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_create_engine.return_value = mock_engine

        mock_df = pd.DataFrame([{
            "total_transactions": 100,
            "unique_customers": 10,
            "unique_products": 20,
            "unique_countries": 5,
            "total_revenue": 999.99,
            "return_count": 3,
            "earliest_transaction": "2021-01-01",
            "latest_transaction": "2021-12-31"
        }])

        with patch("pandas.read_sql", return_value=mock_df):
            loader = DatabaseLoader()
            loader.engine = mock_engine
            summary = loader.get_data_summary()

            self.assertEqual(summary["total_transactions"], 100)

    
    

