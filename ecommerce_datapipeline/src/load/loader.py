import pandas as pd
from sqlalchemy import create_engine,text
from typing import List,Dict
from src.utils.config import settings
from src.utils.logger import etl_logger

class DatabaseLoader:
    """
    Handles loading processed data into PostgreSQL database.
    """

    def __init__(self):
        self.logger = etl_logger
        self.engine = None
        self.connect_to_database()

    def connect_to_database(self):
        """
        Establish connection to PostgreSQL database...
        """
        try:
            self.engine  = create_engine(settings.db_url)
            # test connection 
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            self.logger.info("Successfully connected to postgreSQL database.. ")

        except Exception as e:
            self.logger.error(f"Failed to connect to database : {str(e)}")

            self.logger.warning("Database operation will be skipped")
            self.engine = None
    
    def create_tables(self):
        """creates necessary tables if they don;t exists"""
        if not self.engine:
            self.logger.warning("Noe database connection - skipping tables creation..")
            return
        try:
            with open('sql/create_tables.sql','r') as f:
                create_sql = f.read()
            with self.engine.connect() as conn:
                # execute each statement separately..
                statements = create_sql.split(';')
                for statement in statements:
                    statement = statement.strip()  # ✅ this fixes the bug
                    if statement:
                        conn.execute(text(statement))
                conn.commit()
            self.logger.info("Database tables created successfully...")

        except Exception as e:
            self.logger.error(f"Failed to create tables: {str(e)}")
            raise
    
    def load_data(self, enhanced_records: List[dict], table_name: str = 'retail_transactions'):
        """
        Load processed data into the database. 

        Args:
            enhanced_records: List of processed record dictionaries

            table_name: Target table name
        """
        if not self.engine:
            self.logger.warning("No database connection  - skipping data load...")
            return 
        try:
            df = pd.DataFrame(enhanced_records)
            self.logger.info(f"Loading {len(df)} records into table: { table_name}")

            df.to_sql(table_name,self.engine,if_exists='replace',index=True, method='multi')
            self.logger.info(f"Successfully loadded {len(df)} records into {table_name}")
        except Exception as e:
            self.logger.error(f"Failed to load data into database: {str(e)}")
            raise

    
    def execute_analysis_queries(self) -> Dict[str,pd.DataFrame]:
        """
        Execute common analysis queires and return results.
        Returns:
            Dict[str, pd.DataFrame]: Dictionary of query results
        """
        if not self.engine:
            self.logger.warning("No database connection - skipping analaysis queries")
            return {}
        results = {}
        queries = {
            'top_products': """
                SELECT 
                    stock_code,
                    description,
                    SUM(quantity) as total_quantity_sold,
                    SUM(total_amount) as total_revenue,
                    COUNT(*) as transaction_count
                FROM retail_transactions 
                WHERE is_return = false
                GROUP BY stock_code, description
                ORDER BY total_revenue DESC
                LIMIT 10
            """,
            
            'monthly_revenue': """
                SELECT 
                    year,
                    month,
                    SUM(total_amount) as monthly_revenue,
                    COUNT(*) as transaction_count,
                    COUNT(DISTINCT customer_id) as unique_customers
                FROM retail_transactions 
                WHERE is_return = false
                GROUP BY year, month
                ORDER BY year, month
            """,
            
            'top_countries': """
                SELECT 
                    country,
                    SUM(total_amount) as total_revenue,
                    COUNT(*) as transaction_count,
                    COUNT(DISTINCT customer_id) as unique_customers
                FROM retail_transactions 
                WHERE is_return = false
                GROUP BY country
                ORDER BY total_revenue DESC
                LIMIT 10
            """,
            
            'customer_analysis': """
                SELECT 
                    customer_id,
                    COUNT(*) as transaction_count,
                    SUM(total_amount) as total_spent,
                    AVG(total_amount) as avg_transaction_value,
                    MIN(invoice_date) as first_purchase,
                    MAX(invoice_date) as last_purchase
                FROM retail_transactions 
                WHERE is_return = false AND customer_id != 'UNKNOWN'
                GROUP BY customer_id
                HAVING COUNT(*) > 1
                ORDER BY total_spent DESC
                LIMIT 20
            """
        }
        try:
            with self.engine.connect() as conn:
                for query_name,query_sql in queries.items():
                    self.logger.info(f"Executing analaysis query: {query_name}")
                    result_df = pd.read_sql(query_sql,conn)
                    results[query_name] = result_df
                    self.logger.info(f"Query {query_name} returned {len(result_df)} rows...")

                    return results
        except Exception as e:
            self.logger.error(f"Failed to execute queries analysis: {str(e)}")
            return {}
        
    def get_data_summary(self) -> Dict:
        """
        Get basic data summary from the database.
        
        Returns:
            Dict: Summary statistics
        """
        if not self.engine:
            return {}
        
        try:
            with self.engine.connect() as conn:
                summary_query = """
                    SELECT 
                        COUNT(*) as total_transactions,
                        COUNT(DISTINCT customer_id) as unique_customers,
                        COUNT(DISTINCT stock_code) as unique_products,
                        COUNT(DISTINCT country) as unique_countries,
                        SUM(CASE WHEN is_return = false THEN total_amount ELSE 0 END) as total_revenue,
                        SUM(CASE WHEN is_return = true THEN 1 ELSE 0 END) as return_count,
                        MIN(invoice_date) as earliest_transaction,
                        MAX(invoice_date) as latest_transaction
                    FROM retail_transactions
                """
                
                result = pd.read_sql(summary_query, conn)
                return result.iloc[0].to_dict()
                
        except Exception as e:
            self.logger.error(f"Failed to get data summary: {str(e)}")
            return {}
        
    
