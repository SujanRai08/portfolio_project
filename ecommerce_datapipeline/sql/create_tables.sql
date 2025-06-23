-- Create the main retail transactions table
DROP TABLE IF EXISTS retail_transactions;

CREATE TABLE retail_transactions (
    id SERIAL PRIMARY KEY,
    invoice_no VARCHAR(50) NOT NULL,
    stock_code VARCHAR(50) NOT NULL,
    description TEXT,
    quantity INTEGER NOT NULL,
    invoice_date TIMESTAMP NOT NULL,
    unit_price DECIMAL(10,4) NOT NULL,
    customer_id VARCHAR(50),
    country VARCHAR(100) NOT NULL,
    total_amount DECIMAL(12,4) NOT NULL,
    is_return BOOLEAN NOT NULL DEFAULT FALSE,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- create indexes for better query performance #
CREATE INDEX idx_retail_invoice_no ON retail_transactions(invoice_no);
CREATE INDEX idx_retail_customer_id ON retail_transactions(customer_id);
CREATE INDEX idx_retail_stock_code ON retail_transactions(stock_code);
CREATE INDEX idx_retail_country ON retail_transactions(country);
CREATE INDEX idx_retail_invoice_date ON retail_transactions(invoice_date);
CREATE INDEX idx_retail_year_month ON retail_transactions(year, month);
CREATE INDEX idx_retail_is_return ON retail_transactions(is_return);

-- Create a summary table for quick analytics
DROP TABLE IF EXISTS retail_summary;

CREATE TABLE retail_summary (
    id SERIAL PRIMARY KEY,
    summary_date DATE DEFAULT CURRENT_DATE,
    total_transactions INTEGER,
    unique_customers INTEGER,
    unique_products INTEGER,
    unique_countries INTEGER,
    total_revenue DECIMAL(15,2),
    return_count INTEGER,
    data_quality_score DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create product performance table
DROP TABLE IF EXISTS product_performance;

CREATE TABLE product_performance (
    id SERIAL PRIMARY KEY,
    stock_code VARCHAR(50) NOT NULL,
    description TEXT,
    total_quantity_sold INTEGER,
    total_revenue DECIMAL(12,4),
    transaction_count INTEGER,
    unique_customers INTEGER,
    avg_unit_price DECIMAL(10,4),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_code)
);

-- Create customer analysis table
DROP TABLE IF EXISTS customer_analysis;

CREATE TABLE customer_analysis (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    total_transactions INTEGER,
    total_spent DECIMAL(12,4),
    avg_transaction_value DECIMAL(10,4),
    first_purchase_date TIMESTAMP,
    last_purchase_date TIMESTAMP,
    favorite_product VARCHAR(50),
    primary_country VARCHAR(100),
    customer_lifetime_days INTEGER,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(customer_id)
);