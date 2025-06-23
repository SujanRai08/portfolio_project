-- Sample queries for data analysis and reporting

-- 1. Top 10 best-selling products by revenue
SELECT 
    stock_code,
    description,
    SUM(quantity) as total_quantity_sold,
    SUM(total_amount) as total_revenue,
    COUNT(*) as transaction_count,
    AVG(unit_price) as avg_unit_price
FROM retail_transactions 
WHERE is_return = false
GROUP BY stock_code, description
ORDER BY total_revenue DESC
LIMIT 10;

-- 2. Monthly revenue trend
SELECT 
    year,
    month,
    TO_DATE(year || '-' || month || '-01', 'YYYY-MM-DD') as month_date,
    SUM(total_amount) as monthly_revenue,
    COUNT(*) as transaction_count,
    COUNT(DISTINCT customer_id) as unique_customers,
    AVG(total_amount) as avg_transaction_value
FROM retail_transactions 
WHERE is_return = false
GROUP BY year, month
ORDER BY year, month;

-- 3. Top countries by revenue
SELECT 
    country,
    SUM(total_amount) as total_revenue,
    COUNT(*) as transaction_count,
    COUNT(DISTINCT customer_id) as unique_customers,
    AVG(total_amount) as avg_transaction_value
FROM retail_transactions 
WHERE is_return = false
GROUP BY country
ORDER BY total_revenue DESC;

-- 4. Customer segmentation analysis
WITH customer_stats AS (
    SELECT 
        customer_id,
        COUNT(*) as transaction_count,
        SUM(total_amount) as total_spent,
        AVG(total_amount) as avg_transaction_value,
        MIN(invoice_date) as first_purchase,
        MAX(invoice_date) as last_purchase,
        MAX(invoice_date) - MIN(invoice_date) as customer_lifetime
    FROM retail_transactions 
    WHERE is_return = false AND customer_id != 'UNKNOWN'
    GROUP BY customer_id
)
SELECT 
    CASE 
        WHEN total_spent >= 10000 THEN 'VIP'
        WHEN total_spent >= 5000 THEN 'High Value'
        WHEN total_spent >= 1000 THEN 'Medium Value'
        ELSE 'Low Value'
    END as customer_segment,
    COUNT(*) as customer_count,
    AVG(total_spent) as avg_total_spent,
    AVG(transaction_count) as avg_transactions,
    AVG(avg_transaction_value) as avg_order_value
FROM customer_stats
GROUP BY 
    CASE 
        WHEN total_spent >= 10000 THEN 'VIP'
        WHEN total_spent >= 5000 THEN 'High Value'
        WHEN total_spent >= 1000 THEN 'Medium Value'
        ELSE 'Low Value'
    END
ORDER BY AVG(total_spent) DESC;

-- 5. Product return analysis
SELECT 
    stock_code,
    description,
    SUM(CASE WHEN is_return = false THEN quantity ELSE 0 END) as sold_quantity,
    SUM(CASE WHEN is_return = true THEN ABS(quantity) ELSE 0 END) as returned_quantity,
    ROUND(
        (SUM(CASE WHEN is_return = true THEN ABS(quantity) ELSE 0 END) * 100.0) / 
        NULLIF(SUM(CASE WHEN is_return = false THEN quantity ELSE 0 END), 0), 
        2
    ) as return_rate_percentage
FROM retail_transactions
GROUP BY stock_code, description
HAVING SUM(CASE WHEN is_return = false THEN quantity ELSE 0 END) > 0
ORDER BY return_rate_percentage DESC
LIMIT 20;

-- 6. Seasonal analysis
SELECT 
    month,
    CASE 
        WHEN month IN (12, 1, 2) THEN 'Winter'
        WHEN month IN (3, 4, 5) THEN 'Spring'
        WHEN month IN (6, 7, 8) THEN 'Summer'
        WHEN month IN (9, 10, 11) THEN 'Fall'
    END as season,
    SUM(total_amount) as seasonal_revenue,
    COUNT(*) as transaction_count,
    AVG(total_amount) as avg_transaction_value
FROM retail_transactions 
WHERE is_return = false
GROUP BY month
ORDER BY month;