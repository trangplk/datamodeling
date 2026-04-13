-- SECTION 1: NORMALIZED OPERATIONAL SCHEMA IMPROVEMENTS

-- ============================================================================

-- The provided schema is already in 3NF, but we can add some optimizations

-- Add indexes for common query patterns

CREATE INDEX idx_customers_registration_date ON customers(registration_date);

CREATE INDEX idx_customers_location ON customers(city, state, country);

CREATE INDEX idx_orders_customer_date ON orders(customer_id, order_date);

CREATE INDEX idx_orders_status_date ON orders(order_status, order_date);

CREATE INDEX idx_order_items_product ON order_items(product_id);

CREATE INDEX idx_products_category ON products(category, subcategory);

-- Add update timestamp triggers for change tracking

CREATE OR REPLACE FUNCTION update_modified_column()

RETURNS TRIGGER AS $$

BEGIN

    NEW.updated_at = CURRENT_TIMESTAMP;

    RETURN NEW;

END;

$$ language 'plpgsql';

CREATE TRIGGER update_customers_modtime BEFORE UPDATE ON customers

    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_products_modtime BEFORE UPDATE ON products

    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_orders_modtime BEFORE UPDATE ON orders

    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

-- ============================================================================

-- SECTION 2: DATA WAREHOUSE STAR SCHEMA DESIGN

-- ============================================================================

-- Switch to data warehouse database

-- \c ecommerce_dw;

-- Dimension Tables (denormalized for analytical performance)

CREATE TABLE dim_customer (

    customer_key SERIAL PRIMARY KEY,

    customer_id INTEGER NOT NULL,

    email VARCHAR(255),

    full_name VARCHAR(255),

    first_name VARCHAR(100),

    last_name VARCHAR(100),

    registration_date DATE,

    city VARCHAR(100),

    state VARCHAR(50),

    country VARCHAR(50),

    customer_segment VARCHAR(50), -- Derived attribute for analytics

    effective_date DATE DEFAULT CURRENT_DATE,

    expiry_date DATE DEFAULT '9999-12-31',

    is_current BOOLEAN DEFAULT TRUE

);

CREATE TABLE dim_product (

    product_key SERIAL PRIMARY KEY,

    product_id INTEGER NOT NULL,

    product_name VARCHAR(255),

    category VARCHAR(100),

    subcategory VARCHAR(100),

    brand VARCHAR(100),

    current_price DECIMAL(10,2),

    current_cost DECIMAL(10,2),

    profit_margin DECIMAL(5,4), -- Calculated field

    effective_date DATE DEFAULT CURRENT_DATE,

    expiry_date DATE DEFAULT '9999-12-31',

    is_current BOOLEAN DEFAULT TRUE

);

CREATE TABLE dim_date (

    date_key INTEGER PRIMARY KEY, -- Format: YYYYMMDD

    date_value DATE NOT NULL,

    day_of_week INTEGER,

    day_name VARCHAR(10),

    day_of_month INTEGER,

    day_of_year INTEGER,

    week_of_year INTEGER,

    month_number INTEGER,

    month_name VARCHAR(10),

    quarter INTEGER,

    quarter_name VARCHAR(2),

    year INTEGER,

    is_weekend BOOLEAN,

    is_holiday BOOLEAN

);

-- Populate date dimension for 5 years

INSERT INTO dim_date (date_key, date_value, day_of_week, day_name, day_of_month, 

                      day_of_year, week_of_year, month_number, month_name, 

                      quarter, quarter_name, year, is_weekend, is_holiday)

SELECT 

    TO_CHAR(date_series, 'YYYYMMDD')::INTEGER as date_key,

    date_series,

    EXTRACT(DOW FROM date_series),

    TO_CHAR(date_series, 'Day'),

    EXTRACT(DAY FROM date_series),

    EXTRACT(DOY FROM date_series),

    EXTRACT(WEEK FROM date_series),

    EXTRACT(MONTH FROM date_series),

    TO_CHAR(date_series, 'Month'),

    EXTRACT(QUARTER FROM date_series),

    'Q' || EXTRACT(QUARTER FROM date_series),

    EXTRACT(YEAR FROM date_series),

    CASE WHEN EXTRACT(DOW FROM date_series) IN (0,6) THEN TRUE ELSE FALSE END,

    FALSE -- Simplified - could add holiday logic

FROM generate_series('2020-01-01'::DATE, '2024-12-31'::DATE, '1 day') AS date_series;

-- Fact Table

CREATE TABLE fact_order_items (

    order_item_key SERIAL PRIMARY KEY,

    customer_key INTEGER REFERENCES dim_customer(customer_key),

    product_key INTEGER REFERENCES dim_product(product_key),

    order_date_key INTEGER REFERENCES dim_date(date_key),

    order_id INTEGER NOT NULL,

    order_item_id INTEGER NOT NULL,

    quantity INTEGER NOT NULL,

    unit_price DECIMAL(10,2),

    unit_cost DECIMAL(10,2),

    line_total DECIMAL(10,2),

    line_profit DECIMAL(10,2),

    tax_amount DECIMAL(10,2),

    shipping_amount DECIMAL(10,2),

    order_total DECIMAL(10,2),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);

-- Indexes for analytical queries

CREATE INDEX idx_fact_customer ON fact_order_items(customer_key);

CREATE INDEX idx_fact_product ON fact_order_items(product_key);

CREATE INDEX idx_fact_date ON fact_order_items(order_date_key);

CREATE INDEX idx_fact_order ON fact_order_items(order_id);

CREATE INDEX idx_fact_composite ON fact_order_items(order_date_key, customer_key, product_key);

-- ============================================================================

-- SECTION 3: ADVANCED ANALYTICS WITH WINDOW FUNCTIONS

-- ============================================================================

-- Query 1: Rolling 30-day sales by product category

CREATE VIEW rolling_sales_by_category AS

SELECT 

    d.date_value,

    p.category,

    SUM(f.line_total) as daily_sales,

    SUM(SUM(f.line_total)) OVER (

        PARTITION BY p.category 

        ORDER BY d.date_value 

        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW

    ) as rolling_30_day_sales,

    AVG(SUM(f.line_total)) OVER (

        PARTITION BY p.category 

        ORDER BY d.date_value 

        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW

    ) as rolling_30_day_avg

FROM fact_order_items f

JOIN dim_date d ON f.order_date_key = d.date_key

JOIN dim_product p ON f.product_key = p.product_key

GROUP BY d.date_value, p.category

ORDER BY p.category, d.date_value;

-- Query 2: Customer ranking and segmentation

CREATE VIEW customer_analytics AS

SELECT 

    c.customer_id,

    c.full_name,

    c.registration_date,

    COUNT(DISTINCT f.order_id) as total_orders,

    SUM(f.line_total) as total_spent,

    AVG(f.line_total) as avg_order_value,

    MAX(d.date_value) as last_order_date,

    -- Ranking customers by total spent

    RANK() OVER (ORDER BY SUM(f.line_total) DESC) as spending_rank,

    -- Percentile ranking for segmentation

    NTILE(10) OVER (ORDER BY SUM(f.line_total)) as spending_decile,

    -- Recency analysis

    CURRENT_DATE - MAX(d.date_value) as days_since_last_order,

    -- Customer lifetime calculation

    CURRENT_DATE - c.registration_date as customer_lifetime_days,

    -- Period over period growth

    LAG(SUM(f.line_total), 1) OVER (

        PARTITION BY c.customer_id 

        ORDER BY EXTRACT(YEAR FROM d.date_value), EXTRACT(QUARTER FROM d.date_value)

    ) as previous_period_spend

FROM dim_customer c

JOIN fact_order_items f ON c.customer_key = f.customer_key

JOIN dim_date d ON f.order_date_key = d.date_key

WHERE c.is_current = TRUE

GROUP BY c.customer_id, c.full_name, c.registration_date, 

         EXTRACT(YEAR FROM d.date_value), EXTRACT(QUARTER FROM d.date_value);

-- Query 3: Product performance with rankings

CREATE VIEW product_performance AS

SELECT 

    p.product_name,

    p.category,

    p.brand,

    SUM(f.quantity) as total_quantity_sold,

    SUM(f.line_total) as total_revenue,

    SUM(f.line_profit) as total_profit,

    AVG(f.unit_price) as avg_selling_price,

    -- Rankings within category

    RANK() OVER (PARTITION BY p.category ORDER BY SUM(f.line_total) DESC) as revenue_rank_in_category,

    RANK() OVER (PARTITION BY p.category ORDER BY SUM(f.line_profit) DESC) as profit_rank_in_category,

    -- Cumulative metrics

    SUM(SUM(f.line_total)) OVER (

        PARTITION BY p.category 

        ORDER BY SUM(f.line_total) DESC

        ROWS UNBOUNDED PRECEDING

    ) as cumulative_category_revenue,

    -- Percentage of category total

    ROUND(

        100.0 * SUM(f.line_total) / SUM(SUM(f.line_total)) OVER (PARTITION BY p.category),

        2

    ) as pct_of_category_revenue

FROM dim_product p

JOIN fact_order_items f ON p.product_key = f.product_key

WHERE p.is_current = TRUE

GROUP BY p.product_id, p.product_name, p.category, p.brand;

-- ============================================================================

-- SECTION 4: DATABASE REPLICATION CONFIGURATION

-- ============================================================================

-- On Master Database (ecommerce_ops):

-- 1. Edit postgresql.conf:

--    wal_level = replica

--    max_wal_senders = 3

--    max_replication_slots = 3

--    synchronous_commit = on

-- 2. Edit pg_hba.conf:

--    host replication replica_user 0.0.0.0/0 md5

-- Create replication user

CREATE ROLE replica_user WITH REPLICATION LOGIN PASSWORD 'secure_password';

-- Create replication slot

SELECT pg_create_physical_replication_slot('replica_slot');

-- On Replica Database:

-- Use pg_basebackup to create initial replica:

-- pg_basebackup -h master_host -D /path/to/replica -U replica_user -P -W -R

-- Create recovery.conf:

-- standby_mode = 'on'

-- primary_conninfo = 'host=master_host port=5432 user=replica_user'

-- primary_slot_name = 'replica_slot'

-- Monitoring replication lag query:

CREATE VIEW replication_status AS

SELECT 

    client_addr,

    state,

    pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn) as send_lag,

    pg_wal_lsn_diff(pg_current_wal_lsn(), write_lsn) as write_lag,

    pg_wal_lsn_diff(pg_current_wal_lsn(), flush_lsn) as flush_lag,

    pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) as replay_lag

FROM pg_stat_replication;

-- ============================================================================

-- SECTION 5: INCREMENTAL ETL PROCESSES

-- ============================================================================

-- Create ETL control table to track load status

CREATE TABLE etl_control (

    table_name VARCHAR(100) PRIMARY KEY,

    last_load_timestamp TIMESTAMP,

    last_load_status VARCHAR(20),

    rows_processed INTEGER,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);

-- Initialize control table

INSERT INTO etl_control (table_name, last_load_timestamp, last_load_status, rows_processed)

VALUES 

    ('dim_customer', '1900-01-01', 'SUCCESS', 0),

    ('dim_product', '1900-01-01', 'SUCCESS', 0),

    ('fact_order_items', '1900-01-01', 'SUCCESS', 0);

-- Customer dimension ETL procedure

CREATE OR REPLACE FUNCTION load_dim_customer()

RETURNS INTEGER AS $$

DECLARE

    last_load_ts TIMESTAMP;

    rows_affected INTEGER := 0;

BEGIN

    -- Get last successful load timestamp

    SELECT last_load_timestamp INTO last_load_ts 

    FROM etl_control 

    WHERE table_name = 'dim_customer';

    

    -- Update ETL control status

    UPDATE etl_control 

    SET last_load_status = 'RUNNING', updated_at = CURRENT_TIMESTAMP

    WHERE table_name = 'dim_customer';

    

    -- Handle new customers (Type 1 SCD)

    INSERT INTO dim_customer (customer_id, email, full_name, first_name, last_name, 

                             registration_date, city, state, country, customer_segment)

    SELECT 

        c.customer_id,

        c.email,

        CONCAT(c.first_name, ' ', c.last_name),

        c.first_name,

        c.last_name,

        c.registration_date,

        c.city,

        c.state,

        c.country,

        CASE 

            WHEN c.registration_date >= CURRENT_DATE - INTERVAL '90 days' THEN 'New'

            ELSE 'Existing'

        END

    FROM ecommerce_ops.customers c

    WHERE c.updated_at > last_load_ts

    AND NOT EXISTS (

        SELECT 1 FROM dim_customer dc 

        WHERE dc.customer_id = c.customer_id AND dc.is_current = TRUE

    );

    

    GET DIAGNOSTICS rows_affected = ROW_COUNT;

    

    -- Update existing customers (Type 1 SCD - overwrite)

    UPDATE dim_customer SET

        email = c.email,

        full_name = CONCAT(c.first_name, ' ', c.last_name),

        first_name = c.first_name,

        last_name = c.last_name,

        city = c.city,

        state = c.state,

        country = c.country,

        effective_date = CURRENT_DATE

    FROM ecommerce_ops.customers c

    WHERE dim_customer.customer_id = c.customer_id

    AND dim_customer.is_current = TRUE

    AND c.updated_at > last_load_ts;

    

    -- Update ETL control table

    UPDATE etl_control 

    SET last_load_timestamp = CURRENT_TIMESTAMP,

        last_load_status = 'SUCCESS',

        rows_processed = rows_affected,

        updated_at = CURRENT_TIMESTAMP

    WHERE table_name = 'dim_customer';

    

    RETURN rows_affected;

    

EXCEPTION

    WHEN OTHERS THEN

        -- Log error and update status

        UPDATE etl_control 

        SET last_load_status = 'FAILED',

            updated_at = CURRENT_TIMESTAMP

        WHERE table_name = 'dim_customer';

        

        RAISE EXCEPTION 'Customer dimension load failed: %', SQLERRM;

END;

$$ LANGUAGE plpgsql;

-- Product dimension ETL procedure (similar pattern)

CREATE OR REPLACE FUNCTION load_dim_product()

RETURNS INTEGER AS $$

DECLARE

    last_load_ts TIMESTAMP;

    rows_affected INTEGER := 0;

BEGIN

    SELECT last_load_timestamp INTO last_load_ts 

    FROM etl_control 

    WHERE table_name = 'dim_product';

    

    UPDATE etl_control 

    SET last_load_status = 'RUNNING', updated_at = CURRENT_TIMESTAMP

    WHERE table_name = 'dim_product';

    

    -- Insert new products

    INSERT INTO dim_product (product_id, product_name, category, subcategory, 

                           brand, current_price, current_cost, profit_margin)

    SELECT 

        p.product_id,

        p.product_name,

        p.category,

        p.subcategory,

        p.brand,

        p.price,

        p.cost,

        CASE WHEN p.price > 0 THEN (p.price - p.cost) / p.price ELSE 0 END

    FROM ecommerce_ops.products p

    WHERE p.updated_at > last_load_ts

    AND NOT EXISTS (

        SELECT 1 FROM dim_product dp 

        WHERE dp.product_id = p.product_id AND dp.is_current = TRUE

    );

    

    GET DIAGNOSTICS rows_affected = ROW_COUNT;

    

    -- Update existing products

    UPDATE dim_product SET

        product_name = p.product_name,

        category = p.category,

        subcategory = p.subcategory,

        brand = p.brand,

        current_price = p.price,

        current_cost = p.cost,

        profit_margin = CASE WHEN p.price > 0 THEN (p.price - p.cost) / p.price ELSE 0 END,

        effective_date = CURRENT_DATE

    FROM ecommerce_ops.products p

    WHERE dim_product.product_id = p.product_id

    AND dim_product.is_current = TRUE

    AND p.updated_at > last_load_ts;

    

    UPDATE etl_control 

    SET last_load_timestamp = CURRENT_TIMESTAMP,

        last_load_status = 'SUCCESS',

        rows_processed = rows_affected,

        updated_at = CURRENT_TIMESTAMP

    WHERE table_name = 'dim_product';

    

    RETURN rows_affected;

    

EXCEPTION

    WHEN OTHERS THEN

        UPDATE etl_control 

        SET last_load_status = 'FAILED',

            updated_at = CURRENT_TIMESTAMP

        WHERE table_name = 'dim_product';

        

        RAISE EXCEPTION 'Product dimension load failed: %', SQLERRM;

END;

$$ LANGUAGE plpgsql;

-- Fact table ETL procedure

CREATE OR REPLACE FUNCTION load_fact_order_items()

RETURNS INTEGER AS $$

DECLARE

    last_load_ts TIMESTAMP;

    rows_affected INTEGER := 0;

BEGIN

    SELECT last_load_timestamp INTO last_load_ts 

    FROM etl_control 

    WHERE table_name = 'fact_order_items';

    

    UPDATE etl_control 

    SET last_load_status = 'RUNNING', updated_at = CURRENT_TIMESTAMP

    WHERE table_name = 'fact_order_items';

    

    -- Load new fact records

    INSERT INTO fact_order_items (

        customer_key, product_key, order_date_key, order_id, order_item_id,

        quantity, unit_price, unit_cost, line_total, line_profit,

        tax_amount, shipping_amount, order_total

    )

    SELECT 

        dc.customer_key,

        dp.product_key,

        TO_CHAR(o.order_date, 'YYYYMMDD')::INTEGER,

        o.order_id,

        oi.order_item_id,

        oi.quantity,

        oi.unit_price,

        dp.current_cost,

        oi.line_total,

        oi.line_total - (oi.quantity * dp.current_cost),

        o.tax_amount * (oi.line_total / o.total_amount), -- Proportional tax

        o.shipping_amount * (oi.line_total / o.total_amount), -- Proportional shipping

        o.total_amount

    FROM ecommerce_ops.order_items oi

    JOIN ecommerce_ops.orders o ON oi.order_id = o.order_id

    JOIN dim_customer dc ON o.customer_id = dc.customer_id AND dc.is_current = TRUE

    JOIN dim_product dp ON oi.product_id = dp.product_id AND dp.is_current = TRUE

    WHERE oi.created_at > last_load_ts

    AND NOT EXISTS (

        SELECT 1 FROM fact_order_items f 

        WHERE f.order_item_id = oi.order_item_id

    );

    

    GET DIAGNOSTICS rows_affected = ROW_COUNT;

    

    UPDATE etl_control 

    SET last_load_timestamp = CURRENT_TIMESTAMP,

        last_load_status = 'SUCCESS',

        rows_processed = rows_affected,

        updated_at = CURRENT_TIMESTAMP

    WHERE table_name = 'fact_order_items';

    

    RETURN rows_affected;

    

EXCEPTION

    WHEN OTHERS THEN

        UPDATE etl_control 

        SET last_load_status = 'FAILED',

            updated_at = CURRENT_TIMESTAMP

        WHERE table_name = 'fact_order_items';

        

        RAISE EXCEPTION 'Fact table load failed: %', SQLERRM;

END;

$$ LANGUAGE plpgsql;

-- Master ETL orchestration procedure

CREATE OR REPLACE FUNCTION run_incremental_etl()

RETURNS TEXT AS $$

DECLARE

    result_message TEXT := '';

    customer_rows INTEGER;

    product_rows INTEGER;

    fact_rows INTEGER;

BEGIN

    -- Load dimensions first

    SELECT load_dim_customer() INTO customer_rows;

    result_message := result_message || 'Customers: ' || customer_rows || ' rows. ';

    

    SELECT load_dim_product() INTO product_rows;

    result_message := result_message || 'Products: ' || product_rows || ' rows. ';

    

    -- Load facts last

    SELECT load_fact_order_items() INTO fact_rows;

    result_message := result_message || 'Order Items: ' || fact_rows || ' rows. ';

    

    result_message := 'ETL completed successfully. ' || result_message;

    

    RETURN result_message;

    

EXCEPTION

    WHEN OTHERS THEN

        RETURN 'ETL failed: ' || SQLERRM;

END;

$$ LANGUAGE plpgsql;

-- Schedule ETL to run (example using pg_cron extension)

-- SELECT cron.schedule('nightly-etl', '0 2 * * *', 'SELECT run_incremental_etl();');