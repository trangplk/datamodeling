-- ============================================================================
-- DATA WAREHOUSE PROJECT - STARTER CODE PACKAGE
-- Complete SQL scripts with operational data schema and sample data
-- ============================================================================

-- ============================================================================
-- SECTION 1: OPERATIONAL DATABASE SETUP (ecommerce_ops)
-- ============================================================================

-- Create operational database schema
CREATE DATABASE ecommerce_ops;
\c ecommerce_ops;

-- Customers table with sample data
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    registration_date DATE,
    city VARCHAR(100),
    state VARCHAR(50),
    country VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Products table with sample data
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    price DECIMAL(10,2),
    cost DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Orders table
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    order_date DATE NOT NULL,
    total_amount DECIMAL(10,2),
    tax_amount DECIMAL(10,2),
    shipping_amount DECIMAL(10,2),
    order_status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Order items table
CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(order_id),
    product_id INTEGER REFERENCES products(product_id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2),
    line_total DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Marketing campaigns table (additional context)
CREATE TABLE campaigns (
    campaign_id SERIAL PRIMARY KEY,
    campaign_name VARCHAR(255),
    channel VARCHAR(100),
    start_date DATE,
    end_date DATE,
    budget DECIMAL(12,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Customer campaign attribution
CREATE TABLE customer_acquisitions (
    customer_id INTEGER REFERENCES customers(customer_id),
    campaign_id INTEGER REFERENCES campaigns(campaign_id),
    acquisition_date DATE,
    PRIMARY KEY (customer_id, campaign_id)
);

-- ============================================================================
-- SECTION 2: SAMPLE DATA INSERTION
-- ============================================================================

-- Insert sample customers
INSERT INTO customers (email, first_name, last_name, registration_date, city, state, country) VALUES
('john.doe@email.com', 'John', 'Doe', '2023-01-15', 'New York', 'NY', 'USA'),
('jane.smith@email.com', 'Jane', 'Smith', '2023-02-20', 'Los Angeles', 'CA', 'USA'),
('bob.wilson@email.com', 'Bob', 'Wilson', '2023-03-10', 'Chicago', 'IL', 'USA'),
('alice.brown@email.com', 'Alice', 'Brown', '2023-04-05', 'Houston', 'TX', 'USA'),
('charlie.davis@email.com', 'Charlie', 'Davis', '2023-05-18', 'Phoenix', 'AZ', 'USA'),
('diana.miller@email.com', 'Diana', 'Miller', '2023-06-22', 'Philadelphia', 'PA', 'USA'),
('edward.garcia@email.com', 'Edward', 'Garcia', '2023-07-14', 'San Antonio', 'TX', 'USA'),
('fiona.rodriguez@email.com', 'Fiona', 'Rodriguez', '2023-08-30', 'San Diego', 'CA', 'USA'),
('george.martinez@email.com', 'George', 'Martinez', '2023-09-11', 'Dallas', 'TX', 'USA'),
('helen.anderson@email.com', 'Helen', 'Anderson', '2023-10-25', 'San Jose', 'CA', 'USA');

-- Insert sample products
INSERT INTO products (product_name, category, subcategory, brand, price, cost) VALUES
('Trail Running Shoes', 'Footwear', 'Running Shoes', 'OutdoorPro', 129.99, 65.00),
('Hiking Backpack 40L', 'Gear', 'Backpacks', 'MountainGear', 189.99, 95.00),
('Waterproof Jacket', 'Clothing', 'Jackets', 'WeatherShield', 249.99, 125.00),
('Camping Tent 4-Person', 'Gear', 'Tents', 'CampMaster', 399.99, 200.00),
('Sleeping Bag', 'Gear', 'Sleep Systems', 'ComfortCamp', 159.99, 80.00),
('Trekking Poles', 'Gear', 'Hiking Accessories', 'TrailBlazer', 89.99, 45.00),
('Fleece Pullover', 'Clothing', 'Mid-layers', 'WarmTech', 79.99, 40.00),
('Hiking Boots', 'Footwear', 'Hiking Boots', 'RockSolid', 199.99, 100.00),
('Daypack 25L', 'Gear', 'Backpacks', 'MountainGear', 99.99, 50.00),
('Rain Pants', 'Clothing', 'Rain Gear', 'WeatherShield', 129.99, 65.00);

-- Insert sample campaigns
INSERT INTO campaigns (campaign_name, channel, start_date, end_date, budget) VALUES
('Spring Sale 2023', 'Email', '2023-03-01', '2023-03-31', 15000.00),
('Summer Adventure', 'Social Media', '2023-06-01', '2023-08-31', 25000.00),
('Back to School', 'Google Ads', '2023-08-15', '2023-09-15', 12000.00),
('Holiday Gear Up', 'Email', '2023-11-01', '2023-12-31', 30000.00);

-- Insert sample orders (spread across different dates)
INSERT INTO orders (customer_id, order_date, total_amount, tax_amount, shipping_amount, order_status) VALUES
(1, '2023-03-20', 219.98, 17.60, 9.99, 'Completed'),
(2, '2023-04-15', 159.99, 12.80, 0.00, 'Completed'),
(1, '2023-05-10', 489.97, 39.20, 19.99, 'Completed'),
(3, '2023-06-05', 329.98, 26.40, 14.99, 'Completed'),
(4, '2023-06-20', 89.99, 7.20, 9.99, 'Completed'),
(2, '2023-07-14', 279.98, 22.40, 9.99, 'Completed'),
(5, '2023-08-01', 189.99, 15.20, 9.99, 'Completed'),
(1, '2023-08-25', 129.99, 10.40, 0.00, 'Completed'),
(6, '2023-09-10', 449.98, 36.00, 19.99, 'Completed'),
(7, '2023-10-05', 199.99, 16.00, 9.99, 'Completed');

-- Insert sample order items
INSERT INTO order_items (order_id, product_id, quantity, unit_price, line_total) VALUES
-- Order 1: Trail Running Shoes + Trekking Poles
(1, 1, 1, 129.99, 129.99),
(1, 6, 1, 89.99, 89.99),
-- Order 2: Sleeping Bag
(2, 5, 1, 159.99, 159.99),
-- Order 3: Camping Tent + Waterproof Jacket + Daypack
(3, 4, 1, 399.99, 399.99),
(3, 3, 1, 249.99, 249.99),
(3, 9, 1, 99.99, 99.99),
-- Order 4: Hiking Backpack + Fleece Pullover
(4, 2, 1, 189.99, 189.99),
(4, 7, 1, 79.99, 79.99),
-- Order 5: Trekking Poles
(5, 6, 1, 89.99, 89.99),
-- Order 6: Hiking Boots + Rain Pants
(6, 8, 1, 199.99, 199.99),
(6, 10, 1, 129.99, 129.99),
-- Order 7: Hiking Backpack
(7, 2, 1, 189.99, 189.99),
-- Order 8: Rain Pants
(8, 10, 1, 129.99, 129.99),
-- Order 9: Camping Tent + Sleeping Bag
(9, 4, 1, 399.99, 399.99),
(9, 5, 1, 159.99, 159.99),
-- Order 10: Hiking Boots
(10, 8, 1, 199.99, 199.99);

-- Insert customer acquisition data
INSERT INTO customer_acquisitions (customer_id, campaign_id, acquisition_date) VALUES
(1, 1, '2023-01-15'),
(2, 1, '2023-02-20'),
(3, 1, '2023-03-10'),
(4, 2, '2023-04-05'),
(5, 2, '2023-05-18'),
(6, 2, '2023-06-22'),
(7, 3, '2023-07-14'),
(8, 3, '2023-08-30'),
(9, 3, '2023-09-11'),
(10, 4, '2023-10-25');

-- ============================================================================
-- SECTION 3: DATA WAREHOUSE DATABASE SETUP (ecommerce_dw)
-- ============================================================================

-- Create data warehouse database
CREATE DATABASE ecommerce_dw;
\c ecommerce_dw;

-- YOUR CODE HERE: Create your star schema tables
-- Hint: You'll need dimension tables for customers, products, dates, and campaigns
-- Plus a fact table for order items with foreign keys to all dimensions

-- Example dimension table structure (you'll need to complete this):
/*
CREATE TABLE dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    -- Add other customer attributes here
    -- Don't forget SCD fields: effective_date, expiry_date, is_current
);
*/

-- YOUR CODE HERE: Create indexes for performance optimization

-- ============================================================================
-- SECTION 4: UPDATE TIMESTAMP TRIGGERS (for change tracking)
-- ============================================================================

-- Function to update modified timestamp
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply triggers to operational tables for change tracking
-- (Switch back to operational database first)
\c ecommerce_ops;

CREATE TRIGGER update_customers_modtime BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_products_modtime BEFORE UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

CREATE TRIGGER update_orders_modtime BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

-- ============================================================================
-- SECTION 5: YOUR IMPLEMENTATION SECTIONS
-- ============================================================================

-- YOUR CODE HERE: Add normalization improvements to operational schema
-- Consider what indexes might improve query performance

-- YOUR CODE HERE: Implement your star schema design
-- Remember to include surrogate keys, SCD support, and proper indexing

-- YOUR CODE HERE: Write window function queries
-- Examples: rolling sales, customer rankings, period-over-period analysis

-- YOUR CODE HERE: Configure replication setup
-- Note: Actual replication setup requires server-level configuration

-- YOUR CODE HERE: Build incremental ETL procedures
-- Include change detection, error handling, and logging

-- ============================================================================
-- SECTION 6: SAMPLE QUERIES TO TEST YOUR IMPLEMENTATION
-- ============================================================================

-- Sample analytical queries you should be able to answer:

-- 1. Rolling 30-day sales by product category
-- YOUR WINDOW FUNCTION QUERY HERE

-- 2. Top 10 customers by total spending with ranking
-- YOUR WINDOW FUNCTION QUERY HERE

-- 3. Product performance within each category
-- YOUR WINDOW FUNCTION QUERY HERE

-- 4. Customer acquisition cohort analysis
-- YOUR WINDOW FUNCTION QUERY HERE

-- ============================================================================
-- SECTION 7: TEST DATA GENERATION (Optional - for larger datasets)
-- ============================================================================

-- Uncomment and modify this section if you want to generate more test data:
/*
-- Generate additional customers
INSERT INTO customers (email, first_name, last_name, registration_date, city, state, country)
SELECT 
    'user' || generate_series(11, 1000) || '@email.com',
    'User',
    'Test' || generate_series(11, 1000),
    CURRENT_DATE - (random() * 365)::INTEGER,
    'City' || (random() * 50)::INTEGER,
    'ST',
    'USA';

-- Generate additional orders and order items
-- Add your data generation logic here
*/

-- ============================================================================
-- END OF STARTER CODE PACKAGE
-- Complete the YOUR CODE HERE sections to implement your data warehouse
-- ============================================================================