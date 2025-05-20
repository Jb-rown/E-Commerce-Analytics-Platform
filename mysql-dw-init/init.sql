CREATE DATABASE ecommerce_dw;
USE ecommerce_dw;

-- Dimension Tables
CREATE TABLE dim_customers (
    customer_key INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT NOT NULL,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50),
    registration_date DATE,
    effective_date DATE NOT NULL,
    expiry_date DATE,
    current_flag BOOLEAN DEFAULT TRUE,
    UNIQUE KEY (customer_id, effective_date)
);

CREATE TABLE dim_products (
    product_key INT AUTO_INCREMENT PRIMARY KEY,
    product_id INT NOT NULL,
    product_name VARCHAR(100),
    category_name VARCHAR(50),
    parent_category_name VARCHAR(50),
    price DECIMAL(10, 2),
    cost DECIMAL(10, 2),
    effective_date DATE NOT NULL,
    expiry_date DATE,
    current_flag BOOLEAN DEFAULT TRUE,
    UNIQUE KEY (product_id, effective_date)
);

CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    day_of_week TINYINT NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    day_of_month TINYINT NOT NULL,
    day_of_year SMALLINT NOT NULL,
    week_of_year TINYINT NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    month_of_year TINYINT NOT NULL,
    quarter TINYINT NOT NULL,
    year INT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    UNIQUE KEY (full_date)
);

-- Fact Tables
CREATE TABLE fact_sales (
    sale_key INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT NOT NULL,
    customer_key INT NOT NULL,
    product_key INT NOT NULL,
    date_key INT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    subtotal DECIMAL(10, 2) NOT NULL,
    cost_amount DECIMAL(10, 2) NOT NULL,
    profit DECIMAL(10, 2) GENERATED ALWAYS AS (subtotal - cost_amount) STORED,
    FOREIGN KEY (customer_key) REFERENCES dim_customers(customer_key),
    FOREIGN KEY (product_key) REFERENCES dim_products(product_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    INDEX idx_customer_product (customer_key, product_key),
    INDEX idx_date (date_key)
);

CREATE TABLE fact_inventory (
    inventory_key INT AUTO_INCREMENT PRIMARY KEY,
    product_key INT NOT NULL,
    date_key INT NOT NULL,
    starting_quantity INT NOT NULL,
    ending_quantity INT NOT NULL,
    items_sold INT NOT NULL,
    items_received INT NOT NULL,
    items_adjusted INT NOT NULL,
    FOREIGN KEY (product_key) REFERENCES dim_products(product_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    UNIQUE KEY (product_key, date_key)
);