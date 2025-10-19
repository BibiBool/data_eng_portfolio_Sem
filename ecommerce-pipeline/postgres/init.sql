-- Create product category translation table first as it's referenced by products
CREATE TABLE product_categories (
    category_name VARCHAR(50) PRIMARY KEY,
    category_name_english VARCHAR(50) NOT NULL
);

-- Customers table
CREATE TABLE customers (
    customer_id VARCHAR(32) PRIMARY KEY,
    customer_unique_id VARCHAR(32) NOT NULL,
    customer_zip_code_prefix VARCHAR(5) NOT NULL,
    customer_city VARCHAR(100) NOT NULL,
    customer_state CHAR(2) NOT NULL
);

-- Sellers table
CREATE TABLE sellers (
    seller_id VARCHAR(32) PRIMARY KEY,
    seller_zip_code_prefix VARCHAR(5) NOT NULL,
    seller_city VARCHAR(100) NOT NULL,
    seller_state CHAR(2) NOT NULL
);

-- Products table
CREATE TABLE products (
    product_id VARCHAR(32) PRIMARY KEY,
    product_category_name VARCHAR(50) REFERENCES product_categories(category_name),
    product_name_length INTEGER,
    product_description_length INTEGER,
    product_photos_qty INTEGER,
    product_weight_g INTEGER,
    product_length_cm INTEGER,
    product_height_cm INTEGER,
    product_width_cm INTEGER
);

-- Orders table
CREATE TABLE orders (
    order_id VARCHAR(32) PRIMARY KEY,
    customer_id VARCHAR(32) REFERENCES customers(customer_id),
    order_status VARCHAR(20) NOT NULL,
    order_purchase_timestamp TIMESTAMP NOT NULL,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

-- Order items table
CREATE TABLE order_items (
    order_id VARCHAR(32) REFERENCES orders(order_id),
    order_item_id INTEGER NOT NULL,
    product_id VARCHAR(32) REFERENCES products(product_id),
    seller_id VARCHAR(32) REFERENCES sellers(seller_id),
    shipping_limit_date TIMESTAMP NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    freight_value NUMERIC(10,2) NOT NULL,
    PRIMARY KEY (order_id, order_item_id)
);

-- Order payments table
CREATE TABLE order_payments (
    order_id VARCHAR(32) REFERENCES orders(order_id),
    payment_sequential INTEGER NOT NULL,
    payment_type VARCHAR(20) NOT NULL,
    payment_installments INTEGER NOT NULL,
    payment_value NUMERIC(10,2) NOT NULL,
    PRIMARY KEY (order_id, payment_sequential)
);

-- Order reviews table
CREATE TABLE order_reviews (
    review_id VARCHAR(32) PRIMARY KEY,
    order_id VARCHAR(32) REFERENCES orders(order_id),
    review_score INTEGER NOT NULL,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP NOT NULL,
    review_answer_timestamp TIMESTAMP NOT NULL
);

-- Geolocation table
CREATE TABLE geolocations (
    geolocation_zip_code_prefix VARCHAR(5),
    geolocation_lat NUMERIC(15,12) NOT NULL,
    geolocation_lng NUMERIC(15,12) NOT NULL,
    geolocation_city VARCHAR(100) NOT NULL,
    geolocation_state CHAR(2) NOT NULL,
    PRIMARY KEY (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng)
);
