-- Seed Customers
INSERT INTO customers (first_name, last_name, email)
VALUES 
('Alice', 'Smith', 'alice@example.com'),
('Bob', 'Johnson', 'bob@example.com'),
('Charlie', 'Brown', 'charlie@example.com');

-- Seed Products
INSERT INTO products (name, category, price)
VALUES
('Laptop', 'Electronics', 1200.00),
('Headphones', 'Electronics', 150.00),
('Coffee Maker', 'Home Appliances', 80.00);

-- Seed Orders
INSERT INTO orders (customer_id, product_id, quantity)
SELECT c.customer_id, p.product_id, 1
FROM customers c, products p
LIMIT 5;
