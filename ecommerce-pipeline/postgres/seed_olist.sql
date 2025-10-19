-- Seed product categories
INSERT INTO product_categories (category_name, category_name_english) VALUES
('beleza_saude', 'health_beauty'),
('informatica_acessorios', 'computers_accessories'),
('moveis_decoracao', 'furniture_decor');

-- Seed customers
INSERT INTO customers (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state) VALUES
('8371223df9751aff1a37c7ab1841a240', 'a123b456c789d012e345f678g901h234', '01000', 'sao paulo', 'SP'),
('6721977bc71511e3e7ff91789b1e8b2a', 'b234c567d890e123f456g789h012i345', '20000', 'rio de janeiro', 'RJ'),
('3e9ac0875b5f8fd1dc817db6acd6c7d1', 'c345d678e901f234g567h890i123j456', '30000', 'belo horizonte', 'MG');

-- Seed sellers
INSERT INTO sellers (seller_id, seller_zip_code_prefix, seller_city, seller_state) VALUES
('3442f8959a84dea7ee197c632cb2df15', '12000', 'campinas', 'SP'),
('1f50f920176fa81dab994f9023523100', '13000', 'santos', 'SP'),
('d1b65fc7debc3361ea86b5f14c68d2e2', '14000', 'ribeirao preto', 'SP');

-- Seed products
INSERT INTO products (product_id, product_category_name, product_name_length, product_description_length, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm) VALUES
('8371223df97acd6c7d1841a2401e8b2a', 'beleza_saude', 30, 200, 3, 500, 20, 15, 10),
('6721977bc715acd6c7d1841a2401e8b2', 'informatica_acessorios', 25, 150, 2, 300, 30, 5, 25),
('3e9ac0875b5f8acd6c7d1841a2401e8b', 'moveis_decoracao', 35, 300, 4, 2000, 100, 50, 60);

-- Seed orders
INSERT INTO orders (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date) VALUES
('e481f51cbdc54678b7cc49136f2d6af7', '8371223df9751aff1a37c7ab1841a240', 'delivered', '2023-01-01 10:00:00', '2023-01-01 10:30:00', '2023-01-02 09:00:00', '2023-01-05 14:30:00', '2023-01-07 00:00:00'),
('b81ef226f3fe1789b1e8b2acac839d17', '6721977bc71511e3e7ff91789b1e8b2a', 'shipped', '2023-01-02 11:00:00', '2023-01-02 11:30:00', '2023-01-03 10:00:00', NULL, '2023-01-08 00:00:00'),
('a71ef226f3fe1789b1e8b2acac839d18', '3e9ac0875b5f8fd1dc817db6acd6c7d1', 'processing', '2023-01-03 12:00:00', '2023-01-03 12:30:00', NULL, NULL, '2023-01-09 00:00:00');

-- Seed order items
INSERT INTO order_items (order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value) VALUES
('e481f51cbdc54678b7cc49136f2d6af7', 1, '8371223df97acd6c7d1841a2401e8b2a', '3442f8959a84dea7ee197c632cb2df15', '2023-01-03 00:00:00', 59.90, 15.50),
('b81ef226f3fe1789b1e8b2acac839d17', 1, '6721977bc715acd6c7d1841a2401e8b2', '1f50f920176fa81dab994f9023523100', '2023-01-04 00:00:00', 129.90, 20.75),
('a71ef226f3fe1789b1e8b2acac839d18', 1, '3e9ac0875b5f8acd6c7d1841a2401e8b', 'd1b65fc7debc3361ea86b5f14c68d2e2', '2023-01-05 00:00:00', 499.90, 50.25);

-- Seed order payments
INSERT INTO order_payments (order_id, payment_sequential, payment_type, payment_installments, payment_value) VALUES
('e481f51cbdc54678b7cc49136f2d6af7', 1, 'credit_card', 3, 75.40),
('b81ef226f3fe1789b1e8b2acac839d17', 1, 'boleto', 1, 150.65),
('a71ef226f3fe1789b1e8b2acac839d18', 1, 'credit_card', 10, 550.15);

-- Seed order reviews
INSERT INTO order_reviews (review_id, order_id, review_score, review_comment_title, review_comment_message, review_creation_date, review_answer_timestamp) VALUES
('7bc2406110b926393aa56f80a40eba40', 'e481f51cbdc54678b7cc49136f2d6af7', 5, 'Great product', 'Very satisfied with the purchase', '2023-01-06 09:00:00', '2023-01-06 10:00:00'),
('6ac1305000b815482aa45f70b30eba41', 'b81ef226f3fe1789b1e8b2acac839d17', 4, 'Good product', 'Satisfied but delivery was a bit slow', '2023-01-07 10:00:00', '2023-01-07 11:00:00'),
('5ab0204889a704571bb34e60c20eba42', 'a71ef226f3fe1789b1e8b2acac839d18', 3, NULL, 'Average product', '2023-01-08 11:00:00', '2023-01-08 12:00:00');

-- Seed geolocations
INSERT INTO geolocations (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state) VALUES
('01000', -23.54562128115268, -46.63929204800168, 'sao paulo', 'SP'),
('20000', -22.90642879874123, -43.18223008567891, 'rio de janeiro', 'RJ'),
('30000', -19.91671234567890, -43.93451234567890, 'belo horizonte', 'MG');
