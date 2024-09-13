CREATE TABLE IF NOT EXISTS product_category (
    category_id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);


CREATE TABLE IF NOT EXISTS products (
    product_id UUID PRIMARY KEY,
    name VARCHAR(255),
    category_id INTEGER,
    weight DECIMAL,
    price DECIMAL,
    FOREIGN KEY (category_id) REFERENCES product_category(category_id) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS customers (
    customer_id UUID PRIMARY KEY,
    name VARCHAR(100),
    surname VARCHAR(100),
    address TEXT,
    state VARCHAR(100),
    gender VARCHAR(10)
);


CREATE TABLE IF NOT EXISTS orders (
    order_id UUID PRIMARY KEY,
    customer_id UUID,
    order_time TIMESTAMP,
    shipping_time TIMESTAMP,
    status VARCHAR(100),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS shopping_carts (
    order_id UUID,
    product_id UUID,
    amount INTEGER,
    PRIMARY KEY (order_id, product_id),
    FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS refunds (
    order_id UUID PRIMARY KEY,
    reason TEXT,
    FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE
);


