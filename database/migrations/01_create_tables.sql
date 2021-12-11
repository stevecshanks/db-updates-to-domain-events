CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL
);

CREATE TABLE invoices (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers (id),
    amount DECIMAL NOT NULL,
    is_paid BOOLEAN NOT NULL DEFAULT FALSE
);
