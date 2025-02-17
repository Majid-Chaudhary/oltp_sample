-- ========================================
-- SQL Script: Create Retail, Logistics, and Accounts Databases with Tables and Audit Tables
-- ========================================

-- Step 1: Create Retail Database
CREATE DATABASE retail;

-- Step 2: Create Logistics Database
CREATE DATABASE logistics;

-- Step 3: Create Accounts Database
CREATE DATABASE accounts;

-------------------------------------------------
-- Step 4: Create Tables for Retail Database
\c retail;

-------------------------
-- Main Tables in Retail
-------------------------

-- Settings Table
CREATE TABLE public.settings (
    setting_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    value INT NOT NULL,
    CONSTRAINT settings_unique UNIQUE (name)
);

-- Products Table
CREATE TABLE Products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    category_id VARCHAR(50),
    price NUMERIC(10, 2) NOT NULL,
    last_modified TIMESTAMP DEFAULT current_timestamp
);

-- Customers Table
CREATE TABLE Customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    address TEXT,
    phone VARCHAR(50),
    last_modified TIMESTAMP DEFAULT current_timestamp
);

-- Orders Table
CREATE TABLE Orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES Customers(customer_id),
    order_date TIMESTAMP DEFAULT NOW(),
    total_amount NUMERIC(10, 2) NOT NULL,
    last_modified TIMESTAMP DEFAULT current_timestamp
);

-- Order Items Table
CREATE TABLE Order_Items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INT REFERENCES Orders(order_id),
    product_id INT REFERENCES Products(product_id),
    quantity INT NOT NULL,
    subtotal NUMERIC(10, 2) NOT NULL,
    last_modified TIMESTAMP DEFAULT current_timestamp
);

-- Settings
INSERT INTO public.settings(name, value) VALUES ('batch_size', 100000);
INSERT INTO public.settings(name, value) VALUES ('pause_seconds', 60);
INSERT INTO public.settings(name, value) VALUES ('first_load', 1);
INSERT INTO public.settings(name, value) VALUES ('continous_loading', 1);

-------------------------
-- Audit Tables for Retail
-------------------------

-- Audit table for Products
CREATE TABLE Products_audit (
    audit_id SERIAL PRIMARY KEY,
    operation VARCHAR(10) NOT NULL,
    audit_timestamp TIMESTAMP DEFAULT current_timestamp,
    product_id INT,
    name VARCHAR(100),
    category_id VARCHAR(50),
    price NUMERIC(10, 2),
    last_modified TIMESTAMP
);

-- Audit table for Customers
CREATE TABLE Customers_audit (
    audit_id SERIAL PRIMARY KEY,
    operation VARCHAR(10) NOT NULL,
    audit_timestamp TIMESTAMP DEFAULT current_timestamp,
    customer_id INT,
    name VARCHAR(100),
    email VARCHAR(100),
    address TEXT,
    phone VARCHAR(50),
    last_modified TIMESTAMP
);

-- Audit table for Orders
CREATE TABLE Orders_audit (
    audit_id SERIAL PRIMARY KEY,
    operation VARCHAR(10) NOT NULL,
    audit_timestamp TIMESTAMP DEFAULT current_timestamp,
    order_id INT,
    customer_id INT,
    order_date TIMESTAMP,
    total_amount NUMERIC(10, 2),
    last_modified TIMESTAMP
);

-- Audit table for Order Items
CREATE TABLE Order_Items_audit (
    audit_id SERIAL PRIMARY KEY,
    operation VARCHAR(10) NOT NULL,
    audit_timestamp TIMESTAMP DEFAULT current_timestamp,
    order_item_id INT,
    order_id INT,
    product_id INT,
    quantity INT,
    subtotal NUMERIC(10, 2),
    last_modified TIMESTAMP
);

-------------------------------------------------
-- Step 5: Create Tables for Logistics Database
\c logistics;

-------------------------
-- Main Tables in Logistics
-------------------------

-- Warehouses Table
CREATE TABLE Warehouses (
    warehouse_id SERIAL PRIMARY KEY,
    location VARCHAR(100) NOT NULL,
    capacity INT NOT NULL,
    last_modified TIMESTAMP DEFAULT current_timestamp
);

-- Cities Table
CREATE TABLE Cities (
    city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL UNIQUE,
    last_modified TIMESTAMP DEFAULT current_timestamp
);

-- Shipments Table
CREATE TABLE Shipments (
    shipment_id SERIAL PRIMARY KEY,
    order_id INT NOT NULL,
    warehouse_id INT REFERENCES Warehouses(warehouse_id),
    shipment_date TIMESTAMP DEFAULT NOW(),
    delivery_date TIMESTAMP,
    total_amount NUMERIC(10,2),
    city_id INT REFERENCES Cities(city_id),
    last_modified TIMESTAMP DEFAULT current_timestamp
);

-------------------------
-- Audit Tables for Logistics
-------------------------

-- Audit table for Warehouses
CREATE TABLE Warehouses_audit (
    audit_id SERIAL PRIMARY KEY,
    operation VARCHAR(10) NOT NULL,
    audit_timestamp TIMESTAMP DEFAULT current_timestamp,
    warehouse_id INT,
    location VARCHAR(100),
    capacity INT,
    last_modified TIMESTAMP
);

-- Audit table for Cities
CREATE TABLE Cities_audit (
    audit_id SERIAL PRIMARY KEY,
    operation VARCHAR(10) NOT NULL,
    audit_timestamp TIMESTAMP DEFAULT current_timestamp,
    city_id INT,
    city_name VARCHAR(100),
    last_modified TIMESTAMP
);

-- Audit table for Shipments
CREATE TABLE Shipments_audit (
    audit_id SERIAL PRIMARY KEY,
    operation VARCHAR(10) NOT NULL,
    audit_timestamp TIMESTAMP DEFAULT current_timestamp,
    shipment_id INT,
    order_id INT,
    warehouse_id INT,
    shipment_date TIMESTAMP,
    delivery_date TIMESTAMP,
    total_amount NUMERIC(10,2),
    city_id INT,
    last_modified TIMESTAMP
);

-------------------------------------------------
-- Step 6: Create Tables for Accounts Database
\c accounts;

-------------------------
-- Main Tables in Accounts
-------------------------

-- Payment Methods Table
CREATE TABLE PaymentMethods (
    payment_method_id SERIAL PRIMARY KEY,
    method_name VARCHAR(50) NOT NULL UNIQUE,
    last_modified TIMESTAMP DEFAULT current_timestamp
);

-- Transactions Table
CREATE TABLE Transactions (
    transaction_id SERIAL PRIMARY KEY,
    order_id INT NOT NULL,
    customer_id INT NOT NULL,
    payment_method_id INT REFERENCES PaymentMethods(payment_method_id),
    transaction_date TIMESTAMP DEFAULT NOW(),
    amount NUMERIC(10,2) NOT NULL,
    last_modified TIMESTAMP DEFAULT current_timestamp
);

-------------------------
-- Audit Tables for Accounts
-------------------------

-- Audit table for PaymentMethods
CREATE TABLE PaymentMethods_audit (
    audit_id SERIAL PRIMARY KEY,
    operation VARCHAR(10) NOT NULL,
    audit_timestamp TIMESTAMP DEFAULT current_timestamp,
    payment_method_id INT,
    method_name VARCHAR(50),
    last_modified TIMESTAMP
);

-- Audit table for Transactions
CREATE TABLE Transactions_audit (
    audit_id SERIAL PRIMARY KEY,
    operation VARCHAR(10) NOT NULL,
    audit_timestamp TIMESTAMP DEFAULT current_timestamp,
    transaction_id INT,
    order_id INT,
    customer_id INT,
    payment_method_id INT,
    transaction_date TIMESTAMP,
    amount NUMERIC(10,2),
    last_modified TIMESTAMP
);

-- =====================================================
-- TRIGGER FUNCTIONS AND TRIGGERS FOR THE RETAIL DATABASE
-- =====================================================

\c retail;

-----------------------------------------------
-- 2. Products Table Trigger & Function
-----------------------------------------------
CREATE OR REPLACE FUNCTION audit_products() 
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') THEN
         INSERT INTO Products_audit(operation, product_id, name, category, price, stock_quantity, last_modified)
         VALUES ('INSERT', NEW.product_id, NEW.name, NEW.category, NEW.price, NEW.stock_quantity, NEW.last_modified);
         RETURN NEW;
    ELSIF (TG_OP = 'UPDATE') THEN
         INSERT INTO Products_audit(operation, product_id, name, category, price, stock_quantity, last_modified)
         VALUES ('UPDATE', NEW.product_id, NEW.name, NEW.category, NEW.price, NEW.stock_quantity, NEW.last_modified);
         RETURN NEW;
    ELSIF (TG_OP = 'DELETE') THEN
         INSERT INTO Products_audit(operation, product_id, name, category, price, stock_quantity, last_modified)
         VALUES ('DELETE', OLD.product_id, OLD.name, OLD.category, OLD.price, OLD.stock_quantity, OLD.last_modified);
         RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_audit_products
AFTER UPDATE OR DELETE ON Products
FOR EACH ROW EXECUTE FUNCTION audit_products();

-----------------------------------------------
-- 3. Customers Table Trigger & Function
-----------------------------------------------
CREATE OR REPLACE FUNCTION audit_customers() 
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') THEN
         INSERT INTO Customers_audit(operation, customer_id, name, email, address, phone, last_modified)
         VALUES ('INSERT', NEW.customer_id, NEW.name, NEW.email, NEW.address, NEW.phone, NEW.last_modified);
         RETURN NEW;
    ELSIF (TG_OP = 'UPDATE') THEN
         INSERT INTO Customers_audit(operation, customer_id, name, email, address, phone, last_modified)
         VALUES ('UPDATE', NEW.customer_id, NEW.name, NEW.email, NEW.address, NEW.phone, NEW.last_modified);
         RETURN NEW;
    ELSIF (TG_OP = 'DELETE') THEN
         INSERT INTO Customers_audit(operation, customer_id, name, email, address, phone, last_modified)
         VALUES ('DELETE', OLD.customer_id, OLD.name, OLD.email, OLD.address, OLD.phone, OLD.last_modified);
         RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_audit_customers
AFTER UPDATE OR DELETE ON Customers
FOR EACH ROW EXECUTE FUNCTION audit_customers();

-----------------------------------------------
-- 4. Orders Table Trigger & Function
-----------------------------------------------
CREATE OR REPLACE FUNCTION audit_orders() 
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') THEN
         INSERT INTO Orders_audit(operation, order_id, customer_id, order_date, total_amount, last_modified)
         VALUES ('INSERT', NEW.order_id, NEW.customer_id, NEW.order_date, NEW.total_amount, NEW.last_modified);
         RETURN NEW;
    ELSIF (TG_OP = 'UPDATE') THEN
         INSERT INTO Orders_audit(operation, order_id, customer_id, order_date, total_amount, last_modified)
         VALUES ('UPDATE', NEW.order_id, NEW.customer_id, NEW.order_date, NEW.total_amount, NEW.last_modified);
         RETURN NEW;
    ELSIF (TG_OP = 'DELETE') THEN
         INSERT INTO Orders_audit(operation, order_id, customer_id, order_date, total_amount, last_modified)
         VALUES ('DELETE', OLD.order_id, OLD.customer_id, OLD.order_date, OLD.total_amount, OLD.last_modified);
         RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_audit_orders
AFTER UPDATE OR DELETE ON Orders
FOR EACH ROW EXECUTE FUNCTION audit_orders();

-----------------------------------------------
-- 5. Order_Items Table Trigger & Function
-----------------------------------------------
CREATE OR REPLACE FUNCTION audit_order_items() 
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') THEN
         INSERT INTO Order_Items_audit(operation, order_item_id, order_id, product_id, quantity, subtotal, last_modified)
         VALUES ('INSERT', NEW.order_item_id, NEW.order_id, NEW.product_id, NEW.quantity, NEW.subtotal, NEW.last_modified);
         RETURN NEW;
    ELSIF (TG_OP = 'UPDATE') THEN
         INSERT INTO Order_Items_audit(operation, order_item_id, order_id, product_id, quantity, subtotal, last_modified)
         VALUES ('UPDATE', NEW.order_item_id, NEW.order_id, NEW.product_id, NEW.quantity, NEW.subtotal, NEW.last_modified);
         RETURN NEW;
    ELSIF (TG_OP = 'DELETE') THEN
         INSERT INTO Order_Items_audit(operation, order_item_id, order_id, product_id, quantity, subtotal, last_modified)
         VALUES ('DELETE', OLD.order_item_id, OLD.order_id, OLD.product_id, OLD.quantity, OLD.subtotal, OLD.last_modified);
         RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_audit_order_items
AFTER UPDATE OR DELETE ON Order_Items
FOR EACH ROW EXECUTE FUNCTION audit_order_items();

-- =====================================================
-- TRIGGER FUNCTIONS AND TRIGGERS FOR THE LOGISTICS DATABASE
-- =====================================================

\c logistics;

-----------------------------------------------
-- 6. Warehouses Table Trigger & Function
-----------------------------------------------
CREATE OR REPLACE FUNCTION audit_warehouses() 
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') THEN
         INSERT INTO Warehouses_audit(operation, warehouse_id, location, capacity, last_modified)
         VALUES ('INSERT', NEW.warehouse_id, NEW.location, NEW.capacity, NEW.last_modified);
         RETURN NEW;
    ELSIF (TG_OP = 'UPDATE') THEN
         INSERT INTO Warehouses_audit(operation, warehouse_id, location, capacity, last_modified)
         VALUES ('UPDATE', NEW.warehouse_id, NEW.location, NEW.capacity, NEW.last_modified);
         RETURN NEW;
    ELSIF (TG_OP = 'DELETE') THEN
         INSERT INTO Warehouses_audit(operation, warehouse_id, location, capacity, last_modified)
         VALUES ('DELETE', OLD.warehouse_id, OLD.location, OLD.capacity, OLD.last_modified);
         RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_audit_warehouses
AFTER UPDATE OR DELETE ON Warehouses
FOR EACH ROW EXECUTE FUNCTION audit_warehouses();

-----------------------------------------------
-- 7. Cities Table Trigger & Function
-----------------------------------------------
CREATE OR REPLACE FUNCTION audit_cities() 
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') THEN
         INSERT INTO Cities_audit(operation, city_id, city_name, last_modified)
         VALUES ('INSERT', NEW.city_id, NEW.city_name, NEW.last_modified);
         RETURN NEW;
    ELSIF (TG_OP = 'UPDATE') THEN
         INSERT INTO Cities_audit(operation, city_id, city_name, last_modified)
         VALUES ('UPDATE', NEW.city_id, NEW.city_name, NEW.last_modified);
         RETURN NEW;
    ELSIF (TG_OP = 'DELETE') THEN
         INSERT INTO Cities_audit(operation, city_id, city_name, last_modified)
         VALUES ('DELETE', OLD.city_id, OLD.city_name, OLD.last_modified);
         RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_audit_cities
AFTER UPDATE OR DELETE ON Cities
FOR EACH ROW EXECUTE FUNCTION audit_cities();

-----------------------------------------------
-- 8. Shipments Table Trigger & Function
-----------------------------------------------
CREATE OR REPLACE FUNCTION audit_shipments() 
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') THEN
         INSERT INTO Shipments_audit(operation, shipment_id, order_id, warehouse_id, shipment_date, delivery_date, total_amount, city_id, last_modified)
         VALUES ('INSERT', NEW.shipment_id, NEW.order_id, NEW.warehouse_id, NEW.shipment_date, NEW.delivery_date, NEW.total_amount, NEW.city_id, NEW.last_modified);
         RETURN NEW;
    ELSIF (TG_OP = 'UPDATE') THEN
         INSERT INTO Shipments_audit(operation, shipment_id, order_id, warehouse_id, shipment_date, delivery_date, total_amount, city_id, last_modified)
         VALUES ('UPDATE', NEW.shipment_id, NEW.order_id, NEW.warehouse_id, NEW.shipment_date, NEW.delivery_date, NEW.total_amount, NEW.city_id, NEW.last_modified);
         RETURN NEW;
    ELSIF (TG_OP = 'DELETE') THEN
         INSERT INTO Shipments_audit(operation, shipment_id, order_id, warehouse_id, shipment_date, delivery_date, total_amount, city_id, last_modified)
         VALUES ('DELETE', OLD.shipment_id, OLD.order_id, OLD.warehouse_id, OLD.shipment_date, OLD.delivery_date, OLD.total_amount, OLD.city_id, OLD.last_modified);
         RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_audit_shipments
AFTER UPDATE OR DELETE ON Shipments
FOR EACH ROW EXECUTE FUNCTION audit_shipments();

-----------------------------------------------
-- 9. Inventory Table Trigger & Function
-----------------------------------------------
CREATE OR REPLACE FUNCTION audit_inventory() 
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') THEN
         INSERT INTO Inventory_audit(operation, inventory_id, warehouse_id, product_id, quantity, last_modified)
         VALUES ('INSERT', NEW.inventory_id, NEW.warehouse_id, NEW.product_id, NEW.quantity, NEW.last_modified);
         RETURN NEW;
    ELSIF (TG_OP = 'UPDATE') THEN
         INSERT INTO Inventory_audit(operation, inventory_id, warehouse_id, product_id, quantity, last_modified)
         VALUES ('UPDATE', NEW.inventory_id, NEW.warehouse_id, NEW.product_id, NEW.quantity, NEW.last_modified);
         RETURN NEW;
    ELSIF (TG_OP = 'DELETE') THEN
         INSERT INTO Inventory_audit(operation, inventory_id, warehouse_id, product_id, quantity, last_modified)
         VALUES ('DELETE', OLD.inventory_id, OLD.warehouse_id, OLD.product_id, OLD.quantity, OLD.last_modified);
         RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_audit_inventory
AFTER UPDATE OR DELETE ON Inventory
FOR EACH ROW EXECUTE FUNCTION audit_inventory();

-- =====================================================
-- TRIGGER FUNCTIONS AND TRIGGERS FOR THE ACCOUNTS DATABASE
-- =====================================================

\c accounts;

-----------------------------------------------
-- 10. PaymentMethods Table Trigger & Function
-----------------------------------------------
CREATE OR REPLACE FUNCTION audit_paymentmethods() 
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') THEN
         INSERT INTO PaymentMethods_audit(operation, payment_method_id, method_name, last_modified)
         VALUES ('INSERT', NEW.payment_method_id, NEW.method_name, NEW.last_modified);
         RETURN NEW;
    ELSIF (TG_OP = 'UPDATE') THEN
         INSERT INTO PaymentMethods_audit(operation, payment_method_id, method_name, last_modified)
         VALUES ('UPDATE', NEW.payment_method_id, NEW.method_name, NEW.last_modified);
         RETURN NEW;
    ELSIF (TG_OP = 'DELETE') THEN
         INSERT INTO PaymentMethods_audit(operation, payment_method_id, method_name, last_modified)
         VALUES ('DELETE', OLD.payment_method_id, OLD.method_name, OLD.last_modified);
         RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_audit_paymentmethods
AFTER UPDATE OR DELETE ON PaymentMethods
FOR EACH ROW EXECUTE FUNCTION audit_paymentmethods();

-----------------------------------------------
-- 11. Transactions Table Trigger & Function
-----------------------------------------------
CREATE OR REPLACE FUNCTION audit_transactions() 
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') THEN
         INSERT INTO Transactions_audit(operation, transaction_id, order_id, customer_id, payment_method_id, transaction_date, amount, last_modified)
         VALUES ('INSERT', NEW.transaction_id, NEW.order_id, NEW.customer_id, NEW.payment_method_id, NEW.transaction_date, NEW.amount, NEW.last_modified);
         RETURN NEW;
    ELSIF (TG_OP = 'UPDATE') THEN
         INSERT INTO Transactions_audit(operation, transaction_id, order_id, customer_id, payment_method_id, transaction_date, amount, last_modified)
         VALUES ('UPDATE', NEW.transaction_id, NEW.order_id, NEW.customer_id, NEW.payment_method_id, NEW.transaction_date, NEW.amount, NEW.last_modified);
         RETURN NEW;
    ELSIF (TG_OP = 'DELETE') THEN
         INSERT INTO Transactions_audit(operation, transaction_id, order_id, customer_id, payment_method_id, transaction_date, amount, last_modified)
         VALUES ('DELETE', OLD.transaction_id, OLD.order_id, OLD.customer_id, OLD.payment_method_id, OLD.transaction_date, OLD.amount, OLD.last_modified);
         RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_audit_transactions
AFTER UPDATE OR DELETE ON Transactions
FOR EACH ROW EXECUTE FUNCTION audit_transactions();

-- =====================================================
-- End of Trigger Functions and Triggers Script
-- =====================================================
