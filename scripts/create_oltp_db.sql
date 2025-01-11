-- SQL Script: Create Retail and Logistics Databases with Tables

-- Step 1: Create Retail Database
CREATE DATABASE retail;

-- Step 2: Create Logistics Database
CREATE DATABASE logistics;

-- Step 3: Create Tables for Retail Database
\c retail;

-- Products Table
CREATE TABLE Products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    price NUMERIC(10, 2) NOT NULL,
    stock_quantity INT NOT NULL
);

-- Customers Table
CREATE TABLE Customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    address TEXT,
    phone VARCHAR(15)
);

-- Orders Table
CREATE TABLE Orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES Customers(customer_id),
    order_date TIMESTAMP DEFAULT NOW(),
    total_amount NUMERIC(10, 2) NOT NULL
);

-- Order Items Table
CREATE TABLE Order_Items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INT REFERENCES Orders(order_id),
    product_id INT REFERENCES Products(product_id),
    quantity INT NOT NULL,
    subtotal NUMERIC(10, 2) NOT NULL
);

-- Step 4: Create Tables for Logistics Database
\c logistics;

-- Warehouses Table
CREATE TABLE Warehouses (
    warehouse_id SERIAL PRIMARY KEY,
    location VARCHAR(100) NOT NULL,
    capacity INT NOT NULL
);

-- Shipments Table
CREATE TABLE Shipments (
    shipment_id SERIAL PRIMARY KEY,
    order_id INT NOT NULL,
    warehouse_id INT REFERENCES Warehouses(warehouse_id),
    shipment_date TIMESTAMP DEFAULT NOW(),
    delivery_date TIMESTAMP
);

-- Inventory Table
CREATE TABLE Inventory (
    inventory_id SERIAL PRIMARY KEY,
    warehouse_id INT REFERENCES Warehouses(warehouse_id),
    product_id INT NOT NULL,
    quantity INT NOT NULL
);

-- End of SQL Script
