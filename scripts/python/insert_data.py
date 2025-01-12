import psycopg2
import random
import time
from faker import Faker

# Database connection parameters
RETAIL_DB_PARAMS = {
    "dbname": "retail",
    "user": "postgres",
    "password": "Delta_12345678",
    "host": "localhost",
    "port": 5432
}

LOGISTICS_DB_PARAMS = {
    "dbname": "logistics",
    "user": "postgres",
    "password": "Delta_12345678",
    "host": "localhost",
    "port": 5432
}

# Initialize Faker
fake = Faker()

# Retail Database Functions

def populate_products(conn):
    cursor = conn.cursor()
    for _ in range(100):
        product_name = fake.word()
        category = fake.word()
        price = round(random.uniform(5, 500), 2)
        stock_quantity = random.randint(50, 500)
        cursor.execute(
            "INSERT INTO Products (name, category, price, stock_quantity) VALUES (%s, %s, %s, %s)",
            (product_name, category, price, stock_quantity)
        )
    conn.commit()
    print("100 sample products added.")

def populate_customers(conn):
    cursor = conn.cursor()
    emails = set()  # Track unique emails
    for customer_id in range(1, 1001):  # Ensure customer IDs are sequential
        name = fake.name()
        email = fake.email()
        while email in emails:  # Ensure email uniqueness
            email = fake.email()
        emails.add(email)  # Add to the set
        address = fake.address()
        phone = fake.phone_number()[:15]  # Truncate to 15 characters
        cursor.execute(
            "INSERT INTO Customers (customer_id, name, email, address, phone) VALUES (%s, %s, %s, %s, %s)",
            (customer_id, name, email, address, phone)
        )
    conn.commit()
    print("1000 sample customers added.")

# Logistics Database Functions

def populate_warehouses(conn):
    cursor = conn.cursor()
    for _ in range(10):
        location = fake.city()
        capacity = random.randint(1000, 5000)
        cursor.execute(
            "INSERT INTO Warehouses (location, capacity) VALUES (%s, %s)",
            (location, capacity)
        )
    conn.commit()
    print("10 sample warehouses added.")

def populate_inventory(conn):
    cursor = conn.cursor()
    for _ in range(500):
        warehouse_id = random.randint(1, 10)
        product_id = random.randint(1, 100)
        quantity = random.randint(100, 1000)
        cursor.execute(
            "INSERT INTO Inventory (warehouse_id, product_id, quantity) VALUES (%s, %s, %s)",
            (warehouse_id, product_id, quantity)
        )
    conn.commit()
    print("500 inventory records added.")

# Fact Data Functions

def populate_orders_and_shipments(retail_conn, logistics_conn):
    retail_cursor = retail_conn.cursor()
    logistics_cursor = logistics_conn.cursor()

    for _ in range(100000):  # Generate 100,000 records
        # Retail - Orders
        customer_id = random.randint(1, 1000)  # Use valid customer IDs
        order_date = fake.date_time_this_year()
        total_amount = round(random.uniform(20, 1000), 2)

        retail_cursor.execute(
            "INSERT INTO Orders (customer_id, order_date, total_amount) VALUES (%s, %s, %s) RETURNING order_id",
            (customer_id, order_date, total_amount)
        )
        order_id = retail_cursor.fetchone()[0]

        for _ in range(random.randint(1, 5)):
            product_id = random.randint(1, 100)
            quantity = random.randint(1, 10)
            retail_cursor.execute("SELECT price FROM Products WHERE product_id = %s", (product_id,))
            price = retail_cursor.fetchone()[0]
            subtotal = price * quantity

            retail_cursor.execute(
                "INSERT INTO Order_Items (order_id, product_id, quantity, subtotal) VALUES (%s, %s, %s, %s)",
                (order_id, product_id, quantity, subtotal)
            )

        # Logistics - Shipments
        warehouse_id = random.randint(1, 10)
        shipment_date = fake.date_time_this_year()
        delivery_date = fake.future_date()
        logistics_cursor.execute(
            "INSERT INTO Shipments (order_id, warehouse_id, shipment_date, delivery_date) VALUES (%s, %s, %s, %s)",
            (order_id, warehouse_id, shipment_date, delivery_date)
        )

        if _ % 1000 == 0:  # Commit every 1000 records for performance
            retail_conn.commit()
            logistics_conn.commit()
            print(f"Processed {_} orders and shipments.")

    retail_conn.commit()
    logistics_conn.commit()
    print("100,000 orders and shipments added.")

def generate_live_data(retail_conn, logistics_conn):
    retail_cursor = retail_conn.cursor()
    logistics_cursor = logistics_conn.cursor()

    while True:
        # Add 2 orders for retail
        for _ in range(2):
            customer_id = random.randint(1, 1000)
            total_amount = round(random.uniform(20, 1000), 2)

            retail_cursor.execute(
                "INSERT INTO Orders (customer_id, total_amount) VALUES (%s, %s) RETURNING order_id",
                (customer_id, total_amount)
            )
            order_id = retail_cursor.fetchone()[0]

            for _ in range(random.randint(1, 5)):
                product_id = random.randint(1, 100)
                quantity = random.randint(1, 10)
                retail_cursor.execute("SELECT price FROM Products WHERE product_id = %s", (product_id,))
                price = retail_cursor.fetchone()[0]
                subtotal = price * quantity

                retail_cursor.execute(
                    "INSERT INTO Order_Items (order_id, product_id, quantity, subtotal) VALUES (%s, %s, %s, %s)",
                    (order_id, product_id, quantity, subtotal)
                )

        # Add 1 shipment for logistics
        order_id = random.randint(1, 100000)  # Assume order IDs from the initial population
        warehouse_id = random.randint(1, 10)
        shipment_date = fake.date_time_this_year()
        delivery_date = fake.future_date()
        logistics_cursor.execute(
            "INSERT INTO Shipments (order_id, warehouse_id, shipment_date, delivery_date) VALUES (%s, %s, %s, %s)",
            (order_id, warehouse_id, shipment_date, delivery_date)
        )

        retail_conn.commit()
        logistics_conn.commit()
        print("Added 2 retail orders and 1 logistics shipment.")

        time.sleep(1)  # Wait for 1 second

# Main script
if __name__ == "__main__":
    retail_conn = None
    logistics_conn = None
    try:
        # Connect to Retail and Logistics Databases
        print("Connecting to Retail and Logistics Databases...")
        retail_conn = psycopg2.connect(**RETAIL_DB_PARAMS)
        logistics_conn = psycopg2.connect(**LOGISTICS_DB_PARAMS)

        # Populate Dimensional Data
        populate_products(retail_conn)
        populate_customers(retail_conn)
        populate_warehouses(logistics_conn)
        populate_inventory(logistics_conn)

        # Populate Fact Data
        print("Populating initial orders and shipments...")
        populate_orders_and_shipments(retail_conn, logistics_conn)

        # Generate live data
        print("Generating live data...")
        generate_live_data(retail_conn, logistics_conn)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if retail_conn:
            retail_conn.close()
        if logistics_conn:
            logistics_conn.close()
