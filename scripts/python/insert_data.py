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

# Function to populate Cities table
def populate_cities(conn):
    cursor = conn.cursor()
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"]
    for city in cities:
        cursor.execute("INSERT INTO Cities (city_name) VALUES (%s) ON CONFLICT (city_name) DO NOTHING", (city,))
    conn.commit()
    print("Cities table populated.")

# Function to populate Warehouses table
def populate_warehouses(conn):
    cursor = conn.cursor()
    warehouse_ids = []
    for _ in range(10):  # Insert 10 warehouses
        location = fake.city()
        capacity = random.randint(100, 10000)  # Ensure valid capacity
        cursor.execute("INSERT INTO Warehouses (location, capacity) VALUES (%s, %s) RETURNING warehouse_id", (location, capacity))
        warehouse_id = cursor.fetchone()[0]
        warehouse_ids.append(warehouse_id)
    conn.commit()
    print("Warehouses table populated.")
    return warehouse_ids

# Function to ensure Customers are populated
def populate_customers(conn):
    cursor = conn.cursor()
    customer_ids = []
    used_emails = set()
    for _ in range(1000):  # Insert exactly 1000 customers
        name = fake.name()
        email = fake.email()
        while email in used_emails:  # Ensure email is unique
            email = fake.email()
        used_emails.add(email)
        address = fake.address()
        phone = fake.phone_number()[:15]  # Truncate to 15 characters
        cursor.execute(
            "INSERT INTO Customers (name, email, address, phone) VALUES (%s, %s, %s, %s) RETURNING customer_id",
            (name, email, address, phone)
        )
        customer_id = cursor.fetchone()[0]
        customer_ids.append(customer_id)
    conn.commit()
    print("1000 Customers inserted successfully.")
    return customer_ids

# Function to ensure total_amount is copied to Shipments
def populate_orders_and_shipments(retail_conn, logistics_conn, customer_ids, warehouse_ids):
    retail_cursor = retail_conn.cursor()
    logistics_cursor = logistics_conn.cursor()
    
    retail_conn.autocommit = True  # Enable autocommit to avoid transaction issues
    logistics_conn.autocommit = True
    
    for _ in range(100000):  # Let the database generate order_id
        customer_id = random.choice(customer_ids)  # Use only valid customer IDs
        order_date = fake.date_time_this_year()
        total_amount = round(random.uniform(20, 1000), 2)
        
        try:
            # Insert into Orders and retrieve generated order_id
            retail_cursor.execute(
                "INSERT INTO Orders (customer_id, order_date, total_amount) VALUES (%s, %s, %s) RETURNING order_id",
                (customer_id, order_date, total_amount)
            )
            order_id = retail_cursor.fetchone()
        
            if order_id is None:
                print("ERROR: Failed to retrieve order_id!")
                continue  # Skip to next iteration if order_id is not retrieved
        
            order_id = order_id[0]
            print(f"Inserted Order ID: {order_id} for Customer ID: {customer_id}")  # Debugging output
            retail_conn.commit()  # Explicit commit after inserting an order
        
            warehouse_id = random.choice(warehouse_ids)  # Ensure valid warehouse_id
            city_id = random.randint(1, 10)  # Assuming 10 predefined cities
            shipment_date = fake.date_time_this_year()
            delivery_date = fake.future_date()
        
            logistics_cursor.execute(
                "INSERT INTO Shipments (order_id, warehouse_id, shipment_date, delivery_date, total_amount, city_id) VALUES (%s, %s, %s, %s, %s, %s)",
                (order_id, warehouse_id, shipment_date, delivery_date, total_amount, city_id)
            )
            logistics_conn.commit()  # Explicit commit for shipments
        
        except Exception as e:
            print(f"Database Error: {e}")
            continue  # Skip problematic entries

        if _ % 1000 == 0:
            print(f"Processed {_} orders and shipments.")
    
    print("100,000 orders and shipments added.")

# Main execution
if __name__ == "__main__":
    retail_conn = None
    logistics_conn = None
    try:
        print("Connecting to Retail and Logistics Databases...")
        retail_conn = psycopg2.connect(**RETAIL_DB_PARAMS)
        logistics_conn = psycopg2.connect(**LOGISTICS_DB_PARAMS)
        
        populate_cities(logistics_conn)
        warehouse_ids = populate_warehouses(logistics_conn)  # Ensure Warehouses are populated
        customer_ids = populate_customers(retail_conn)  # Ensure Customers are populated first
        populate_orders_and_shipments(retail_conn, logistics_conn, customer_ids, warehouse_ids)
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if retail_conn:
            retail_conn.close()
        if logistics_conn:
            logistics_conn.close()
