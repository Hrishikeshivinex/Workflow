from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import pandas as pd
from datetime import datetime
import sys
import os

# Add the project root directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.database import DB_CONFIG

def get_db_connection():
    """Create database connection"""
    connection_url = URL.create(
        "mysql+pymysql",
        username=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        database=DB_CONFIG['database']
    )
    return create_engine(connection_url)

def insert_single_order(region, sales, product_name, customer_name, order_date=None):
    """Insert a single order into the database"""
    if order_date is None:
        order_date = datetime.now().date()
    
    engine = get_db_connection()
    
    try:
        with engine.connect() as connection:
            query = text("""
                INSERT INTO orders (region, sales, order_date, product_name, customer_name)
                VALUES (:region, :sales, :order_date, :product_name, :customer_name)
            """)
            
            result = connection.execute(query, {
                "region": region,
                "sales": sales,
                "order_date": order_date,
                "product_name": product_name,
                "customer_name": customer_name
            })
            connection.commit()
            return result.rowcount
    except Exception as e:
        print(f"Error inserting data: {str(e)}")
        return 0

def bulk_insert_orders(orders_data):
    """Insert multiple orders at once"""
    engine = get_db_connection()
    
    try:
        with engine.connect() as connection:
            query = text("""
                INSERT INTO orders (region, sales, order_date, product_name, customer_name)
                VALUES (:region, :sales, :order_date, :product_name, :customer_name)
            """)
            
            result = connection.execute(query, orders_data)
            connection.commit()
            return result.rowcount
    except Exception as e:
        print(f"Error inserting bulk data: {str(e)}")
        return 0

def insert_from_csv(csv_file):
    """Insert data from a CSV file"""
    try:
        df = pd.read_csv(csv_file)
        engine = get_db_connection()
        df.to_sql('orders', engine, if_exists='append', index=False)
        return len(df)
    except Exception as e:
        print(f"Error inserting CSV data: {str(e)}")
        return 0

if __name__ == "__main__":
    # Example usage:
    
    # 1. Insert single order
    print("\n1. Inserting single order:")
    rows = insert_single_order(
        region="North",
        sales=12500.75,
        product_name="Laptop Pro",
        customer_name="Alice Johnson"
    )
    print(f"Inserted {rows} row(s)")

    # 2. Bulk insert multiple orders
    print("\n2. Bulk inserting orders:")
    bulk_data = [
        {
            "region": "South",
            "sales": 8500.00,
            "order_date": "2024-03-15",
            "product_name": "Desktop PC",
            "customer_name": "Bob Smith"
        },
        {
            "region": "East",
            "sales": 15000.50,
            "order_date": "2024-03-15",
            "product_name": "Server",
            "customer_name": "Carol White"
        }
    ]
    rows = bulk_insert_orders(bulk_data)
    print(f"Inserted {rows} row(s)")

    # 3. Insert from CSV (example CSV structure)
    """
    Example CSV file structure (orders.csv):
    region,sales,order_date,product_name,customer_name
    West,9500.25,2024-03-15,Printer,David Brown
    North,11000.00,2024-03-15,Scanner,Eve Wilson
    """
    print("\n3. Inserting from CSV:")
    if os.path.exists('orders.csv'):
        rows = insert_from_csv('orders.csv')
        print(f"Inserted {rows} row(s) from CSV") 