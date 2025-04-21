import os
import sys
from pathlib import Path
import pandas as pd
from datetime import datetime

# Add the project root to Python path
project_root = str(Path(__file__).parent.parent)
sys.path.append(project_root)

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
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

def bulk_insert_orders(orders_data):
    """
    Bulk insert orders from a list of dictionaries
    """
    engine = get_db_connection()
    
    try:
        with engine.connect() as connection:
            # Convert to DataFrame
            df = pd.DataFrame(orders_data)
            # Insert data
            df.to_sql('orders', engine, if_exists='append', index=False)
            print(f"Successfully inserted {len(orders_data)} records!")
            return len(orders_data)
    except Exception as e:
        print(f"Error inserting data: {str(e)}")
        return 0

# Sample data to insert
sample_orders = [
    {
        "region": "North",
        "sales": 15000.00,
        "order_date": "2024-03-15",
        "product_name": "Gaming Laptop",
        "customer_name": "John Smith"
    },
    {
        "region": "South",
        "sales": 8500.50,
        "order_date": "2024-03-15",
        "product_name": "Office Desktop",
        "customer_name": "Mary Johnson"
    },
    {
        "region": "East",
        "sales": 12000.75,
        "order_date": "2024-03-15",
        "product_name": "Server Stack",
        "customer_name": "Robert Brown"
    },
    {
        "region": "West",
        "sales": 9500.25,
        "order_date": "2024-03-15",
        "product_name": "Network Kit",
        "customer_name": "Lisa Davis"
    },
    {
        "region": "North",
        "sales": 11000.00,
        "order_date": "2024-03-16",
        "product_name": "Workstation",
        "customer_name": "Michael Wilson"
    },
    {
        "region": "South",
        "sales": 7500.50,
        "order_date": "2024-03-16",
        "product_name": "Storage Array",
        "customer_name": "Sarah Miller"
    },
    {
        "region": "East",
        "sales": 13500.00,
        "order_date": "2024-03-16",
        "product_name": "Security System",
        "customer_name": "James Anderson"
    },
    {
        "region": "West",
        "sales": 6500.75,
        "order_date": "2024-03-16",
        "product_name": "Cloud License",
        "customer_name": "Patricia Thomas"
    }
]

if __name__ == "__main__":
    # Insert the sample data
    bulk_insert_orders(sample_orders) 