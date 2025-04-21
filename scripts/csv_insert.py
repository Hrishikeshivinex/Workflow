import os
import sys
from pathlib import Path
import pandas as pd

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

def insert_from_csv(csv_file):
    """
    Insert data from a CSV file
    CSV should have columns: region,sales,order_date,product_name,customer_name
    """
    try:
        # Read CSV file
        df = pd.read_csv(csv_file)
        
        # Validate columns
        required_columns = ['region', 'sales', 'order_date', 'product_name', 'customer_name']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"Error: Missing columns in CSV: {missing_columns}")
            return 0
        
        # Get database connection
        engine = get_db_connection()
        
        # Insert data
        df.to_sql('orders', engine, if_exists='append', index=False)
        print(f"Successfully inserted {len(df)} records from {csv_file}!")
        return len(df)
        
    except Exception as e:
        print(f"Error inserting CSV data: {str(e)}")
        return 0

if __name__ == "__main__":
    # Example CSV file path
    csv_file = "sample_orders.csv"
    
    if os.path.exists(csv_file):
        insert_from_csv(csv_file)
    else:
        print(f"CSV file not found: {csv_file}")
        print("\nCSV file should have the following format:")
        print("region,sales,order_date,product_name,customer_name")
        print("North,15000.00,2024-03-15,Gaming Laptop,John Smith")
        print("South,8500.50,2024-03-15,Office Desktop,Mary Johnson") 