import os
import sys
from pathlib import Path

# Add the project root to Python path
project_root = str(Path(__file__).parent.parent)
sys.path.append(project_root)

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from config.database import DB_CONFIG

def setup_database():
    # Create initial connection URL without database
    initial_url = URL.create(
        "mysql+pymysql",
        username=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port']
    )
    
    # Create engine without database first
    engine = create_engine(initial_url)
    
    try:
        with engine.connect() as connection:
            # Create database if it doesn't exist
            connection.execute(text(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}"))
            connection.commit()
        
        # Create new URL with database
        db_url = URL.create(
            "mysql+pymysql",
            username=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            database=DB_CONFIG['database']
        )
        
        # Create new engine with database
        db_engine = create_engine(db_url)
        
        # Create tables and insert data
        with db_engine.connect() as connection:
            # Create orders table
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS orders (
                    order_id INT PRIMARY KEY AUTO_INCREMENT,
                    region VARCHAR(50),
                    sales DECIMAL(10,2),
                    order_date DATE,
                    product_name VARCHAR(100),
                    customer_name VARCHAR(100)
                )
            """))
            
            # Insert sample data
            sample_data = [
                ('North', 15000.00, '2024-01-15', 'Laptop', 'John Smith'),
                ('North', 8500.50, '2024-01-16', 'Desktop', 'Mary Johnson'),
                ('South', 12000.75, '2024-01-15', 'Server', 'Robert Brown'),
                ('South', 9500.25, '2024-01-17', 'Printer', 'Lisa Davis'),
                ('East', 11000.00, '2024-01-18', 'Network Switch', 'Michael Wilson'),
                ('East', 7500.50, '2024-01-19', 'Scanner', 'Sarah Miller'),
                ('West', 13500.00, '2024-01-20', 'Storage Array', 'James Anderson'),
                ('West', 6500.75, '2024-01-21', 'Monitor', 'Patricia Thomas')
            ]
            
            # Insert sample data
            for record in sample_data:
                connection.execute(
                    text("""
                        INSERT INTO orders (region, sales, order_date, product_name, customer_name)
                        VALUES (:region, :sales, :order_date, :product_name, :customer_name)
                    """),
                    {
                        "region": record[0],
                        "sales": record[1],
                        "order_date": record[2],
                        "product_name": record[3],
                        "customer_name": record[4]
                    }
                )
            
            connection.commit()
            print("Database and tables created successfully!")
            
    except Exception as e:
        print(f"Error setting up database: {str(e)}")

if __name__ == "__main__":
    setup_database() 